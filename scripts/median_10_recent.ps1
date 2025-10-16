# Description: Aggregate benchmark CSVs from the 10 most recent result folders by computing the median per function.
# Produces new CSVs in a timestamped output folder, and generates plots
param(
  [string]$ResultsRoot = "C:\Users\PCDoctor\Documents\Tese\hash-thesis\results",
  [string]$PlotsScript = "C:\Users\PCDoctor\Documents\Tese\hash-thesis\plots\plot_time_per_hash_no_title.py",
  [int]$Take = 10,
  [string]$OutDir = $(Join-Path $ResultsRoot ( "median_" + (Get-Date -Format "yyyyMMdd_HHmmss") ))
)

# --- Helpers ---
function Get-Median([double[]]$arr) {
  if (-not $arr -or $arr.Count -eq 0) { return $null }
  $sorted = $arr | Sort-Object
  $n = $sorted.Count
  if ($n % 2 -eq 1) { return $sorted[ [int][math]::Floor($n/2) ] }
  else { return ( ($sorted[$n/2 - 1] + $sorted[$n/2]) / 2.0 ) }
}

# Run Python as 'python'
$python = "python"

# Culture-invariant number formatting (so the CSVs use '.' decimals)
$inv = [System.Globalization.CultureInfo]::InvariantCulture

# 1) Pick the 10 most recent date-named folders (start with '2')
$dirs = Get-ChildItem -Path $ResultsRoot -Directory |
        Where-Object { $_.Name -like "2*" } |
        Sort-Object Name -Descending |
        Select-Object -First $Take

if (-not $dirs -or $dirs.Count -eq 0) {
  Write-Host "No matching result folders in $ResultsRoot" -ForegroundColor Yellow
  exit 1
}

New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

# 2) Group CSVs by filename across chosen folders
$groups = @{}  # filename -> [paths...]
foreach ($d in $dirs) {
  Get-ChildItem -Path $d.FullName -Filter *.csv -File | ForEach-Object {
    $key = $_.Name
    if (-not $groups.ContainsKey($key)) { $groups[$key] = @() }
    $groups[$key] += $_.FullName
  }
}

# 3) Aggregate: median per function for ns_per_hash and Mhash_s
$aggCsvPaths = @()

foreach ($csvName in ($groups.Keys | Sort-Object)) {
  # function -> { ns: List<double>, mh: List<double> }
  $map = @{}

  foreach ($csvPath in $groups[$csvName]) {
    try {
      $rows = Import-Csv -Path $csvPath
    } catch {
      Write-Host "WARN: failed to read $csvPath ($_)" -ForegroundColor Yellow
      continue
    }

    foreach ($row in $rows) {
      $func = $row.function
      if ([string]::IsNullOrWhiteSpace($func)) { continue }

      # parse numbers robustly with invariant culture
      $ns = $null; $mh = $null
      try { $ns = [double]::Parse($row.ns_per_hash, $inv) } catch { try { $ns = [double]$row.ns_per_hash } catch {} }
      try { $mh = [double]::Parse($row.Mhash_s,    $inv) } catch { try { $mh = [double]$row.Mhash_s    } catch {} }

      if (-not $map.ContainsKey($func)) {
        $map[$func] = @{
          ns = New-Object System.Collections.Generic.List[double]
          mh = New-Object System.Collections.Generic.List[double]
        }
      }
      if ($ns -ne $null) { $map[$func].ns.Add($ns) }
      if ($mh -ne $null) { $map[$func].mh.Add($mh) }
    }
  }

  # Write aggregated CSV with invariant '.' decimals and expected headers
  $outPath = Join-Path $OutDir $csvName
  $lines = New-Object System.Collections.Generic.List[string]
  $lines.Add("function,Mhash_s,ns_per_hash")
  foreach ($func in ($map.Keys | Sort-Object)) {
    $nsMed = Get-Median($map[$func].ns.ToArray())
    $mhMed = Get-Median($map[$func].mh.ToArray())
    $nsStr = if ($nsMed -ne $null) { $nsMed.ToString($inv) } else { "" }
    $mhStr = if ($mhMed -ne $null) { $mhMed.ToString($inv) } else { "" }
    $lines.Add(("{0},{1},{2}" -f $func, $mhStr, $nsStr))
  }
  Set-Content -Path $outPath -Value $lines -Encoding ASCII
  $aggCsvPaths += $outPath
}

# 4) Log which folders/files were used
$log = New-Object System.Collections.Generic.List[string]
$log.Add("Selected folders:")
$dirs | ForEach-Object { $log.Add("  " + $_.FullName) }
$log.Add("")
$log.Add("CSV groups and counts:")
foreach ($k in ($groups.Keys | Sort-Object)) { $log.Add(("  {0} : {1} files" -f $k, $groups[$k].Count)) }
Set-Content -Path (Join-Path $OutDir "aggregation_log.txt") -Value $log -Encoding ASCII

# 5) Plot aggregated CSVs (no titles) into the same output folder
if (-not (Test-Path $PlotsScript)) {
  Write-Host "WARNING: plotting script not found: $PlotsScript" -ForegroundColor Yellow
} else {
  & $python $PlotsScript --outdir $OutDir @aggCsvPaths
}

Write-Host "Done. Aggregated CSVs and PNGs: $OutDir" -ForegroundColor Green
