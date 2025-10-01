# run_all_updated.ps1
param(
  [string]$OutRoot = ".\results",
  [int]$AItems = 500000,
  [int]$K = 24500,   # for Bottom-k
  [int]$R = 10,
  [int]$Width = 32768,
  [int]$Depth = 3
)

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OUTDIR = Join-Path $OutRoot "run_$stamp"
New-Item -ItemType Directory -Force -Path $OUTDIR | Out-Null

function Run-Steps {
  param([string]$Exe, [string[]]$Args)
  Write-Host ">> $Exe $($Args -join ' ')"
  & $Exe @Args
  if ($LASTEXITCODE -ne 0) { throw "FAILED: $Exe" }
}

$BIN = ".\build\win-release-vs\Release"  # or wherever Visual Studio puts them

# --- CMS ---
Run-Steps (Join-Path $BIN "serial_CM_a1.exe") @("--items", $AItems, "--width", $Width, "--depth", $Depth, "--R", $R, "--out", "cms_a1_relerr.csv")
Run-Steps (Join-Path $BIN "serial_CM_a2.exe") @("--items", $AItems, "--width", $Width, "--depth", $Depth, "--R", $R, "--out", "cms_a2_relerr.csv")
Run-Steps (Join-Path $BIN "serial_CM_r1.exe") @("--width", $Width, "--depth", $Depth, "--R", $R, "--out", "cms_r1_relerr.csv")
Run-Steps (Join-Path $BIN "serial_CM_r2.exe") @("--width", $Width, "--depth", $Depth, "--R", $R, "--out", "cms_r2_relerr.csv")

# --- Bottom-k ---
Run-Steps (Join-Path $BIN "serial_BK_a1.exe") @("--items", $AItems, "--k", $K, "--R", $R, "--out", "bottomk_a1_relerr.csv")
Run-Steps (Join-Path $BIN "serial_BK_a2.exe") @("--items", $AItems, "--k", $K, "--R", $R, "--out", "bottomk_a2_relerr.csv")
Run-Steps (Join-Path $BIN "serial_BK_r1.exe") @("--k", $K, "--R", $R, "--out", "bottomk_r1_relerr.csv")
Run-Steps (Join-Path $BIN "serial_BK_r2.exe") @("--k", $K, "--R", $R, "--out", "bottomk_r2_relerr.csv")

# --- OPH ---
# A1/A2 allow --items & --split-seed; R1/R2 are fixed datasets/splits.
Run-Steps (Join-Path $BIN "serial_OPH_a1.exe") @("--items", $AItems, "--K", 200, "--R", $R, "--out", "oph_a1_relerr.csv", "--split-seed", "0xC0FFEE")
Run-Steps (Join-Path $BIN "serial_OPH_a2.exe") @("--items", $AItems, "--K", 200, "--R", $R, "--out", "oph_a2_relerr.csv", "--split-seed", "0xC0FFEE")
Run-Steps (Join-Path $BIN "serial_OPH_r1.exe") @("--K", 200, "--R", $R, "--out", "oph_r1_relerr.csv")
Run-Steps (Join-Path $BIN "serial_OPH_r2.exe") @("--K", 200, "--R", $R, "--out", "oph_r2_relerr.csv")

# Collect CSVs
Get-ChildItem -Filter "*.csv" | ForEach-Object {
  Move-Item $_.FullName (Join-Path $OUTDIR $_.Name) -Force
}

Write-Host "All done. Results at: $OUTDIR"
