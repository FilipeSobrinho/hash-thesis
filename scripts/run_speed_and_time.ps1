#runs all the performance tests and generates plots. 
# run from the root and it will create a timestamped folder in results/
#number of loops defined here is not working, they are being defined in the sorce directly
param(
    [int]$LoopsA2 = 200,
    [int]$LoopsR1 = 200,
    [int]$LoopsR2 = 200,
    [int]$LoopsOPH = 5000,
    [int]$BK_K = 24500,
    [int]$OPH_K = 200,
    [int]$CM_Width = 32768,
    [int]$CM_Depth = 3,

    # Fixed paths
    [string]$BinRoot     = "C:\Users\PCDoctor\Documents\Tese\hash-thesis\build\win-release-vs\Release",
    [string]$PlotsScript = "C:\Users\PCDoctor\Documents\Tese\hash-thesis\plots\plot_time_per_hash_no_title.py",
    [string]$ResultsRoot = "C:\Users\PCDoctor\Documents\Tese\hash-thesis\results",
    [string]$ResultsDir  = $(Join-Path -Path "C:\Users\PCDoctor\Documents\Tese\hash-thesis\results" -ChildPath (Get-Date -Format "yyyyMMdd_HHmmss"))
)

function Resolve-Exe { param([string]$Name) return (Join-Path $BinRoot $Name) }

# Run an exe with its WorkingDirectory set to the results folder so outputs land there
# Run an exe with CWD = $ResultsDir (so outputs land there)
function Run-Tool {
    param([string]$Exe,[object[]]$Args)

    $exeName = Split-Path $Exe -Leaf
    # sanitize and stringify args to avoid nulls
    $argList = @()
    foreach ($a in $Args) { if ($null -ne $a) { $argList += ($a.ToString()) } }

    Write-Host "-> $exeName $($argList -join ' ')"
    if (-not (Test-Path $Exe)) {
        Write-Host "WARNING: not found: $Exe" -ForegroundColor Yellow
        return
    }

    Push-Location $ResultsDir
    try {
        & $Exe @argList
        if ($LASTEXITCODE -ne 0) {
            Write-Host "EXIT CODE $LASTEXITCODE for $exeName" -ForegroundColor Red
        }
    } finally {
        Pop-Location
    }
}


New-Item -ItemType Directory -Force -Path $ResultsRoot | Out-Null
New-Item -ItemType Directory -Force -Path $ResultsDir  | Out-Null
New-Item -ItemType Directory -Force -Path (Split-Path -Parent $PlotsScript) | Out-Null

# -------- TIME tests --------
Run-Tool (Resolve-Exe "time_all_a2.exe") @("--loops", $LoopsA2, "--out", "a2_time.csv")
Run-Tool (Resolve-Exe "time_all_r1.exe") @("--loops", $LoopsR1, "--out", "r1_time.csv")
Run-Tool (Resolve-Exe "time_all_r2.exe") @("--loops", $LoopsR2, "--out", "r2_time.csv")

# -------- SPEED tests: A2 --------
Run-Tool (Resolve-Exe "speed_CM_a2.exe")  @("--loops", $LoopsA2, "--width", $CM_Width, "--depth", $CM_Depth, "--out", "a2_speed_cm.csv")
Run-Tool (Resolve-Exe "speed_BK_a2.exe")  @("--loops", $LoopsA2, "--K", $BK_K, "--out", "a2_speed_bk.csv")
Run-Tool (Resolve-Exe "speed_OPH_a2.exe") @("--loops", $LoopsOPH, "--K", $OPH_K, "--out", "a2_speed_oph.csv")

# -------- SPEED tests: R1 --------
Run-Tool (Resolve-Exe "speed_CM_r1.exe")  @("--loops", $LoopsR1, "--width", $CM_Width, "--depth", $CM_Depth, "--out", "r1_speed_cm.csv")
Run-Tool (Resolve-Exe "speed_BK_r1.exe")  @("--loops", $LoopsR1, "--K", $BK_K, "--out", "r1_speed_bk.csv")
Run-Tool (Resolve-Exe "speed_OPH_r1.exe") @("--loops", $LoopsOPH, "--K", $OPH_K, "--out", "r1_speed_oph.csv")

# -------- SPEED tests: R2 --------
Run-Tool (Resolve-Exe "speed_CM_r2.exe")  @("--loops", $LoopsR2, "--width", $CM_Width, "--depth", $CM_Depth, "--out", "r2_speed_cm.csv")
Run-Tool (Resolve-Exe "speed_BK_r2.exe")  @("--loops", $LoopsR2, "--K", $BK_K, "--out", "r2_speed_bk.csv")
Run-Tool (Resolve-Exe "speed_OPH_r2.exe") @("--loops", $LoopsOPH, "--K", $OPH_K, "--out", "r2_speed_oph.csv")

# -------- Sweep up any stray CSVs that previous runs dumped in repo root --------
Get-ChildItem -Path . -Filter *.csv -File | ForEach-Object {
    $dest = Join-Path $ResultsDir $_.Name
    if (-not (Test-Path $dest)) {
        try { Move-Item -Path $_.FullName -Destination $dest -Force } catch {}
    }
}

Write-Host ""
Write-Host "Plotting PNGs (no titles) to: $ResultsDir" -ForegroundColor Cyan

$python = "python"
$csvs = Get-ChildItem -Path $ResultsDir -Filter *.csv | ForEach-Object { $_.FullName }
if (-not (Test-Path $PlotsScript)) {
    Write-Host "WARNING: plotting script not found: $PlotsScript" -ForegroundColor Yellow
} else {
    & $python $PlotsScript --outdir $ResultsDir @csvs
}

Write-Host ""
Write-Host "Done. CSVs and PNGs are in: $ResultsDir" -ForegroundColor Green
