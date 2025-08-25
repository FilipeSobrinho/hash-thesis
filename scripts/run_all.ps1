Param(
  [string]$Preset = "win-release"
)
cmake --preset $Preset
cmake --build --preset $Preset -j
Write-Host "Built targets to build/$Preset"
# TODO: add invocations for bench_timing.exe and accuracy.exe with configs
