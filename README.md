# Hash Thesis Starter

Minimal CMake + C++ scaffold tailored for Windows (Visual Studio 2022).

## Quick start (Visual Studio)

1. Install **Visual Studio 2022 Community** with the **Desktop development with C++** workload.
   - Optional components: *MSVC v143 (x64/x86)*, *Windows 10/11 SDK*, *C++ CMake tools for Windows*.
2. Open **File → Open → CMake...** and select `CMakeLists.txt` in this folder.
3. Switch to **x64** and **Release** in the toolbar.
4. Choose a target (`bench_timing`, `accuracy`, or `verify`) and press **Ctrl+F5**.

## Targets

- `bench_timing`: Phase A timing harness (skeleton).
- `accuracy`: Phase B accuracy harness (skeleton).
- `verify`: Fast contract tests to validate new hashes/sketches/datasets.

## Layout

- `include/` public headers (interfaces, utilities)
- `src/`       source files and executables
- `config/`    YAML configs (placeholders)
- `results/`   CSV outputs (created by executables)
- `plots/`     Python scripts to render figures (placeholder)
- `scripts/`   helpers (PowerShell)
- `third_party/` licenses & notes for external code

## Notes

- Windows: set Power Plan to **High performance** for more stable timings.
- Vendor Control Center: prefer **Turbo/Custom** with high CPU wattage and fan curve.
