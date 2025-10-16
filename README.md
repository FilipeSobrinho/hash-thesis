
# Hash Benchmarking — Thesis

Fast, practical **non-cryptographic hashing** benchmarks for short–moderate inputs, covering raw speed and sketch usage (CM, Bottom-k, OPH).

## Prerequisites

* **Windows + Visual Studio 2022** (Release | x64)
* (Optional) **Python 3** with `pandas` and `matplotlib` for plotting

## Quick start
Inside Visual Studio 2022:
Build all: Ctrl + Shift + B

### 1) Speed + raw hash timing

From the repo root:

```
run_speed_and_time.ps1
```

* Generates **CSV** for speed.
* Also generates **plots** automatically.

### 2) Accuracy

From the repo root:

```
run_all.ps1
```

* Generates **CSV** for accuracy inside a new folder under `results/`.

Then run:

```
plot_all.py
```

* Creates **plots** from the folder produced in `results/`.

## Datasets
Dataset generation/extraction in include/core/

## Tips
* Close all background apps to reduce noise.
* Windows: set Power Plan to **High performance** for more stable timings.



