
# Hash Benchmarking — Thesis

Fast, practical **non-cryptographic hashing** benchmarks for short–moderate inputs, covering raw speed and sketch usage (CM, Bottom-k, OPH).

## Prerequisites

* **Windows + Visual Studio 2022** (Release | x64)
* (Optional) **Python 3** with `pandas` and `matplotlib` for plotting

## Quick start
Inside Visual Studio 2022:
Build all: Ctrl + Shift + B

### 0.1) Download the datasets
Download the following datasets:

https://www.rngresearch.com/download/block0.rng

https://www.kaggle.com/datasets/gupta24789/text8-word-embedding?resource=download

https://www.gharchive.org/2015-01-01-15.json.gz)

Execute the following scripts to keep only the necessary data:

extract_commit_sha1.py

extract_first_words.py

(The datasets should be in the root of the project)

### 1) Speed + raw hash timing

From the repo root:

```
.\scripts\run_speed_and_time.ps1
```

* Generates **CSV** for speed.
* Also generates **plots** automatically.

### 2) Accuracy

From the repo root:

```
 .\scripts\run_all.ps1
```

* Generates **CSV** for accuracy inside a new folder under `results/`.

Then run in the directory created in results :

```
plot_all.py
```

* Creates **plots** from the folder produced in `results/`.

## Datasets
Dataset generation/extraction in include/core/

## Tips
* Close all possible background apps to reduce noise.
* Windows: set Power Plan to **High performance** for more stable timings.
* The source files starting with paralel are paralelized versions of the accuracy experiments that greatly improve execution speed. They were not used to obtain the final results as they are not completely reproducible (the seed order will vary)

## Adding New Components

### Add a new sketch
1. Implement the sketch in `include/sketch/`, following the style of existing sketches.
2. Add the corresponding experiments under `src/` for each dataset, mirroring the structure of existing experiments.
3. Update the script that runs all experiments so the new experiments are included (e.g., your run-all script).

### Add a new dataset
1. Add the dataset type in `include/core/`, matching the conventions of datasets with the same key size (fixed or variable).
2. Create experiments under `src/` for each sketch, consistent with the existing layout.
3. Update the script that runs all experiments so the new experiments are included.

### Add a new hash function
1. Implement it in `include/hash/`, following the pattern of hashes with the same key size (fixed or variable).
2. Update all experiments in `src/` to include the new hash, mirroring how other hash functions (with the same key size) are wired in.


