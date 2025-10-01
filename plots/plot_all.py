# plot_all.py
# Iterate over all CSV files in a directory and generate strip plots
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import moment
import csv
import sys
from collections import defaultdict
import os
import glob

# Directory to search
indir = "."
if len(sys.argv) > 1:
    indir = sys.argv[1]

csv_files = glob.glob(os.path.join(indir, "*.csv"))
if not csv_files:
    print("No CSV files found in", indir)
    sys.exit(0)

for infile in csv_files:
    rel_by_func = defaultdict(list)
    with open(infile, newline='') as f:
        rd = csv.DictReader(f)
        for row in rd:
            rel_by_func[row['function']].append(float(row['relerr']))

    funcs = sorted(rel_by_func.keys())
    if not funcs:
        print("Skipping", infile, "(no data)")
        continue

    # 6th moment normalized
    m6 = {}
    for fn in funcs:
        arr = np.array(rel_by_func[fn], dtype=float)
        mu = arr.mean()
        sd = arr.std(ddof=0)
        z = (arr - mu) / (sd if sd > 0 else 1.0)
        m6[fn] = moment(z, moment=6)

    plt.figure(figsize=(7.2, 0.9 + 0.45 * len(funcs)))
    for i, fn in enumerate(funcs):
        arr = np.array(rel_by_func[fn], dtype=float)
        y = np.full_like(arr, fill_value=i, dtype=float)
        plt.scatter(arr, y, s=5, alpha=0.25)

    plt.xlabel('Relative error')
    yticks = list(range(len(funcs)))
    ylabs = [f"{fn}\nμ6={m6[fn]:.2g}" for fn in funcs]
    ax = plt.gca()
    ax.set_yticks(yticks)
    ax.set_yticklabels(ylabs)

    plt.tight_layout()
    outpng = os.path.splitext(os.path.basename(infile))[0] + ".png"
    plt.savefig(outpng, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"Saved {outpng} from {infile}")
