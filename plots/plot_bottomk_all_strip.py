# analysis/plot_bottomk_all_strip.py
# Strip plot for multiple hash functions from CSV: function,rep,relerr
# Produces a single figure with one strip per function.
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import moment
import csv
import sys
from collections import defaultdict

infile = 'bottomk_all_relerr.csv'
if len(sys.argv) > 1:
    infile = sys.argv[1]

# Load CSV
rel_by_func = defaultdict(list)
with open(infile, newline='') as f:
    rd = csv.DictReader(f)
    for row in rd:
        rel_by_func[row['function']].append(float(row['relerr']))

funcs = sorted(rel_by_func.keys())
m6 = {}
for fn in funcs:
    arr = np.array(rel_by_func[fn], dtype=float)
    mu = arr.mean()
    sd = arr.std(ddof=0)
    z = (arr - mu) / (sd if sd>0 else 1.0)
    m6[fn] = moment(z, moment=6)

plt.figure(figsize=(7.2, 0.9 + 0.45*len(funcs)))

for i, fn in enumerate(funcs):
    arr = np.array(rel_by_func[fn], dtype=float)
    y = np.full_like(arr, fill_value=i, dtype=float)
    plt.scatter(arr, y, s=5, alpha=0.25)
    # label later via yticks

# Reference lines
plt.axvline(0.0, linewidth=1)
for v in [0.02, -0.02, 0.01, -0.01]:
    plt.axvline(v, linewidth=0.5, linestyle='--')

plt.xlabel('Relative error ((est - D) / D)')
yticks = [i for i in range(len(funcs))]
ylabs  = [f'{fn}\\n6th moment={m6[fn]:.2g}' for fn in funcs]
plt.yticks(yticks, ylabs)
plt.tight_layout()
plt.savefig('bottomk_all_strip.png', dpi=150, bbox_inches='tight')
print('Saved bottomk_all_strip.png from', infile)
