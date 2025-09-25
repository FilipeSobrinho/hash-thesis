# Strip plot for multiple hash functions from CSV: function,rep,relerr
# Produces a single figure with one strip per function.
# --- Auto-zoom: include ALL points from non-MultShift functions ---
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import moment
import csv
import sys
from collections import defaultdict

infile = 'cms_all_relerr.csv'
if len(sys.argv) > 1:
    infile = sys.argv[1]

# Load CSV
rel_by_func = defaultdict(list)
with open(infile, newline='') as f:
    rd = csv.DictReader(f)
    for row in rd:
        rel_by_func[row['function']].append(float(row['relerr']))

funcs = sorted(rel_by_func.keys())

# 6th standardized moment per function
m6 = {}
for fn in funcs:
    arr = np.array(rel_by_func[fn], dtype=float)
    mu = arr.mean()
    sd = arr.std(ddof=0)
    z = (arr - mu) / (sd if sd > 0 else 1.0)
    m6[fn] = moment(z, moment=6)

plt.figure(figsize=(7.2, 0.9 + 0.45*len(funcs)))

# Strip points
for i, fn in enumerate(funcs):
    arr = np.array(rel_by_func[fn], dtype=float)
    y = np.full_like(arr, fill_value=i, dtype=float)
    plt.scatter(arr, y, s=5, alpha=0.25)

# --- Auto-zoom: include ALL points from non-MultShift functions ---
EXCLUDE_FOR_LIMITS = {'MultShift'}
included = [fn for fn in funcs if fn not in EXCLUDE_FOR_LIMITS] or funcs

vals = np.concatenate([np.asarray(rel_by_func[fn], dtype=float) for fn in included])
lo = float(np.min(vals))
hi = float(np.max(vals))
pad = 0.02 * max(1e-12, hi - lo)   # small visual padding
plt.xlim(lo - pad, hi + pad)

plt.xlabel('Relative error')
yticks = list(range(len(funcs)))
ylabs  = [f"{fn}\nμ6={m6[fn]:.2g}" for fn in funcs]  # second line beneath the name

ax = plt.gca()
ax.set_yticks(yticks)
ax.set_yticklabels(ylabs)

plt.tight_layout()
plt.savefig('exclude_ms.png', dpi=150, bbox_inches='tight')
print('Saved bottomk_all_strip.png from', infile)
