# plot_cms_strip_zoom_manual.py
# Strip plot from CSV (function,rep,relerr) with adjustable zoom.
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import moment
import csv, sys, argparse
from collections import defaultdict

# ---- knobs (edit these or use CLI flags) ----
XMIN = 2.3     # e.g., -0.05  (set both XMIN/XMAX to use absolute limits)
XMAX = 2.5     # e.g.,  0.05
PCT_LO = 1.0    # used only if XMIN/XMAX are None
PCT_HI = 99.0

# ---- CLI ----
ap = argparse.ArgumentParser(description='Strip plot with adjustable zoom')
ap.add_argument('infile', nargs='?', default='cms_all_relerr.csv')
ap.add_argument('--xlim', nargs=2, type=float, help='absolute x-limits: xmin xmax')
ap.add_argument('--pct',  nargs=2, type=float, help='percentile limits: lo hi (used if no --xlim)')
args = ap.parse_args()

infile = args.infile
if args.xlim:
    XMIN, XMAX = float(args.xlim[0]), float(args.xlim[1])
elif args.pct:
    PCT_LO, PCT_HI = float(args.pct[0]), float(args.pct[1])

# ---- load ----
rel = defaultdict(list)
with open(infile, newline='') as f:
    rd = csv.DictReader(f)
    for row in rd:
        rel[row['function']].append(float(row['relerr']))

funcs = sorted(rel.keys())

# ---- 6th standardized moment ----
m6 = {}
for fn in funcs:
    arr = np.array(rel[fn], dtype=float)
    mu, sd = arr.mean(), arr.std(ddof=0)
    z = (arr - mu) / (sd if sd > 0 else 1.0)
    m6[fn] = moment(z, moment=6)

# ---- plot ----
plt.figure(figsize=(7.5, 0.9 + 0.45 * len(funcs)))
for i, fn in enumerate(funcs):
    arr = np.array(rel[fn], dtype=float)
    plt.scatter(arr, np.full_like(arr, i, dtype=float), s=6, alpha=0.28, label=fn)

plt.axvline(0.0, linewidth=1)
for v in [0.02, -0.02, 0.01, -0.01]:
    plt.axvline(v, linewidth=0.5, linestyle='--')

# ---- compute x-limits ----
all_vals = np.concatenate([np.asarray(rel[fn], dtype=float) for fn in funcs])
if XMIN is not None and XMAX is not None:
    lo, hi = float(XMIN), float(XMAX)
else:
    lo, hi = np.percentile(all_vals, [PCT_LO, PCT_HI])
pad = 0.05 * max(1e-12, (hi - lo))
plt.xlim(lo - pad, hi + pad)

plt.xlabel('Relative error')
yticks = list(range(len(funcs)))
ylabs  = [f"{fn}\nμ6={m6[fn]:.2g}" for fn in funcs]  # second line beneath the name

ax = plt.gca()
ax.set_yticks(yticks)
ax.set_yticklabels(ylabs)

# Optional: make the two-line labels nicely centered
#for lab in ax.get_yticklabels():
#    lab.set_multialignment('center')

plt.tight_layout()
outpng = 'zoom.png'
plt.savefig(outpng, dpi=150, bbox_inches='tight')
print(f'Saved {outpng} from', infile)

# -------- How to adjust manually --------
# 1) Open this file and set:
#       XMIN = -0.05
#       XMAX =  0.05
#    (and leave PCT_LO/PCT_HI as you like).
# 2) Or pass on the command line:
#       python plot_cms_strip_zoom_manual.py cms_all_relerr.csv --xlim -0.05 0.05
# 3) Or use percentiles (when you do not know good absolute bounds):
#       python plot_cms_strip_zoom_manual.py cms_all_relerr.csv --pct 2 98
