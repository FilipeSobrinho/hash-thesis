# analysis/plot_bottomk_ms_strip.py
# Strip plot of relative errors for bottom-k (MS, A1).
# Expects one file with one float per line: bottomk_ms_a1_relerr.txt

import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import moment
import os

infile = 'bottomk_ms_a1_relerr.txt'
if not os.path.exists(infile):
    raise SystemExit(f"Input file not found: {infile} (run the accuracy program first)")

rel = np.loadtxt(infile)  # 1-D array of relative errors

# 6th standardized moment (outlier sensitivity)
mu = rel.mean()
sigma = rel.std(ddof=0)
z = (rel - mu) / (sigma if sigma>0 else 1.0)
m6 = moment(z, moment=6)

plt.figure(figsize=(6.4, 2.8))
y = np.zeros_like(rel)  # one strip at y=0
plt.scatter(rel, y, s=5, alpha=0.25, color='black')

# Reference lines
plt.axvline(0.0, linewidth=1)
for v in [0.005, -0.005, 0.01, -0.01]:
    plt.axvline(v, linewidth=0.5, linestyle='--')

plt.xlabel('Relative error ((est - D) / D)')
plt.yticks([0], [f'Bottom-k (k=24500)\n6th moment={m6:.2g}'])
plt.tight_layout()
plt.savefig('bottomk_ms_strip.png', dpi=150, bbox_inches='tight')
print('Saved bottomk_ms_strip.png')
