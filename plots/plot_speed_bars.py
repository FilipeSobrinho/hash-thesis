# plot_time_per_hash.py
# Usage: python plot_time_per_hash.py <results.csv>
# Output: <csv_basename>_time_per_hash.png

import csv, sys, os
import matplotlib.pyplot as plt

def main():
    if len(sys.argv) < 2:
        print("Usage: python plot_time_per_hash.py <results.csv>")
        raise SystemExit(1)

    infile = sys.argv[1]
    base = os.path.splitext(os.path.basename(infile))[0]

    funcs, nsph = [], []
    with open(infile, newline='') as f:
        rd = csv.DictReader(f)
        for row in rd:
            funcs.append(row["function"])
            nsph.append(float(row["ns_per_hash"]))

    if not funcs:
        print("No rows found in", infile)
        raise SystemExit(2)

    # Sort by time per hash (ascending = faster first visually)
    order = sorted(range(len(funcs)), key=lambda i: nsph[i])
    funcs_sorted = [funcs[i] for i in order]
    nsph_sorted  = [nsph[i]  for i in order]

    plt.figure(figsize=(0.45*len(funcs_sorted)+2, 4.8))
    x = range(len(funcs_sorted))
    plt.bar(x, nsph_sorted)
    plt.xticks(list(x), funcs_sorted, rotation=45, ha='right')
    plt.ylabel('ns per hash (lower is better)')
    plt.title(f'Time per hash — {base}')
    plt.tight_layout()
    outpng = f"{base}_time_per_hash.png"
    plt.savefig(outpng, dpi=150, bbox_inches='tight')
    print(f"Saved {outpng}")

if __name__ == "__main__":
    main()
