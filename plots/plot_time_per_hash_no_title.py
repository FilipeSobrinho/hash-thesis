# Creates <outdir>/<csv_basename>_time_per_hash.png (no title).
# Usage: python plot_time_per_hash_no_title.py --outdir <DIR> <csv1> [<csv2> ...]
import sys, os, csv

def parse_args(argv):
    outdir = None
    files = []
    i = 0
    while i < len(argv):
        a = argv[i]
        if a == "--outdir" and i+1 < len(argv):
            outdir = argv[i+1]; i += 2
        else:
            files.append(a); i += 1
    return outdir, files

def plot_file(infile: str, outdir: str | None):
    try:
        import matplotlib.pyplot as plt
    except Exception as e:
        print(f"[error] matplotlib not available: {e}")
        return
    base = os.path.splitext(os.path.basename(infile))[0]
    funcs, nsph = [], []
    with open(infile, newline='') as f:
        rd = csv.DictReader(f)
        for row in rd:
            if "function" in row and "ns_per_hash" in row:
                funcs.append(row["function"])
                try:
                    nsph.append(float(row["ns_per_hash"]))
                except:
                    pass
    if not funcs:
        print(f"[skip] No rows or missing columns in {infile}")
        return

    order = sorted(range(len(funcs)), key=lambda i: nsph[i])
    funcs = [funcs[i] for i in order]
    nsph  = [nsph[i]  for i in order]

    plt.figure(figsize=(0.45*len(funcs)+2, 4.8))
    x = list(range(len(funcs)))
    plt.bar(x, nsph)
    plt.xticks(x, funcs, rotation=45, ha='right')
    plt.ylabel('ns per hash')
    # No title by request
    plt.tight_layout()

    os.makedirs(outdir, exist_ok=True)
    outpng = os.path.join(outdir, f"{base}_time_per_hash.png")
    plt.savefig(outpng, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"[ok] {outpng}")

def main():
    if len(sys.argv) < 3 or sys.argv[1] != "--outdir":
        print("Usage: python plot_time_per_hash_no_title.py --outdir <DIR> <csv1> [<csv2> ...]")
        sys.exit(1)
    outdir, files = parse_args(sys.argv[1:])
    for p in files:
        plot_file(p, outdir)

if __name__ == "__main__":
    main()
