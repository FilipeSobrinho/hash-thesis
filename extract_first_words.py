#!/usr/bin/env python3
import argparse, zipfile, io, os

def stream_words_from_zip(zip_path: str):
    """
    Yield whitespace-separated tokens from the first (largest) text file inside the zip,
    streaming without loading everything into memory.
    """
    with zipfile.ZipFile(zip_path, 'r') as z:
        # Pick the largest file in the archive (text8 zips usually have a single file).
        info = max(z.infolist(), key=lambda i: i.file_size)
        with z.open(info, 'r') as raw:
            # Decode as ASCII/UTF-8; text8 is plain ASCII.
            f = io.TextIOWrapper(raw, encoding='utf-8', errors='strict', newline='')
            buf = ''
            CHUNK = 1 << 20  # 1 MiB chunks
            while True:
                chunk = f.read(CHUNK)
                if not chunk:
                    if buf:
                        # last residue
                        for w in buf.split():
                            yield w
                    break
                buf += chunk
                # Split by whitespace; keep the tail (possibly incomplete token) in buf
                parts = buf.split()
                if not parts:
                    # all whitespace; keep empty residue
                    buf = ''
                    continue
                # If last char in chunk may split a word, preserve tail
                if buf[-1].isspace():
                    # clean split
                    for w in parts:
                        yield w
                    buf = ''
                else:
                    # keep the last segment in buf (may be incomplete)
                    *full, tail = parts
                    for w in full:
                        yield w
                    buf = tail

def write_first_n(zip_path: str, N: int, out_prefix: str):
    os.makedirs(os.path.dirname(out_prefix) or ".", exist_ok=True)
    out_space = f"{out_prefix}_words.txt"
    out_lines = f"{out_prefix}_words_per_line.txt"

    count = 0
    with open(out_space, "w", encoding="utf-8") as f_space, \
         open(out_lines, "w", encoding="utf-8") as f_lines:
        first = True
        for w in stream_words_from_zip(zip_path):
            if count >= N:
                break
            # basic hygiene: drop empty tokens (shouldn't occur) and strip
            if not w:
                continue
            if first:
                f_space.write(w)
                first = False
            else:
                f_space.write(" ")
                f_space.write(w)
            f_lines.write(w)
            f_lines.write("\n")
            count += 1

    print(f"Extracted {count} words.")
    print(f"Wrote: {out_space}")
    print(f"Wrote: {out_lines}")

def main():
    ap = argparse.ArgumentParser(description="Extract the first N words from text8 inside test8.zip.")
    ap.add_argument("zipfile", help="Path to test8.zip")
    ap.add_argument("--count", type=int, default=1_000_000, help="Number of words to extract (default: 1,000,000)")
    ap.add_argument("--out-prefix", default="first_1e6", help="Output prefix (default: first_1e6)")
    args = ap.parse_args()
    write_first_n(args.zipfile, args.count, args.out_prefix)

if __name__ == "__main__":
    main()
