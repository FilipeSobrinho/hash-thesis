#!/usr/bin/env python3
import argparse, gzip, io, json, os, re, sys
from typing import Any, Iterable

SHA1_RE = re.compile(r"\b[0-9a-fA-F]{40}\b")

def open_text_auto(path: str):
    # transparently open .json or .json.gz as text (utf-8)
    if path.endswith(".gz"):
        return io.TextIOWrapper(gzip.open(path, "rb"), encoding="utf-8")
    return open(path, "r", encoding="utf-8")

def iter_json_stream(obj: Any) -> Iterable[Any]:
    """Yield every value in nested JSON (dict/list/scalars)."""
    if isinstance(obj, dict):
        for v in obj.values():
            yield from iter_json_stream(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from iter_json_stream(v)
    else:
        yield obj

def load_events(path: str):
    with open_text_auto(path) as f:
        txt = f.read().strip()
    # GitHub hour dumps are typically a JSON array; also handle JSONL just in case.
    try:
        data = json.loads(txt)
        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]
        elif isinstance(data, dict):
            evs = data.get("events")
            return [x for x in evs if isinstance(x, dict)] if isinstance(evs, list) else [data]
        else:
            return []
    except json.JSONDecodeError:
        # fallback: JSON Lines (one JSON object per line)
        events = []
        for line in txt.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    events.append(obj)
            except json.JSONDecodeError:
                pass
        return events

def extract_all_sha1(events) -> list[str]:
    shas: set[str] = set()

    # Fast paths per GitHub event docs: payload.commits[*].sha, PR head/base, comment.commit_id, etc.
    for ev in events:
        pld = ev.get("payload", {}) if isinstance(ev, dict) else {}

        # PushEvent commits[*].sha
        commits = pld.get("commits")
        if isinstance(commits, list):
            for c in commits:
                s = isinstance(c, dict) and c.get("sha")
                if isinstance(s, str) and SHA1_RE.fullmatch(s):
                    shas.add(s.lower())

        # PR head/base sha (PullRequestEvent & others)
        pr = pld.get("pull_request")
        if isinstance(pr, dict):
            for side in ("head", "base"):
                o = pr.get(side)
                if isinstance(o, dict):
                    s = o.get("sha")
                    if isinstance(s, str) and SHA1_RE.fullmatch(s):
                        shas.add(s.lower())

        # CommitCommentEvent: payload.comment.commit_id
        cmt = pld.get("comment")
        if isinstance(cmt, dict):
            cid = cmt.get("commit_id")
            if isinstance(cid, str) and SHA1_RE.fullmatch(cid):
                shas.add(cid.lower())

        # Common fields: before/after/head_sha in various events
        for key in ("before", "after", "head_sha"):
            v = pld.get(key)
            if isinstance(v, str) and SHA1_RE.fullmatch(v):
                shas.add(v.lower())

        # Fallback: scan ALL string fields for 40-hex SHAs
        for v in iter_json_stream(ev):
            if isinstance(v, str):
                for m in SHA1_RE.findall(v):
                    shas.add(m.lower())

    return sorted(shas)

def main():
    ap = argparse.ArgumentParser(description="Extract all 160-bit SHA-1 commit hashes (40 hex) from GitHub events JSON/JSONL (.json or .json.gz).")
    ap.add_argument("input", help="Path to 2025-01-01-15.json or .json.gz")
    ap.add_argument("--out", default="sha1_all.txt", help="Output file (one SHA-1 per line)")
    args = ap.parse_args()

    events = load_events(args.input)
    if not events:
        print("No events found or unrecognized format.", file=sys.stderr)
        sys.exit(2)

    shas = extract_all_sha1(events)
    with open(args.out, "w", encoding="utf-8") as f:
        for s in shas:
            f.write(s + "\n")

    print(f"Found {len(shas)} unique SHA-1 hashes.")
    print(f"Wrote: {args.out}")

if __name__ == "__main__":
    main()
