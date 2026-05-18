#!/usr/bin/env python3
r"""
Downloads the two MusicBrainz dump files needed to produce a dated
top-editors JSON snapshot, updates manifest.json, then cleans up.

Run it in the folder where you want the download to happen:
    cd /path/to/your/folder
    python checkmbeditors.py

You will be prompted to paste the dump URL and the snapshot date.
"""

import os
import sys
import json
import shutil
import tarfile
import urllib.request
from collections import defaultdict
from datetime import datetime

APPLIED_STATUS_CODE = 2  # MusicBrainz STATUS_APPLIED
NEEDED_TARBALLS = ["mbdump-editor.tar.bz2", "mbdump-edit.tar.bz2"]
JSON_SUBFOLDER = "json"

DATE_FORMATS = [
    "%Y-%m-%d",       # 2026-05-17
    "%d-%m-%Y",       # 17-05-2026
    "%d/%m/%Y",       # 17/05/2026
    "%m/%d/%Y",       # 05/17/2026
    "%d-%b-%Y",       # 17-May-2026
    "%d %b %Y",       # 17 May 2026
    "%d %B %Y",       # 17 May 2026
    "%B %d %Y",       # May 17 2026
    "%b %d %Y",       # May 17 2026
    "%d-%B-%Y",       # 17-May-2026
]


def parse_date(raw: str) -> str:
    """Parse a date string in many formats, return YYYY-MM-DD."""
    raw = raw.strip()
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Could not parse date: '{raw}'\nAccepted formats: YYYY-MM-DD, DD-MM-YYYY, DD-Mon-YYYY, etc.")


# ---------------------------------------------------------------------------
# Download
# ---------------------------------------------------------------------------

def download_file(url: str, dest: str):
    print(f"  Downloading {os.path.basename(dest)} ...", flush=True)
    req = urllib.request.Request(url, headers={"User-Agent": "mb-top-editors-script/1.0"})
    with urllib.request.urlopen(req) as resp:
        total = resp.headers.get("Content-Length")
        total = int(total) if total else None
        downloaded = 0
        chunk = 1024 * 1024
        with open(dest, "wb") as f:
            while True:
                block = resp.read(chunk)
                if not block:
                    break
                f.write(block)
                downloaded += len(block)
                if total:
                    pct = downloaded / total * 100
                    mb = downloaded / 1024 / 1024
                    print(f"\r    {mb:.0f} MB / {total/1024/1024:.0f} MB  ({pct:.1f}%)",
                          end="", flush=True)
                else:
                    print(f"\r    {downloaded/1024/1024:.0f} MB downloaded",
                          end="", flush=True)
        print()


def download_dumps(base_url: str, folder: str) -> list[str]:
    if not base_url.endswith("/"):
        base_url += "/"
    downloaded = []
    for name in NEEDED_TARBALLS:
        dest = os.path.join(folder, name)
        if os.path.isfile(dest):
            print(f"  Already exists, skipping: {name}")
        else:
            download_file(base_url + name, dest)
        downloaded.append(dest)
    return downloaded


# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------

def extract_member(tar_path: str, wanted: list[str], target_root: str):
    print(f"  Extracting from {os.path.basename(tar_path)} ...", flush=True)
    os.makedirs(target_root, exist_ok=True)
    with tarfile.open(tar_path, "r:bz2") as tf:
        members_by_name = {m.name: m for m in tf.getmembers()}
        for name in wanted:
            m = members_by_name.get(name)
            if m is None:
                for cand_name, cand in members_by_name.items():
                    if cand_name.endswith(name):
                        m = cand
                        break
            if m is None:
                print(f"  ! Warning: {name} not found in archive")
                continue
            print(f"    - {m.name}")
            tf.extract(m, path=target_root)


def extract_all(folder: str) -> tuple[str, str]:
    editor_tar  = os.path.join(folder, "mbdump-editor.tar.bz2")
    edit_tar    = os.path.join(folder, "mbdump-edit.tar.bz2")
    editor_root = os.path.join(folder, "mbdump-editor")
    edit_root   = os.path.join(folder, "mbdump-edit")

    extract_member(editor_tar, ["mbdump/editor_sanitized", "mbdump/editor_sanitised", "mbdump/editor"], editor_root)
    extract_member(edit_tar,   ["mbdump/edit"], edit_root)

    editor_dir  = os.path.join(editor_root, "mbdump")
    editor_file = next(
        (os.path.join(editor_dir, n) for n in ["editor_sanitized", "editor_sanitised", "editor"]
         if os.path.isfile(os.path.join(editor_dir, n))), ""
    )
    edit_file = os.path.join(edit_root, "mbdump", "edit")

    if not os.path.isfile(editor_file):
        sys.exit(f"❌ Could not find editor file after extraction in {editor_dir}")
    if not os.path.isfile(edit_file):
        sys.exit(f"❌ Could not find edit file after extraction in {edit_root}")

    return editor_file, edit_file


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def unescape_pg(val: str):
    if val == r"\N":
        return None
    if "\\" not in val:
        return val
    out = []
    i = 0
    s = val
    L = len(s)
    while i < L:
        ch = s[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        i += 1
        if i >= L:
            out.append("\\")
            break
        nxt = s[i]
        i += 1
        if nxt == "n":
            out.append("\n")
        elif nxt == "r":
            out.append("\r")
        elif nxt == "t":
            out.append("\t")
        elif nxt == "\\":
            out.append("\\")
        elif nxt.isdigit():
            octs = nxt
            for _ in range(2):
                if i < L and s[i].isdigit():
                    octs += s[i]
                    i += 1
                else:
                    break
            try:
                out.append(chr(int(octs, 8)))
            except Exception:
                out.append("\\" + octs)
        else:
            out.append(nxt)
    return "".join(out)


def load_editors(editor_file: str) -> dict[int, str]:
    eid_to_name = {}
    with open(editor_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 2:
                continue
            try:
                eid = int(parts[0])
            except Exception:
                continue
            name = unescape_pg(parts[1]) or ""
            eid_to_name[eid] = name
    return eid_to_name


def count_applied_edits(edit_file: str) -> dict[int, int]:
    counts = defaultdict(int)
    with open(edit_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            eid_raw    = unescape_pg(parts[1])
            status_raw = unescape_pg(parts[3])
            if eid_raw is None or status_raw is None:
                continue
            try:
                if int(status_raw) == APPLIED_STATUS_CODE:
                    counts[int(eid_raw)] += 1
            except Exception:
                continue
    return counts


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------

def write_snapshot(path: str, rows, snapshot_date: str):
    data = [
        {
            "rank": i,
            "editor_id": eid,
            "editor_name": name or "",
            "applied_edit_count": cnt,
        }
        for i, (eid, name, cnt) in enumerate(rows, start=1)
    ]
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def update_manifest(folder: str, snapshot_date: str, relative_filename: str):
    """relative_filename should be e.g. 'json/2026-05-17.json'"""
    manifest_path = os.path.join(folder, "manifest.json")
    if os.path.isfile(manifest_path):
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
    else:
        manifest = []

    manifest = [e for e in manifest if e.get("date") != snapshot_date]
    manifest.append({"date": snapshot_date, "file": relative_filename})
    manifest.sort(key=lambda e: e["date"], reverse=True)

    with open(manifest_path, "w", encoding="utf-8") as f:
        json.dump(manifest, f, ensure_ascii=False, indent=2)

    print(f"  ✅ Updated manifest.json ({len(manifest)} snapshots total)")
    for entry in manifest:
        print(f"     {entry['date']} → {entry['file']}")


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def cleanup(folder: str):
    print("\nCleaning up downloaded and extracted files ...")
    for name in NEEDED_TARBALLS:
        p = os.path.join(folder, name)
        if os.path.isfile(p):
            os.remove(p)
            print(f"  Deleted {name}")
    for d in ["mbdump-editor", "mbdump-edit"]:
        p = os.path.join(folder, d)
        if os.path.isdir(p):
            shutil.rmtree(p)
            print(f"  Deleted {d}/")


# ---------------------------------------------------------------------------
# GitHub push
# ---------------------------------------------------------------------------

def push_to_github(folder: str, relative_filename: str):
    import subprocess

    manifest_file = "manifest.json"

    def run(cmd, check=True):
        result = subprocess.run(cmd, cwd=folder, capture_output=True, text=True)
        if check and result.returncode != 0:
            print(f"  ❌ git error: {result.stderr.strip() or result.stdout.strip()}")
            return False
        return result

    def is_git_repo():
        r = run(["git", "rev-parse", "--is-inside-work-tree"], check=False)
        return r and r.returncode == 0

    if not is_git_repo():
        print("  No git repo detected in this folder.")
        sys.stdout.write("  GitHub repo URL (e.g. https://github.com/YoGo9/MBTopEditors): ")
        sys.stdout.flush()
        remote_url = input().strip()
        if not remote_url:
            print("  ❌ No URL provided, skipping push.")
            return
        if not run(["git", "init"]): return
        if not run(["git", "remote", "add", "origin", remote_url]): return
        run(["git", "branch", "-M", "main"], check=False)
        run(["git", "fetch", "origin"], check=False)
        print(f"  ✅ Git repo initialised with remote: {remote_url}")
    else:
        r = run(["git", "remote", "get-url", "origin"], check=False)
        if r.returncode != 0:
            sys.stdout.write("  No remote 'origin' found. GitHub repo URL: ")
            sys.stdout.flush()
            remote_url = input().strip()
            if not remote_url:
                print("  ❌ No URL provided, skipping push.")
                return
            if not run(["git", "remote", "add", "origin", remote_url]): return

    print("  Running git commands ...")
    if not run(["git", "add", relative_filename, manifest_file]): return

    snapshot_date = os.path.basename(relative_filename).replace(".json", "")
    commit_result = run(["git", "commit", "-m", f"snapshot {snapshot_date}"], check=False)
    if commit_result.returncode != 0:
        out = commit_result.stdout.strip() + commit_result.stderr.strip()
        if "nothing to commit" in out:
            print("  (Nothing new to commit — already up to date)")
        else:
            print(f"  ❌ git commit failed: {out}")
            return

    if not run(["git", "push", "-u", "origin", "main"]): return
    print(f"  ✅ Pushed {relative_filename} and manifest.json to GitHub.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    folder = os.getcwd()

    sys.stdout.write("MusicBrainz Top Editors\n")
    sys.stdout.write("=" * 40 + "\n")
    sys.stdout.write(f"Working folder:  {folder}\n\n")

    # --- Ask for snapshot date ---
    sys.stdout.write("Snapshot date (e.g. 2026-05-17, 17-May-2026, 17/05/2026):\n> ")
    sys.stdout.flush()
    raw_date = input().strip()
    try:
        snapshot_date = parse_date(raw_date)
    except ValueError as e:
        sys.exit(f"❌ {e}")

    out_filename     = f"{snapshot_date}.json"
    relative_path    = f"{JSON_SUBFOLDER}/{out_filename}"
    out_path         = os.path.join(folder, JSON_SUBFOLDER, out_filename)

    sys.stdout.write(f"Snapshot date:   {snapshot_date}\n")
    sys.stdout.write(f"Output file:     {relative_path}\n\n")

    # --- Ask for dump URL ---
    sys.stdout.write("Paste the dump URL (e.g. https://data.metabrainz.org/pub/musicbrainz/data/fullexport/20260411-002149/):\n> ")
    sys.stdout.flush()
    base_url = input().strip()
    if not base_url:
        sys.exit("❌ No URL provided.")

    # --- Download ---
    print("\n[1/5] Downloading tarballs ...")
    download_dumps(base_url, folder)

    # --- Extract ---
    print("\n[2/5] Extracting needed files ...")
    editor_file, edit_file = extract_all(folder)

    # --- Process ---
    print(f"\n[3/5] Processing ...")
    print(f"  Reading editors from {editor_file}")
    editors = load_editors(editor_file)
    print(f"  Loaded {len(editors):,} editors.")

    print(f"  Counting applied edits from {edit_file}")
    counts = count_applied_edits(edit_file)
    print(f"  Counted edits for {len(counts):,} editors.")

    rows = [
        (eid, editors.get(eid, f"(editor #{eid})"), cnt)
        for eid, cnt in counts.items()
    ]
    rows.sort(key=lambda x: x[2], reverse=True)
    top = rows[:500]

    # --- Write snapshot ---
    print(f"\n[4/5] Writing output ...")
    write_snapshot(out_path, top, snapshot_date)
    print(f"  ✅ Wrote {len(top)} rows to {relative_path}")

    print("\n  Top 10 preview:")
    for i, (eid, name, cnt) in enumerate(top[:10], start=1):
        print(f"  {i:>2}. {name} (id {eid}) — {cnt:,} applied edits")

    update_manifest(folder, snapshot_date, relative_path)

    # --- Cleanup ---
    print("\n[5/5] Cleanup ...")
    cleanup(folder)

    print(f"\n✅ All done!")

    # --- GitHub push ---
    sys.stdout.write("\nPush to GitHub? (y/n): ")
    sys.stdout.flush()
    answer = input().strip().lower()
    if answer == "y":
        push_to_github(folder, relative_path)
    else:
        print(f"  Skipped. Remember to push {relative_path} and manifest.json manually.")


if __name__ == "__main__":
    main()
