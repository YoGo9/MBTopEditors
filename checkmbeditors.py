#!/usr/bin/env python3
r"""
Reads MusicBrainz dump files and produces top_editors.csv (top 500 by
total APPLIED edits).

Usage:
    python mb_top_editors_overall.py <folder>

<folder> should contain either:
  - Extracted dumps in:
        <folder>/mbdump-editor/mbdump/editor_sanitised   (or .../editor)
        <folder>/mbdump-edit/mbdump/edit
    OR
  - The compressed dump files:
        <folder>/mbdump-editor.tar.bz2
        <folder>/mbdump-edit.tar.bz2

If the extracted files are missing, this script will automatically extract
ONLY the needed files from the .tar.bz2 archives.
"""

import os
import sys
import glob
import tarfile
from collections import defaultdict

APPLIED_STATUS_CODE = 2  # MusicBrainz STATUS_APPLIED


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
            if not line or line[0] in ("-", "C"):
                pass
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
    """
    edit columns (0-based): 0=id, 1=editor_id, 2=type, 3=status, 4=autoedit,
                            5=open_time, 6=close_time, 7=expire_time,
                            8=language, 9=quality
    """
    counts = defaultdict(int)
    with open(edit_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.rstrip("\n").split("\t")
            if len(parts) < 4:
                continue
            eid_raw = unescape_pg(parts[1])    # editor_id
            status_raw = unescape_pg(parts[3]) # status
            if eid_raw is None or status_raw is None:
                continue
            try:
                if int(status_raw) == APPLIED_STATUS_CODE:
                    counts[int(eid_raw)] += 1
            except Exception:
                continue
    return counts


def write_csv(path: str, rows):
    with open(path, "w", encoding="utf-8", newline="") as f:
        f.write("rank,editor_id,editor_name,applied_edit_count\n")
        for i, (eid, name, cnt) in enumerate(rows, start=1):
            safe = (name or "").replace('"', '""')
            if any(ch in safe for ch in [",", "\n", "\r", '"']):
                safe = f'"{safe}"'
            f.write(f"{i},{eid},{safe},{cnt}\n")


def _find_tar(base_folder: str, prefix: str) -> str | None:
    """
    Look for a tar.bz2 file whose name starts with prefix in base_folder.
    e.g. prefix='mbdump-edit' -> 'mbdump-edit.tar.bz2'
    """
    pattern = os.path.join(base_folder, prefix + "*.tar.bz2")
    matches = sorted(glob.glob(pattern))
    return matches[0] if matches else None


def _extract_members(tar_path: str, target_root: str, wanted: list[str]):
    """
    Extract only specific members from tar_path into target_root.
    Members are specified with the paths used inside the tar archive
    (e.g. 'mbdump/edit').
    """
    print(f"Extracting from {os.path.basename(tar_path)} ...")
    os.makedirs(target_root, exist_ok=True)
    with tarfile.open(tar_path, "r:bz2") as tf:
        members_by_name = {m.name: m for m in tf.getmembers()}
        for name in wanted:
            m = members_by_name.get(name)
            if m is None:
                # try to be forgiving if there is an extra leading path, etc.
                for cand_name, cand in members_by_name.items():
                    if cand_name.endswith(name):
                        m = cand
                        break
            if m is None:
                print(f"  ! Warning: member {name} not found in {tar_path}")
                continue
            print(f"  - {m.name}")
            tf.extract(m, path=target_root)


def ensure_required_files(folder: str) -> tuple[str, str]:
    """
    Make sure editor_file and edit_file exist, extracting from tar.bz2 if needed.
    Returns (editor_file_path, edit_file_path).
    """
    editor_dir = os.path.join(folder, "mbdump-editor", "mbdump")
    edit_dir   = os.path.join(folder, "mbdump-edit", "mbdump")

    editor_file = os.path.join(editor_dir, "editor_sanitised")
    if not os.path.isfile(editor_file):
        editor_file = os.path.join(editor_dir, "editor")

    edit_file = os.path.join(edit_dir, "edit")

    # If missing, try to extract from tarballs.
    if not os.path.isfile(editor_file):
        tar_path = _find_tar(folder, "mbdump-editor")
        if not tar_path:
            sys.exit(
                "Missing editor file and mbdump-editor.tar.bz2.\n"
                "Expected either:\n"
                f"  {editor_file}\n"
                "or a tarball like:\n"
                f"  {os.path.join(folder, 'mbdump-editor.tar.bz2')}"
            )
        _extract_members(
            tar_path,
            target_root=os.path.join(folder, "mbdump-editor"),
            wanted=["mbdump/editor_sanitised", "mbdump/editor"],
        )
        # Re-resolve after extraction
        editor_file = os.path.join(editor_dir, "editor_sanitised")
        if not os.path.isfile(editor_file):
            editor_file = os.path.join(editor_dir, "editor")

    if not os.path.isfile(edit_file):
        tar_path = _find_tar(folder, "mbdump-edit")
        if not tar_path:
            sys.exit(
                "Missing edit file and mbdump-edit.tar.bz2.\n"
                "Expected either:\n"
                f"  {edit_file}\n"
                "or a tarball like:\n"
                f"  {os.path.join(folder, 'mbdump-edit.tar.bz2')}"
            )
        _extract_members(
            tar_path,
            target_root=os.path.join(folder, "mbdump-edit"),
            wanted=["mbdump/edit", "mbdump/edit_data"],
        )
        edit_file = os.path.join(edit_dir, "edit")

    if not os.path.isfile(editor_file) or not os.path.isfile(edit_file):
        sys.exit(
            "Missing required files even after extraction.\n"
            f"editor_file: {editor_file}\n"
            f"edit_file:   {edit_file}"
        )

    return editor_file, edit_file


def main(folder: str):
    editor_file, edit_file = ensure_required_files(folder)

    print(f"Reading editors: {editor_file}")
    editors = load_editors(editor_file)
    print(f"Loaded {len(editors):,} editors.")

    print(f"Counting *applied* edits: {edit_file}")
    counts = count_applied_edits(edit_file)
    print(f"Counted applied edits for {len(counts):,} editors.")

    rows = [
        (eid, editors.get(eid, f"(editor #{eid})"), cnt)
        for eid, cnt in counts.items()
    ]
    rows.sort(key=lambda x: x[2], reverse=True)
    top = rows[:500]

    out_csv = os.path.join(folder, "top_editors.csv")
    write_csv(out_csv, top)
    print(f"\n✅ Done. Wrote {len(top)} rows to:\n  {out_csv}")

    print("\nTop 10 preview:")
    for i, (eid, name, cnt) in enumerate(top[:10], start=1):
        print(f"{i:>2}. {name} (id {eid}) — {cnt:,} applied edits")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python mb_top_editors_overall.py <folder>")
        sys.exit(1)
    main(os.path.abspath(sys.argv[1]))
