"""
Streaming JSON profiler using ijson. Does NOT load the full file into memory.

Handles BOM, invalid leading bytes, and NDJSON vs single-JSON. Identifies
top-level structure and candidate record paths (arrays of objects) for use
with 50_extract_json_to_ndjson.py --item_path.

Usage:
  python ingestion/41_profile_json_stream.py "path\\to\\file.json" --max-events 300000 --sample-records 3
"""
import argparse
import json
import os
import sys

import ijson

UTF8_BOM = b"\xef\xbb\xbf"
MAX_NDJSON_LINES = 2000


def find_json_start_offset(file_path: str, max_scan_bytes: int = 1024 * 1024) -> int | None:
    """
    Read up to max_scan_bytes; skip UTF-8 BOM and leading whitespace/control chars;
    return byte offset of first b'{' or b'[', or None if not found.
    """
    with open(file_path, "rb") as f:
        chunk = f.read(max_scan_bytes)
    if not chunk:
        return None
    offset = 0
    if chunk.startswith(UTF8_BOM):
        offset = len(UTF8_BOM)
    while offset < len(chunk):
        b = chunk[offset]
        if b in (ord(b"{"), ord(b"[")):
            return offset
        if b >= 0x20 or b in (ord(b"\t"), ord(b"\n"), ord(b"\r")):
            offset += 1
        else:
            offset += 1
    return None


def _format_size(n: int) -> str:
    if n >= 1 << 30:
        return f"{n / (1 << 30):.2f} GB"
    if n >= 1 << 20:
        return f"{n / (1 << 20):.2f} MB"
    if n >= 1 << 10:
        return f"{n / (1 << 10):.2f} KB"
    return f"{n} B"


def _profile_ndjson(path: str, n_lines: int = MAX_NDJSON_LINES) -> None:
    """Read first N lines; treat as NDJSON; show sample keys and count."""
    all_keys: set[str] = set()
    json_line_count = 0
    with open(path, "r", encoding="utf-8-sig", errors="replace") as f:
        for i, line in enumerate(f):
            if i >= n_lines:
                break
            line = line.strip()
            if not line or not line.startswith("{"):
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    json_line_count += 1
                    all_keys.update(obj.keys())
            except json.JSONDecodeError:
                continue
    print("Detected mode: ndjson")
    print(f"File looks like NDJSON/JSONL. JSON lines scanned (first {n_lines}): {json_line_count}")
    if all_keys:
        sample = sorted(all_keys)[:20]
        print(f"Sample keys from scanned lines: {sample}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream-profile a JSON file with ijson.")
    parser.add_argument("json_path", help="Path to the JSON file")
    parser.add_argument("--max-events", type=int, default=300_000, help="Stop after this many parse events (default 300000)")
    parser.add_argument("--sample-records", type=int, default=3, help="Sample keys from first N objects per path (default 3)")
    parser.add_argument("--max-scan-bytes", type=int, default=1024 * 1024, help="Bytes to scan for JSON start (default 1MB)")
    args = parser.parse_args()

    path = os.path.abspath(args.json_path)
    if not os.path.isfile(path):
        print(f"File not found: {path}", file=sys.stderr)
        sys.exit(1)

    file_size = os.path.getsize(path)
    print(f"File size: {_format_size(file_size)}")

    offset = find_json_start_offset(path, max_scan_bytes=args.max_scan_bytes)

    if offset is None:
        print("Detected mode: unknown (no single-JSON start found); attempting NDJSON detection.")
        _profile_ndjson(path, n_lines=MAX_NDJSON_LINES)
        return

    print(f"Detected mode: single_json")
    print(f"JSON start offset: {offset} bytes")

    top_level_type: str | None = None
    top_level_keys: list[str] = []
    candidate_counts: dict[str, int] = {}
    candidate_sample_keys: dict[str, list[list[str]]] = {}
    events_seen = 0
    max_events = args.max_events
    sample_n = args.sample_records
    in_object_at: str | None = None
    current_keys: list[str] = []

    with open(path, "rb") as f_bin:
        f_bin.seek(offset)
        for prefix, event, value in ijson.parse(f_bin):
            events_seen += 1
            if events_seen == 1:
                if event == "start_map":
                    top_level_type = "object"
                elif event == "start_array":
                    top_level_type = "array"
            if top_level_type == "object" and event == "map_key" and prefix == "":
                top_level_keys.append(value)

            if event == "start_map" and prefix.endswith(".item"):
                candidate_counts[prefix] = candidate_counts.get(prefix, 0) + 1
                if candidate_counts[prefix] <= sample_n:
                    in_object_at = prefix
                    current_keys = []
            if in_object_at is not None and event == "map_key" and prefix == in_object_at:
                current_keys.append(value)
            if event == "end_map" and prefix == in_object_at:
                if in_object_at not in candidate_sample_keys:
                    candidate_sample_keys[in_object_at] = []
                candidate_sample_keys[in_object_at].append(current_keys)
                in_object_at = None

            if events_seen >= max_events:
                print("\n(Stopped after --max-events; results are approximate.)")
                break

    print(f"Top-level type: {top_level_type or 'unknown'}")
    if top_level_keys:
        print(f"Top-level keys: {top_level_keys}")

    if not candidate_counts:
        print("No candidate record paths (arrays of objects) found.")
        return

    print("\nCandidate record paths (arrays of objects; use as --item_path for extraction):")
    sorted_paths = sorted(candidate_counts.keys(), key=lambda p: -candidate_counts[p])
    top10 = sorted_paths[:10]
    for p in top10:
        count = candidate_counts[p]
        samples = candidate_sample_keys.get(p, [])
        sample_str = "; ".join(sorted(set(k for keys in samples for k in keys))[:15]) if samples else "(no keys sampled)"
        print(f"  {p}")
        print(f"    count (approx): {count}")
        print(f"    sample keys: {sample_str}")

    if events_seen >= max_events:
        print("\nRe-run with a higher --max-events to refine counts and discover deeper paths.")


if __name__ == "__main__":
    main()
