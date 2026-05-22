"""Anonymize a captured flight directory into a fixture.

Usage:
    python scripts/anonymize_flight.py <flight_dir> <output_dir>
"""
import json
import sys
from pathlib import Path


REPLACEMENTS = {}


def anonymize_str(s: str) -> str:
    for k, v in REPLACEMENTS.items():
        s = s.replace(k, v)
    return s


def anonymize_obj(obj):
    if isinstance(obj, str):
        return anonymize_str(obj)
    if isinstance(obj, dict):
        return {anonymize_str(k): anonymize_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [anonymize_obj(v) for v in obj]
    return obj


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    src = Path(sys.argv[1])
    dst = Path(sys.argv[2])

    manifest = json.loads((src / "manifest.json").read_text())
    REPLACEMENTS[manifest["dock_sn"]] = "SN_DOCK_FIXTURE"
    REPLACEMENTS[manifest["drone_sn"]] = "SN_DRONE_FIXTURE"
    REPLACEMENTS[manifest["task_id"]] = "T-FIXTURE-001"

    dst.mkdir(parents=True, exist_ok=True)
    (dst / "topics").mkdir(exist_ok=True)

    new_manifest = anonymize_obj(manifest)
    (dst / "manifest.json").write_text(
        json.dumps(new_manifest, ensure_ascii=False, indent=2)
    )

    for topic_entry in manifest["topics"]:
        for f in topic_entry["files"]:
            src_path = src / f["name"]
            new_name = anonymize_str(f["name"])
            dst_path = dst / new_name
            dst_path.parent.mkdir(parents=True, exist_ok=True)
            with src_path.open("r", encoding="utf-8") as inp, \
                 dst_path.open("w", encoding="utf-8") as out:
                for line in inp:
                    line = line.strip()
                    if not line:
                        continue
                    rec = json.loads(line)
                    rec = anonymize_obj(rec)
                    out.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
    print(f"anonymized → {dst}")


if __name__ == "__main__":
    main()
