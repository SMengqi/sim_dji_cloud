from pathlib import Path
from sim_dji_cloud.storage.rotation import RotatingJsonlWriter


def test_rotates_on_record_count(tmp_path: Path):
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=3,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    for i in range(7):
        w.write({"i": i})
    w.close()

    files = sorted(p.name for p in tmp_path.glob("topic.*.jsonl"))
    assert files == ["topic.0001.jsonl", "topic.0002.jsonl", "topic.0003.jsonl"]


def test_rotates_on_byte_size(tmp_path: Path):
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=10**9,
        rotate_max_bytes=50,
        flush_max_records=1,
    )
    big = {"data": "x" * 30}
    for _ in range(5):
        w.write(big)
    w.close()

    files = sorted(tmp_path.glob("topic.*.jsonl"))
    assert len(files) >= 2


def test_rotation_tracks_files_metadata(tmp_path: Path):
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=2,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    w.write({"recv_ts_ms": 100, "i": 0})
    w.write({"recv_ts_ms": 200, "i": 1})
    w.write({"recv_ts_ms": 300, "i": 2})
    w.close()

    files_meta = w.files_metadata()
    assert len(files_meta) == 2
    assert files_meta[0]["name"] == "topic.0001.jsonl"
    assert files_meta[0]["count"] == 2
    assert files_meta[0]["first_ms"] == 100
    assert files_meta[0]["last_ms"] == 200
    assert files_meta[1]["name"] == "topic.0002.jsonl"
    assert files_meta[1]["count"] == 1


def test_no_premature_rotation(tmp_path: Path):
    """Records below both thresholds → exactly 1 volume."""
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=10,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    for i in range(5):
        w.write({"recv_ts_ms": i * 100, "i": i})
    w.close()
    files = sorted(tmp_path.glob("topic.*.jsonl"))
    assert len(files) == 1
    assert files[0].name == "topic.0001.jsonl"
    # verify line count in the single volume
    assert sum(1 for _ in files[0].open()) == 5


def test_close_without_writes_yields_empty_volume_meta(tmp_path: Path):
    """Closing without any writes should still produce a 1-item metadata list."""
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=10,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    w.close()
    meta = w.files_metadata()
    assert len(meta) == 1
    assert meta[0]["count"] == 0
    assert meta[0]["first_ms"] is None
    assert meta[0]["last_ms"] is None


def test_files_metadata_returns_defensive_copy(tmp_path: Path):
    """Mutating the returned list/dicts must not affect internal state."""
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=2,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    w.write({"recv_ts_ms": 100, "i": 0})
    w.write({"recv_ts_ms": 200, "i": 1})
    w.close()

    meta1 = w.files_metadata()
    meta1[0]["count"] = 999  # mutate
    meta1.append({"name": "fake"})  # mutate list
    # re-fetch — should be unaffected
    meta2 = w.files_metadata()
    assert len(meta2) == 1
    assert meta2[0]["count"] == 2
    assert meta2[0]["name"] == "topic.0001.jsonl"


def test_volume_files_contain_correct_records(tmp_path: Path):
    """Verify each volume has the expected number and content of records."""
    base = tmp_path / "topic"
    w = RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=3,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    )
    for i in range(7):
        w.write({"recv_ts_ms": i, "i": i})
    w.close()

    vol1_lines = (tmp_path / "topic.0001.jsonl").read_text().splitlines()
    vol2_lines = (tmp_path / "topic.0002.jsonl").read_text().splitlines()
    vol3_lines = (tmp_path / "topic.0003.jsonl").read_text().splitlines()
    assert len(vol1_lines) == 3
    assert len(vol2_lines) == 3
    assert len(vol3_lines) == 1


def test_context_manager_finalizes_on_exit(tmp_path: Path):
    """Using `with RotatingJsonlWriter(...) as w:` should auto-close."""
    base = tmp_path / "topic"
    with RotatingJsonlWriter(
        base_path=base,
        rotate_max_records=10,
        rotate_max_bytes=10**9,
        flush_max_records=1,
    ) as w:
        w.write({"recv_ts_ms": 100, "i": 0})
    # outside the `with`, file should be closed and meta available
    meta = w.files_metadata()
    assert len(meta) == 1
    assert meta[0]["count"] == 1
