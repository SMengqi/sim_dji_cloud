import json
from pathlib import Path
from sim_dji_cloud.player.seek_index import SeekIndex


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def test_build_index_sparse_anchors(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    records = [{"recv_ts_ms": i * 100, "i": i} for i in range(250)]
    _write_jsonl(f, records)

    idx = SeekIndex.from_jsonl(f, anchor_every=100)
    anchors = idx.anchors()
    assert len(anchors) == 3
    assert anchors[0] == (0, 0)
    assert anchors[1][0] == 10_000
    assert anchors[2][0] == 20_000


def test_seek_offset_for_time_returns_largest_anchor_le_target(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    records = [{"recv_ts_ms": i * 100, "i": i} for i in range(250)]
    _write_jsonl(f, records)

    idx = SeekIndex.from_jsonl(f, anchor_every=100)
    assert idx.seek_offset_for_time(5_000) == 0
    assert idx.seek_offset_for_time(10_000) > 0
    last_offset = idx.seek_offset_for_time(25_000)
    assert last_offset == idx.anchors()[2][1]


def test_seek_before_first_record_returns_zero(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    _write_jsonl(f, [{"recv_ts_ms": 1000, "i": 0}])
    idx = SeekIndex.from_jsonl(f, anchor_every=100)
    assert idx.seek_offset_for_time(0) == 0
    assert idx.seek_offset_for_time(-9999) == 0


def test_empty_file(tmp_path: Path):
    f = tmp_path / "empty.jsonl"
    f.touch()
    idx = SeekIndex.from_jsonl(f, anchor_every=100)
    assert idx.anchors() == []
    assert idx.seek_offset_for_time(0) == 0
