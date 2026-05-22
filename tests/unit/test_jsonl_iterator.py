import json
from pathlib import Path
from sim_dji_cloud.player.jsonl_iterator import JsonlIterator


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, separators=(",", ":")) + "\n")


def test_iterates_single_file_in_order(tmp_path: Path):
    f = tmp_path / "topic.0001.jsonl"
    recs = [{"recv_ts_ms": i, "i": i} for i in range(10)]
    _write_jsonl(f, recs)

    out = list(JsonlIterator([f]))
    assert len(out) == 10
    assert out[0]["i"] == 0
    assert out[-1]["i"] == 9


def test_iterates_multiple_volumes_in_given_order(tmp_path: Path):
    f1 = tmp_path / "topic.0001.jsonl"
    f2 = tmp_path / "topic.0002.jsonl"
    _write_jsonl(f1, [{"recv_ts_ms": i, "i": i} for i in range(3)])
    _write_jsonl(f2, [{"recv_ts_ms": i + 100, "i": i + 3} for i in range(3)])

    out = list(JsonlIterator([f1, f2]))
    assert [r["i"] for r in out] == [0, 1, 2, 3, 4, 5]


def test_start_offset_byte_skips_earlier_records(tmp_path: Path):
    f = tmp_path / "topic.0001.jsonl"
    _write_jsonl(f, [{"recv_ts_ms": i * 100, "i": i} for i in range(10)])

    offset = 0
    with f.open("rb") as fp:
        for i in range(5):
            fp.readline()
        offset = fp.tell()

    out = list(JsonlIterator([f], start_offset_first_file=offset))
    assert out[0]["i"] == 5


def test_skips_blank_lines(tmp_path: Path):
    f = tmp_path / "t.jsonl"
    f.write_text('{"recv_ts_ms":1,"i":1}\n\n{"recv_ts_ms":2,"i":2}\n')
    out = list(JsonlIterator([f]))
    assert len(out) == 2


def test_malformed_json_raises_strict(tmp_path: Path):
    """Pin: malformed JSON 不静默跳过，会抛 JSONDecodeError，立即中断迭代。
    Phase 2 数据由 Recorder 自己写入，应该是良构的；遇到坏行就该出错，
    暴露问题而不是悄悄丢数据。"""
    import json
    f = tmp_path / "bad.jsonl"
    f.write_text('{"recv_ts_ms":1,"i":1}\nNOT_JSON\n{"recv_ts_ms":2,"i":2}\n')
    it = iter(JsonlIterator([f]))
    assert next(it) == {"recv_ts_ms": 1, "i": 1}
    try:
        next(it)
    except json.JSONDecodeError:
        pass
    else:
        assert False, "expected JSONDecodeError on malformed line"


def test_missing_file_yields_nothing(tmp_path: Path):
    out = list(JsonlIterator([tmp_path / "nonexistent.jsonl"]))
    assert out == []
