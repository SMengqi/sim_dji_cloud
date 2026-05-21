from pathlib import Path
from sim_dji_cloud.storage.jsonl import JsonlWriter, JsonlReader

def test_writer_appends_lines_and_reader_iterates_them(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f)
    w.write({"recv_ts_ms": 1, "topic": "a", "payload": {"x": 1}})
    w.write({"recv_ts_ms": 2, "topic": "a", "payload": {"x": 2}})
    w.flush()
    w.close()

    rows = list(JsonlReader(f))
    assert len(rows) == 2
    assert rows[0]["recv_ts_ms"] == 1
    assert rows[1]["payload"]["x"] == 2

def test_writer_batch_flush_threshold(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f, flush_max_records=3)
    w.write({"i": 1})
    w.write({"i": 2})
    # Before threshold: nothing flushed to file yet
    assert sum(1 for _ in f.open()) == 0
    w.write({"i": 3})  # triggers flush
    assert sum(1 for _ in f.open()) == 3
    w.close()

def test_writer_records_count(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f)
    for i in range(5):
        w.write({"i": i})
    w.close()
    assert w.records_written == 5
    assert w.bytes_written > 0

def test_writer_fsync_on_close(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f, flush_max_records=10000)
    w.write({"i": 1})
    w.close()
    rows = list(JsonlReader(f))
    assert rows == [{"i": 1}]

def test_reader_skips_blank_lines(tmp_path: Path):
    f = tmp_path / "topic.jsonl"
    f.write_text('{"a":1}\n\n{"a":2}\n')
    rows = list(JsonlReader(f))
    assert rows == [{"a": 1}, {"a": 2}]

def test_writer_preserves_non_ascii_utf8(tmp_path: Path):
    """ensure_ascii=False must keep raw UTF-8 bytes in the file."""
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f)
    w.write({"msg": "起飞中文😀"})
    w.close()
    # roundtrip
    assert list(JsonlReader(f)) == [{"msg": "起飞中文😀"}]
    # raw bytes contain UTF-8, not \uXXXX escapes
    raw = f.read_bytes()
    assert "起飞".encode("utf-8") in raw

def test_writer_uses_compact_separators(tmp_path: Path):
    """separators=(',', ':') must produce no spaces."""
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f)
    w.write({"a": 1, "b": 2})
    w.close()
    assert f.read_bytes() == b'{"a":1,"b":2}\n'

def test_writer_appends_to_existing_file(tmp_path: Path):
    """Opening for write twice must append, not truncate."""
    f = tmp_path / "topic.jsonl"
    w1 = JsonlWriter(f)
    w1.write({"i": 1})
    w1.close()
    w2 = JsonlWriter(f)
    w2.write({"i": 2})
    w2.close()
    assert list(JsonlReader(f)) == [{"i": 1}, {"i": 2}]

def test_writer_close_is_idempotent(tmp_path: Path):
    """Calling close twice must not raise."""
    f = tmp_path / "topic.jsonl"
    w = JsonlWriter(f)
    w.write({"i": 1})
    w.close()
    w.close()  # should not raise
    assert list(JsonlReader(f)) == [{"i": 1}]

def test_writer_works_as_context_manager(tmp_path: Path):
    """with JsonlWriter(...) as w: ... should flush & close on exit."""
    f = tmp_path / "topic.jsonl"
    with JsonlWriter(f) as w:
        w.write({"i": 1})
        w.write({"i": 2})
    assert list(JsonlReader(f)) == [{"i": 1}, {"i": 2}]

def test_reader_raises_with_file_path_on_malformed_json(tmp_path: Path):
    """Malformed line should raise JSONDecodeError mentioning file path + line number."""
    import json
    f = tmp_path / "topic.jsonl"
    f.write_text('{"a":1}\n{not valid}\n{"b":2}\n')
    reader_iter = iter(JsonlReader(f))
    assert next(reader_iter) == {"a": 1}
    try:
        next(reader_iter)
    except json.JSONDecodeError as e:
        assert str(f) in str(e)
        assert ":2:" in str(e)  # line 2
    else:
        assert False, "expected JSONDecodeError on malformed line"
