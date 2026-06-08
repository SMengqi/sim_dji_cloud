from pathlib import Path
from sim_dji_cloud.tools.validate_config_cmd import validate_config_file


def test_validates_good_config(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.setenv("DJI_MQTT_USERNAME", "u")
    monkeypatch.setenv("DJI_MQTT_PASSWORD", "p")
    p = tmp_path / "ok.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: true
  client_id: c
  dock_sn: SN_TEST
  username: "${env:DJI_MQTT_USERNAME}"
  password: "${env:DJI_MQTT_PASSWORD}"
  subscribe_patterns: ["thing/product/+/+"]
  deny_topics: []
storage:
  root: ./rec
  flush_max_records: 1000
  flush_interval_ms: 200
  queue_max_size: 10000
  rotate_max_bytes: 100
  rotate_max_records: 100
video:
  enabled: false
flight_detection:
  record_steps: [0, 1, 2]
  idle_debounce_seconds: 5
''')
    code = validate_config_file(p)
    captured = capsys.readouterr()
    assert code == 0
    assert "OK" in captured.out


def test_rejects_missing_env(tmp_path: Path, monkeypatch, capsys):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    p = tmp_path / "bad.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: false
  client_id: c
  username: "${env:MISSING_VAR}"
  password: null
  subscribe_patterns: []
  deny_topics: []
storage: {root: ./rec, flush_max_records: 1, flush_interval_ms: 1, queue_max_size: 1, rotate_max_bytes: 1, rotate_max_records: 1}
video: {enabled: false}
flight_detection: {record_steps: [0, 1, 2], idle_debounce_seconds: 5}
''')
    code = validate_config_file(p)
    assert code != 0
    captured = capsys.readouterr()
    assert "MISSING_VAR" in captured.err


def test_rejects_unreadable_ca_file(tmp_path: Path, capsys):
    p = tmp_path / "bad_ca.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: true
  client_id: c
  dock_sn: SN_TEST
  username: null
  password: null
  ca_file: /nonexistent/ca.pem
  subscribe_patterns: []
  deny_topics: []
storage: {root: ./rec, flush_max_records: 1, flush_interval_ms: 1, queue_max_size: 1, rotate_max_bytes: 1, rotate_max_records: 1}
video: {enabled: false}
flight_detection: {record_steps: [0, 1, 2], idle_debounce_seconds: 5}
''')
    code = validate_config_file(p)
    assert code != 0
    captured = capsys.readouterr()
    assert "ca_file" in captured.err or "nonexistent" in captured.err


def test_rejects_missing_dock_sn(tmp_path: Path, capsys):
    """缺 mqtt.dock_sn 必须在 validate-config 阶段被拒，而不是 record 启动深处崩。

    Regression (review MAJOR): 旧实现只校验 top-level section 存在，sub-key
    缺失要等 record 起飞时才抛 KeyError，用户排查困难。
    """
    p = tmp_path / "no_dock.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: true
  client_id: c
  username: null
  password: null
  subscribe_patterns: []
  deny_topics: []
storage: {root: ./rec, flush_max_records: 1, flush_interval_ms: 1, queue_max_size: 1, rotate_max_bytes: 1, rotate_max_records: 1}
video: {enabled: false}
flight_detection: {record_steps: [0, 1, 2], idle_debounce_seconds: 5}
''')
    code = validate_config_file(p)
    assert code != 0
    captured = capsys.readouterr()
    assert "mqtt.dock_sn" in captured.err


def test_rejects_missing_storage_root(tmp_path: Path, capsys):
    """缺 storage.root 必须在 validate-config 阶段被拒。"""
    p = tmp_path / "no_root.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: true
  client_id: c
  dock_sn: SN_TEST
  username: null
  password: null
  subscribe_patterns: []
  deny_topics: []
storage: {flush_max_records: 1, flush_interval_ms: 1, queue_max_size: 1, rotate_max_bytes: 1, rotate_max_records: 1}
video: {enabled: false}
flight_detection: {record_steps: [0, 1, 2], idle_debounce_seconds: 5}
''')
    code = validate_config_file(p)
    assert code != 0
    captured = capsys.readouterr()
    assert "storage.root" in captured.err
