import pytest
from pathlib import Path
from sim_dji_cloud.config import load_pilot_config, ConfigError


def test_load_pilot_config_full(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DJI_MQTT_USERNAME", "u")
    monkeypatch.setenv("DJI_MQTT_PASSWORD", "p")
    yaml_text = '''
mqtt:
  host: example.com
  port: 8883
  rc_sn: SN_RC_TEST
  username: "${env:DJI_MQTT_USERNAME}"
  password: "${env:DJI_MQTT_PASSWORD}"
  subscribe_patterns: ["thing/product/+/+"]
  deny_topics: []
storage:
  root: ./rec_pilot
  flush_max_records: 1000
  flush_interval_ms: 200
  queue_max_size: 10000
  rotate_max_bytes: 100
  rotate_max_records: 100
pilot_flight_detection:
  idle_debounce_seconds: 5
'''
    p = tmp_path / "pilot_rec.yaml"
    p.write_text(yaml_text)
    cfg = load_pilot_config(p)
    assert cfg["mqtt"]["username"] == "u"
    assert cfg["mqtt"]["rc_sn"] == "SN_RC_TEST"
    assert cfg["storage"]["root"] == "./rec_pilot"
    assert cfg["pilot_flight_detection"]["idle_debounce_seconds"] == 5
    assert "video" not in cfg   # pilot 配置不要求也不含 video section


def test_load_pilot_config_missing_section_raises(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text("mqtt:\n  host: x\n")
    with pytest.raises(ConfigError, match="storage"):
        load_pilot_config(p)


def test_load_pilot_config_requires_pilot_flight_detection_not_flight_detection(tmp_path: Path):
    """确认要求的是 pilot_flight_detection 而不是 dock 版的 flight_detection。"""
    p = tmp_path / "bad2.yaml"
    p.write_text('''
mqtt: {host: x}
storage: {root: ./r}
flight_detection: {record_steps: [0]}
''')
    with pytest.raises(ConfigError, match="pilot_flight_detection"):
        load_pilot_config(p)
