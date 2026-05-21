import pytest
from pathlib import Path
from sim_dji_cloud.config import load_config, ConfigError, substitute_env

def test_substitute_env_replaces_placeholder(monkeypatch):
    monkeypatch.setenv("FOO", "bar")
    assert substitute_env("${env:FOO}") == "bar"
    assert substitute_env("prefix-${env:FOO}-suffix") == "prefix-bar-suffix"

def test_substitute_env_raises_on_missing(monkeypatch):
    monkeypatch.delenv("MISSING_VAR", raising=False)
    with pytest.raises(ConfigError, match="MISSING_VAR"):
        substitute_env("${env:MISSING_VAR}")

def test_substitute_env_recurses_nested_dict(monkeypatch):
    monkeypatch.setenv("USER", "u1")
    monkeypatch.setenv("PASS", "p1")
    cfg = {"mqtt": {"username": "${env:USER}", "password": "${env:PASS}", "port": 8883}}
    out = substitute_env(cfg)
    assert out == {"mqtt": {"username": "u1", "password": "p1", "port": 8883}}

def test_load_config_full(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("DJI_MQTT_USERNAME", "u")
    monkeypatch.setenv("DJI_MQTT_PASSWORD", "p")
    yaml_text = '''
mqtt:
  host: example.com
  port: 8883
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
  enabled: true
flight_detection:
  finalize_idle_seconds: 30
  rules:
    start: []
    end: []
'''
    p = tmp_path / "rec.yaml"
    p.write_text(yaml_text)
    cfg = load_config(p)
    assert cfg["mqtt"]["username"] == "u"
    assert cfg["mqtt"]["password"] == "p"
    assert cfg["storage"]["root"] == "./rec"

def test_load_config_missing_section_raises(tmp_path: Path):
    p = tmp_path / "bad.yaml"
    p.write_text("mqtt:\n  host: x\n")
    with pytest.raises(ConfigError, match="storage"):
        load_config(p)
