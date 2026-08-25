import json
from click.testing import CliRunner
from sim_dji_cloud.cli import main


def test_cli_help_lists_subcommands():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "record" in r.output
    assert "stop-record" in r.output
    assert "inspect" in r.output
    assert "validate-config" in r.output


def test_cli_validate_config_invokes_validator(tmp_path, monkeypatch):
    monkeypatch.setenv("U", "u")
    monkeypatch.setenv("P", "p")
    p = tmp_path / "ok.yaml"
    p.write_text('''
mqtt:
  host: x
  port: 8883
  tls: false
  client_id: c
  dock_sn: SN_TEST
  username: "${env:U}"
  password: "${env:P}"
  subscribe_patterns: ["thing/product/+/+"]
  deny_topics: []
storage:
  root: ./rec
  flush_max_records: 1
  flush_interval_ms: 1
  queue_max_size: 1
  rotate_max_bytes: 1
  rotate_max_records: 1
video: {enabled: false}
flight_detection:
  finalize_idle_seconds: 1
  rules: {start: [], end: []}
''')
    r = CliRunner().invoke(main, ["validate-config", "--config", str(p)])
    assert r.exit_code == 0
    assert "OK" in r.output


def test_cli_inspect_invokes_inspector(tmp_path):
    flight = tmp_path / "T1__SN__20260519-100000"
    flight.mkdir()
    (flight / "manifest.json").write_text(json.dumps({
        "schema_version": 1, "status": "ok", "finalize_reason": "auto_idle",
        "task_id": "T1", "dock_sn": "SN", "drone_sn": "DR",
        "started_at_recv_ms": 0, "ended_at_recv_ms": 1000,
        "gaps": [], "topics": [], "video": None,
    }))
    r = CliRunner().invoke(main, ["inspect", str(flight)])
    assert r.exit_code == 0
    assert "T1" in r.output


def test_cli_stop_record_invokes(tmp_path):
    r = CliRunner().invoke(main, [
        "stop-record", "T1", "--state-dir", str(tmp_path),
    ])
    assert r.exit_code == 0


def test_cli_help_lists_record_pilot():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "record-pilot" in r.output
