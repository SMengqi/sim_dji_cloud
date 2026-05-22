from click.testing import CliRunner
from sim_dji_cloud.cli import main


def test_cli_play_help():
    r = CliRunner().invoke(main, ["play", "--help"])
    assert r.exit_code == 0
    assert "flight_dir" in r.output.lower()
    assert "speed" in r.output.lower()
    assert "mqtt-url" in r.output.lower()


def test_cli_help_lists_play():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "play" in r.output


def test_cli_selfcheck_help():
    r = CliRunner().invoke(main, ["selfcheck", "--help"])
    assert r.exit_code == 0
    assert "flight_dir" in r.output.lower()
    assert "tolerance" in r.output.lower()


def test_cli_help_lists_selfcheck():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "selfcheck" in r.output


def test_cli_help_lists_list_and_repair():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "list" in r.output
    assert "repair" in r.output


def test_cli_dashboard_help():
    r = CliRunner().invoke(main, ["dashboard", "--help"])
    assert r.exit_code == 0
    out = r.output.lower()
    assert "broker" in out or "mqtt-url" in out
    assert "port" in out


def test_cli_help_lists_dashboard():
    r = CliRunner().invoke(main, ["--help"])
    assert r.exit_code == 0
    assert "dashboard" in r.output
