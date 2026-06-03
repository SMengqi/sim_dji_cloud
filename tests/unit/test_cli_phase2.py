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


def test_dashboard_cli_accepts_recordings_root():
    """sim-dji dashboard --recordings-root 选项存在。"""
    runner = CliRunner()
    result = runner.invoke(main, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "--recordings-root" in result.output


def test_dashboard_cli_accepts_log_dir():
    """sim-dji dashboard --log-dir 选项存在。"""
    from click.testing import CliRunner
    from sim_dji_cloud.cli import main
    runner = CliRunner()
    result = runner.invoke(main, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "--log-dir" in result.output


def test_dashboard_cli_accepts_default_video_push_url():
    """sim-dji dashboard --default-video-push-url 选项存在。"""
    from click.testing import CliRunner
    from sim_dji_cloud.cli import main
    runner = CliRunner()
    result = runner.invoke(main, ["dashboard", "--help"])
    assert result.exit_code == 0
    assert "--default-video-push-url" in result.output
