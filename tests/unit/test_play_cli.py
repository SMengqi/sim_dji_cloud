def test_play_cli_accepts_control_sidecar_path():
    """sim-dji play --help 含 --control-sidecar-path。"""
    from click.testing import CliRunner
    from sim_dji_cloud.cli import main
    runner = CliRunner()
    result = runner.invoke(main, ["play", "--help"])
    assert result.exit_code == 0
    assert "--control-sidecar-path" in result.output
