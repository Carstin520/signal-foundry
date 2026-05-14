from typer.testing import CliRunner

from quant_sol.wallets.cli import app


def test_cli_help_lists_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "fetch" in result.stdout
    assert "analyze" in result.stdout
    assert "report" in result.stdout

