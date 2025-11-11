from typer.testing import CliRunner

from mxm.pipeline.cli import app

runner = CliRunner()


def test_list():
    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    assert "justetf" in result.stdout
