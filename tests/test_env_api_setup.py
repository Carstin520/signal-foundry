from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.env import load_local_env, masked_secret


def test_load_local_env_does_not_override_shell_env(tmp_path, monkeypatch) -> None:
    env_path = tmp_path / ".env"
    env_path.write_text("X_BEARER_TOKEN=file-token\nTELEGRAM_CHAT_ID='12345'\n", encoding="utf-8")
    monkeypatch.setenv("X_BEARER_TOKEN", "shell-token")
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)

    loaded = load_local_env(env_path)

    assert "X_BEARER_TOKEN" not in loaded
    assert loaded["TELEGRAM_CHAT_ID"] == "12345"
    assert masked_secret("X_BEARER_TOKEN") == "shel...oken"


def test_check_api_without_token_prints_setup_hint(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("X_BEARER_TOKEN", raising=False)

    result = CliRunner().invoke(app, ["check-api", "--service", "x", "--no-call"])

    assert result.exit_code == 0
    assert "X_BEARER_TOKEN is missing" in result.stdout
    assert ".env.example" in result.stdout


def test_check_api_no_call_uses_env_file(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("X_BEARER_TOKEN", raising=False)
    (tmp_path / ".env").write_text('X_BEARER_TOKEN="test-bearer-token"\n', encoding="utf-8")

    result = CliRunner().invoke(app, ["check-api", "--service", "x", "--no-call"])

    assert result.exit_code == 0
    assert "X_BEARER_TOKEN configured" in result.stdout
    assert "Skipped external API call" in result.stdout


def test_sync_accounts_dry_run_estimates_x_api_usage(tmp_path, monkeypatch) -> None:
    _write_minimal_api_configs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("X_BEARER_TOKEN", "test-bearer-token")

    result = CliRunner().invoke(app, ["sync-accounts", "--watchlist", "web3", "--dry-run"])

    assert result.exit_code == 0
    assert "planned_calls=6" in result.stdout
    assert "accounts=2" in result.stdout
    assert "No X API calls made" in result.stdout


def test_sync_accounts_daily_cap_blocks_run_before_api_call(tmp_path, monkeypatch) -> None:
    _write_minimal_api_configs(tmp_path)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("X_BEARER_TOKEN", "test-bearer-token")

    result = CliRunner().invoke(app, ["sync-accounts", "--watchlist", "web3", "--daily-cap", "1"])

    assert result.exit_code == 0
    assert "would exceed" in result.stdout
    assert "local daily cap" in result.stdout


def _write_minimal_api_configs(root) -> None:
    config = root / "config"
    config.mkdir()
    (config / "web3_account_watchlist.yaml").write_text(
        """
accounts:
  - handle: alpha
    language: en
    region: global
    role: originators
    priority: seed
  - handle: beta
    language: zh
    region: cn
    role: fast_curators
    priority: seed
""".strip(),
        encoding="utf-8",
    )
    (config / "api_limits.yaml").write_text(
        """
x:
  daily_call_cap: 20
  sync_accounts:
    max_accounts: 2
    max_posts_per_account: 7
""".strip(),
        encoding="utf-8",
    )
