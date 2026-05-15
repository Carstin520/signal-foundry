from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.models import SignalScore
from quant_sol.signals.telegram import TelegramClient, format_alert


def test_signals_cli_help_lists_commands() -> None:
    result = CliRunner().invoke(app, ["--help"])

    assert result.exit_code == 0
    assert "discover-markets" in result.stdout
    assert "sync-social" in result.stdout
    assert "alert" in result.stdout
    assert "evaluate" in result.stdout
    assert "rank-accounts" in result.stdout
    assert "sync-accounts" in result.stdout
    assert "export-account-seeds" in result.stdout


def test_telegram_missing_config_returns_warning(monkeypatch) -> None:
    monkeypatch.delenv("TELEGRAM_BOT_TOKEN", raising=False)
    monkeypatch.delenv("TELEGRAM_CHAT_ID", raising=False)
    client = TelegramClient(bot_token=None, chat_id=None)
    status, error = client.send_message("hello")

    assert status == "missing_config"
    assert "TELEGRAM_BOT_TOKEN" in (error or "")


def test_format_alert_contains_required_sections() -> None:
    signal = SignalScore(
        signal_id="s1",
        event_family="iran",
        market_slug="us-strikes-iran-by",
        direction_hint="yes_down",
        score=82,
        confidence="high",
        evidence={},
        risk_tags=["wide_spread"],
        source_posts=[{"handle": "WhiteHouse", "created_at": "2026-05-15T12:00:00+00:00", "text": "Iran update", "url": "https://x.com/x/status/1"}],
        wallet_flows=[{"wallet": "GCottrell93", "side": "SELL", "notional": 12000, "activity_ts": "2026-05-15T12:01:00+00:00"}],
        price_window={
            "current_market_probability": 0.24,
            "narrative_direction": "bearish",
            "narrative_velocity": 18,
            "market_move_6h": 0.01,
            "market_move_24h": 0.02,
            "deadline_days": 42,
            "confirmation_status": "unconfirmed",
            "fomo_capacity": 20,
            "spread": 0.02,
            "liquidity": 50000,
            "price_band": "ideal",
        },
    )

    alert = format_alert(signal)

    assert "[FOMO Divergence]" in alert
    assert "Market probability" in alert
    assert "FOMO capacity" in alert
    assert "Wallet flow" in alert
    assert "Risk tags" in alert
