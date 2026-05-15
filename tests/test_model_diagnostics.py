from datetime import datetime, timedelta, timezone

from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.diagnostics import model_diagnostics, write_model_diagnostics
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.storage import connect, insert_market_tick, upsert_markets, upsert_x_accounts, upsert_x_posts


def test_model_diagnostics_flags_missing_price_outcomes(tmp_path) -> None:
    con = connect(tmp_path / "diagnostics.duckdb")
    now = datetime.now(timezone.utc).replace(microsecond=0)
    upsert_x_accounts(
        con,
        [{"handle": "FastAccount", "language": "en", "region": "global", "role": "fast_curators", "priority": "seed", "status": "active"}],
    )
    upsert_x_posts(
        con,
        [
            {
                "post_id": "p1",
                "handle": "FastAccount",
                "created_at": (now - timedelta(hours=1)).isoformat(),
                "text": "Trump nomination odds are lagging the donor chatter",
                "public_metrics": {},
                "referenced_tweets": [],
                "lang": "en",
                "raw_json": {},
            }
        ],
    )
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="will-trump-be-republican-nominee",
                event_slug="trump-nominee",
                question="Will Trump be the Republican nominee?",
                category="Politics",
                tags=["Politics", "Trump"],
                end_time=(now + timedelta(days=60)).isoformat(),
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=100_000,
                raw={},
            )
        ],
    )

    from quant_sol.signals.accounts import rank_accounts

    rank_accounts(con, "30d")
    diagnostics = model_diagnostics(con)

    assert diagnostics["counts"]["account_market_mentions"] == 1
    assert "matched_posts_without_price_outcomes" in diagnostics["blockers"]
    assert "insufficient_tick_history_for_backtest" in diagnostics["blockers"]


def test_model_diagnostics_report_and_cli(tmp_path, monkeypatch) -> None:
    con = connect(tmp_path / "diagnostics.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="hype-market",
                event_slug="hype",
                question="Will HYPE rally?",
                category="Crypto",
                tags=["Crypto"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=50_000,
                raw={},
            )
        ],
    )
    insert_market_tick(con, "2026-05-15T00:00:00+00:00", "hype-market", "yes-token", 0.2, 0.22, None, 50_000, {})

    path = write_model_diagnostics(con, tmp_path, date="2026-05-15")

    assert path.exists()
    assert "Signal Foundry Model Diagnostics" in path.read_text(encoding="utf-8")

    monkeypatch.chdir(tmp_path)
    result = CliRunner().invoke(app, ["diagnose-model", "--date", "2026-05-15"])

    assert result.exit_code == 0
    assert "Wrote model diagnostics" in result.stdout


def test_collect_market_ticks_dry_run_is_bounded(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="hype-market",
                event_slug="hype",
                question="Will HYPE rally?",
                category="Crypto",
                tags=["Crypto"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=50_000,
                raw={},
            )
        ],
    )

    result = CliRunner().invoke(
        app,
        [
            "collect-market-ticks",
            "--category",
            "all",
            "--max-markets",
            "1",
            "--iterations",
            "3",
            "--interval-seconds",
            "0",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "planned_calls=3" in result.stdout
    assert "Dry run only" in result.stdout
