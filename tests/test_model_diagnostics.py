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


def test_collect_market_ticks_can_target_event_case(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="trump-china",
                event_slug="trump-china",
                question="Will Trump visit China?",
                category="Politics",
                tags=["Politics"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token", "no-token"],
                liquidity=50_000,
                raw={},
            ),
            MarketRecord(
                market_slug="other-market",
                event_slug="other",
                question="Other market",
                category="Politics",
                tags=["Politics"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["other-token"],
                liquidity=50_000,
                raw={},
            ),
        ],
    )
    con.execute(
        """
        insert into event_cases
        (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
        values ('trump_china', 'Trump China visit', 'trump-china',
                '2026-05-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00',
                '[]', 'active', current_timestamp, current_timestamp)
        """
    )

    result = CliRunner().invoke(
        app,
        [
            "collect-market-ticks",
            "--case",
            "trump_china",
            "--iterations",
            "4",
            "--interval-seconds",
            "0",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "case=trump_china" in result.stdout
    assert "markets=1" in result.stdout
    assert "planned_calls=4" in result.stdout


def test_collect_market_burst_dry_run_is_bounded(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="trump-china",
                event_slug="trump-china",
                question="Will Trump visit China?",
                category="Politics",
                tags=["Politics"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=50_000,
                raw={},
            )
        ],
    )
    con.execute(
        """
        insert into event_cases
        (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
        values ('trump_china', 'Trump China visit', 'trump-china',
                '2026-05-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00',
                '[]', 'active', current_timestamp, current_timestamp)
        """
    )

    result = CliRunner().invoke(
        app,
        [
            "collect-market-burst",
            "--case",
            "trump_china",
            "--fast-seconds",
            "2",
            "--medium-seconds",
            "20",
            "--slow-seconds",
            "60",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "planned_calls=5" in result.stdout
    assert "Dry run only" in result.stdout


def test_collect_market_burst_dry_run_supports_multiple_cases(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    for case_id, slug in (("case_a", "market-a"), ("case_b", "market-b"), ("case_c", "market-c")):
        upsert_markets(
            con,
            [
                MarketRecord(
                    market_slug=slug,
                    event_slug=slug,
                    question=slug,
                    category="Politics",
                    tags=["Politics"],
                    end_time="2026-06-01T00:00:00+00:00",
                    resolution_source=None,
                    clob_token_ids=[f"{slug}-yes"],
                    liquidity=50_000,
                    raw={},
                )
            ],
        )
        con.execute(
            """
            insert into event_cases
            (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
            values (?, ?, ?, '2026-05-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00',
                    '[]', 'active', current_timestamp, current_timestamp)
            """,
            [case_id, slug, slug],
        )

    result = CliRunner().invoke(
        app,
        [
            "collect-market-burst",
            "--cases",
            "case_a,case_b,case_c",
            "--fast-seconds",
            "2",
            "--medium-seconds",
            "20",
            "--slow-seconds",
            "60",
            "--dry-run",
        ],
    )

    assert result.exit_code == 0
    assert "cases=3" in result.stdout
    assert "planned_calls=15" in result.stdout


def test_monitor_event_live_dry_run_reports_trigger_candidate(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="trump-china",
                event_slug="trump-china",
                question="Will Trump visit China?",
                category="Politics",
                tags=["Politics"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=50_000,
                raw={},
            )
        ],
    )
    con.execute(
        """
        insert into event_cases
        (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
        values ('trump_china', 'Trump China visit', 'trump-china',
                '2026-05-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00',
                '[]', 'active', current_timestamp, current_timestamp)
        """
    )
    con.execute(
        """
        insert into post_market_semantic_matches
        (case_id, post_id, handle, market_slug, method, similarity, matched_concepts,
         rejected_concepts, decision, created_at)
        values ('trump_china', 'p1', 'Alpha', 'trump-china', 'cloud', 0.91,
                '["peace deal"]', '[]', 'matched', current_timestamp)
        """
    )
    con.execute(
        """
        insert into event_case_posts
        (case_id, post_id, handle, created_at, text, direction, matched_keywords, raw_json_hash, raw_json)
        values ('trump_china', 'p1', 'Alpha', current_timestamp, 'Trump China visit report',
                'bullish', '["visit"]', 'hash-p1', '{}')
        """
    )

    result = CliRunner().invoke(app, ["monitor-event-live", "--cases", "trump_china", "--dry-run"])

    assert result.exit_code == 0
    assert "Dry run trigger candidate" in result.stdout
    assert "confidence=0.91" in result.stdout


def test_monitor_event_live_dry_run_ignores_old_backfill_trigger(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="trump-china",
                event_slug="trump-china",
                question="Will Trump visit China?",
                category="Politics",
                tags=["Politics"],
                end_time="2026-06-01T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=50_000,
                raw={},
            )
        ],
    )
    con.execute(
        """
        insert into event_cases
        (case_id, query, market_slug, start_at, end_at, keywords, status, created_at, updated_at)
        values ('trump_china', 'Trump China visit', 'trump-china',
                '2026-05-01T00:00:00+00:00', '2026-06-01T00:00:00+00:00',
                '[]', 'active', current_timestamp, current_timestamp)
        """
    )
    con.execute(
        """
        insert into post_market_semantic_matches
        (case_id, post_id, handle, market_slug, method, similarity, matched_concepts,
         rejected_concepts, decision, created_at)
        values ('trump_china', 'old-p1', 'Alpha', 'trump-china', 'cloud', 0.99,
                '["peace deal"]', '[]', 'matched', current_timestamp)
        """
    )
    con.execute(
        """
        insert into event_case_posts
        (case_id, post_id, handle, created_at, text, direction, matched_keywords, raw_json_hash, raw_json)
        values ('trump_china', 'old-p1', 'Alpha', current_timestamp - interval '1 day',
                'Old Trump China visit report', 'bullish', '["visit"]', 'hash-old-p1', '{}')
        """
    )

    result = CliRunner().invoke(app, ["monitor-event-live", "--cases", "trump_china", "--dry-run", "--max-trigger-age", "10m"])

    assert result.exit_code == 0
    assert "No new high-confidence burst trigger." in result.stdout
