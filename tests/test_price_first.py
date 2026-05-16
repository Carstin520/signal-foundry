from datetime import datetime, timedelta, timezone

from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.price_first import (
    match_price_events,
    mine_price_events,
    plan_source_backfill,
    run_price_first_backtest,
    write_price_first_report,
)
from quant_sol.signals.storage import (
    connect,
    insert_historical_price_ticks,
    post_price_event_matches_for_case,
    price_events_for_case,
    upsert_event_case_posts,
    upsert_event_cases,
    upsert_markets,
)


def test_mine_price_events_classifies_ramp_jump_and_reversal(tmp_path) -> None:
    con = connect(tmp_path / "price_events.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(now + timedelta(minutes=5), 0.41),
            _tick(now + timedelta(minutes=10), 0.42),
            _tick(now + timedelta(minutes=15), 0.425),
            _tick(now + timedelta(minutes=20), 0.435),
            _tick(now + timedelta(minutes=30), 0.45),
            _tick(now + timedelta(hours=3), 0.50),
            _tick(now + timedelta(hours=3, minutes=1), 0.55),
            _tick(now + timedelta(hours=3, minutes=2), 0.56),
            _tick(now + timedelta(hours=6), 0.50),
            _tick(now + timedelta(hours=6, minutes=5), 0.56),
            _tick(now + timedelta(hours=6, minutes=30), 0.51),
            _tick(now + timedelta(hours=8), 0.50),
            _tick(now + timedelta(hours=8, minutes=30), 0.515),
        ],
    )

    events = mine_price_events(con, "trump_china", windows=["30m"], min_move_pp=3)

    event_types = {event["event_type"] for event in events}
    assert {"ramp", "jump", "reversal"}.issubset(event_types)
    assert all(abs(event["move_size"]) >= 0.03 for event in events)
    assert not any(event["start_at"].startswith((now + timedelta(hours=8)).isoformat()[:16]) for event in events)


def test_mine_price_events_dedupes_overlapping_windows(tmp_path) -> None:
    con = connect(tmp_path / "price_dedupe.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(now + timedelta(minutes=5), 0.42),
            _tick(now + timedelta(minutes=10), 0.44),
            _tick(now + timedelta(minutes=20), 0.46),
            _tick(now + timedelta(minutes=30), 0.47),
        ],
    )

    events = mine_price_events(con, "trump_china", windows=["10m", "30m"], min_move_pp=3)

    assert len(events) == 1
    assert round(events[0]["move_size"], 3) == 0.07


def test_sparse_and_wide_spread_events_are_tagged(tmp_path) -> None:
    con = connect(tmp_path / "price_tags.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    for observed_at, bid, ask, mid in [
        (now, 0.38, 0.44, 0.41),
        (now + timedelta(minutes=30), 0.46, 0.52, 0.49),
    ]:
        con.execute(
            """
            insert into market_ticks
            (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread,
             last_trade_price, liquidity, tick_source, ingested_at, raw_json)
            values (?, 'will-trump-visit-china', 'yes-token', ?, ?, ?, ?, null,
                    null, 'historical', current_timestamp, '{}')
            """,
            [observed_at.isoformat(), bid, ask, mid, ask - bid],
        )

    events = mine_price_events(con, "trump_china", windows=["30m"], min_move_pp=3)

    assert events
    assert "sparse_ticks" in events[0]["risk_tags"]
    assert "wide_spread" in events[0]["risk_tags"]
    assert "missing_liquidity" in events[0]["risk_tags"]


def test_source_backfill_planner_respects_cap_and_marks_expensive(tmp_path) -> None:
    con = connect(tmp_path / "source_plan.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    _seed_three_price_events(con, now)

    def counts(_query: str, _start: str, _end: str) -> dict:
        return {"meta": {"total_tweet_count": 999}}

    plans = plan_source_backfill(con, "trump_china", daily_cap=1, max_count=10, count_provider=counts, write=False)

    assert plans[0]["status"] == "too_expensive"
    assert plans[1]["status"] == "cap_exceeded"
    assert plans[2]["status"] == "cap_exceeded"


def test_match_price_events_labels_before_during_after(tmp_path) -> None:
    con = connect(tmp_path / "price_match.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    _seed_one_price_event(con, now)
    start = price_events_for_case(con, "trump_china")[0]["start_at"]
    start_at = _to_dt(start)
    _seed_posts(con, start_at)

    matches = match_price_events(con, "trump_china", method="keyword", min_confidence=0.70)

    positions = {row["handle"]: row["relative_position"] for row in matches}
    assert positions["EarlyAlpha"] == "before"
    assert positions["DuringAmp"] == "during"
    assert positions["LateCommentator"] == "after"
    assert all(row["lead_seconds"] is not None for row in matches)


def test_price_first_backtest_ranks_early_source_and_late_commentator(tmp_path) -> None:
    con = connect(tmp_path / "price_first.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    _seed_one_price_event(con, now)
    start_at = _to_dt(price_events_for_case(con, "trump_china")[0]["start_at"])
    _seed_posts(con, start_at)
    match_price_events(con, "trump_china", method="keyword", min_confidence=0.70)

    run_id, samples, metrics = run_price_first_backtest(con, "trump_china", horizons=["5m"], execution="top-of-book")
    path = write_price_first_report(con, "trump_china", tmp_path)

    by_handle = {row["handle"]: row for row in metrics}
    assert run_id
    assert samples
    assert by_handle["EarlyAlpha"]["tradable_hit_rate"] == 1
    assert by_handle["EarlyAlpha"]["recommended_status"] == "needs_more_samples"
    assert by_handle["DuringAmp"]["recommended_status"] == "during_move_amplifier"
    assert by_handle["LateCommentator"]["recommended_status"] == "late_commentator"
    assert "minute-level validation only" in path.read_text(encoding="utf-8")


def test_plan_source_backfill_cli_dry_run_does_not_write_plans(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    _seed_one_price_event(con, now)

    result = CliRunner().invoke(app, ["plan-source-backfill", "--case", "trump_china", "--dry-run"])

    assert result.exit_code == 0
    assert "Planned" in result.stdout
    assert con.execute("select count(*) from source_backfill_plans").fetchone()[0] == 0


def _seed_case(con, now: datetime) -> None:
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="will-trump-visit-china",
                event_slug="trump-china",
                question="Will Trump visit China?",
                category="Politics",
                tags=["Trump", "China"],
                end_time=(now + timedelta(days=30)).isoformat(),
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=100_000,
                raw={},
            )
        ],
    )
    upsert_event_cases(
        con,
        [
            {
                "case_id": "trump_china",
                "query": "Trump China visit",
                "market_slug": "will-trump-visit-china",
                "start_at": now.isoformat(),
                "end_at": (now + timedelta(days=2)).isoformat(),
                "keywords": ["trump", "china", "visit", "beijing"],
                "status": "active",
            }
        ],
    )


def _seed_one_price_event(con, now: datetime) -> None:
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(now + timedelta(minutes=5), 0.45),
            _tick(now + timedelta(minutes=10), 0.46),
            _tick(now + timedelta(minutes=20), 0.47),
            _tick(now + timedelta(minutes=30), 0.47),
        ],
    )
    mine_price_events(con, "trump_china", windows=["10m"], min_move_pp=3)


def _seed_three_price_events(con, now: datetime) -> None:
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(now + timedelta(minutes=10), 0.44),
            _tick(now + timedelta(hours=2), 0.45),
            _tick(now + timedelta(hours=2, minutes=10), 0.49),
            _tick(now + timedelta(hours=4), 0.50),
            _tick(now + timedelta(hours=4, minutes=10), 0.54),
        ],
    )
    mine_price_events(con, "trump_china", windows=["10m"], min_move_pp=3)


def _seed_posts(con, start_at: datetime) -> None:
    upsert_event_case_posts(
        con,
        [
            _post("p1", "EarlyAlpha", start_at - timedelta(minutes=5), "Trump may visit Beijing and meet Xi"),
            _post("p2", "DuringAmp", start_at + timedelta(minutes=5), "Trump China visit odds are moving"),
            _post("p3", "LateCommentator", start_at + timedelta(minutes=20), "Trump visit China news was priced"),
        ],
    )


def _post(post_id: str, handle: str, created_at: datetime, text: str) -> dict:
    return {
        "case_id": "trump_china",
        "post_id": post_id,
        "handle": handle,
        "created_at": created_at.isoformat(),
        "text": text,
        "direction": "bullish",
        "matched_keywords": ["trump", "china", "visit"],
        "raw_json": {"id": post_id, "text": text},
    }


def _tick(observed_at: datetime, mid: float) -> dict:
    return {
        "observed_at": observed_at.isoformat(),
        "market_slug": "will-trump-visit-china",
        "token_id": "yes-token",
        "mid": mid,
        "liquidity": 100_000,
        "tick_source": "historical",
        "raw": {},
    }


def _to_dt(value) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
