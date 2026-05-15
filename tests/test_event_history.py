from datetime import datetime, timedelta, timezone

import requests
from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.config import Web3AccountConfig
from quant_sol.signals.history import (
    direction_from_text,
    normalize_price_history,
    run_event_backtest,
    store_event_case_posts,
)
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.storage import (
    connect,
    insert_historical_price_ticks,
    upsert_event_cases,
    upsert_markets,
)


def test_batch_prices_history_response_writes_historical_ticks(tmp_path) -> None:
    con = connect(tmp_path / "history.duckdb")
    payload = {"history": {"yes-token": [{"t": 1_776_000_000, "p": 0.42}]}}
    rows = normalize_price_history(
        payload,
        {"yes-token": {"market_slug": "trump-china-visit", "liquidity": 25_000}},
    )

    count = insert_historical_price_ticks(con, rows)
    stored = con.execute("select market_slug, token_id, mid, tick_source from market_ticks").fetchall()

    assert count == 1
    assert stored == [("trump-china-visit", "yes-token", 0.42, "historical")]


def test_trump_china_direction_keywords_support_english_and_chinese() -> None:
    assert direction_from_text("Trump may visit Beijing for a Xi summit") == "bullish"
    assert direction_from_text("暂无计划，visit is unlikely and may be postponed") == "bearish"
    assert direction_from_text("Trump China discussion is noisy") == "watch_only"


def test_event_backtest_marks_early_source_and_late_price_follower(tmp_path) -> None:
    con = connect(tmp_path / "history.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
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
    ticks = [
        (now, 0.20),
        (now + timedelta(hours=1), 0.20),
        (now + timedelta(hours=3), 0.25),
        (now + timedelta(hours=6), 0.30),
        (now + timedelta(hours=24), 0.34),
    ]
    insert_historical_price_ticks(
        con,
        [
            {
                "observed_at": ts.isoformat(),
                "market_slug": "will-trump-visit-china",
                "token_id": "yes-token",
                "mid": mid,
                "liquidity": 100_000,
                "raw": {},
            }
            for ts, mid in ticks
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [
            _post("p1", "EarlyAlpha", now + timedelta(minutes=30), "Trump may visit Beijing for a Xi summit"),
            _post("p2", "EarlyAlpha", now + timedelta(minutes=35), "Trump visit China meeting still possible"),
            _post("p3", "LateFollower", now + timedelta(hours=6), "Trump China visit odds are now surging"),
        ],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["6h", "24h"])

    by_account = {row["account"]: row for row in metrics}
    early_impacts = [row for row in impacts if row["handle"] == "EarlyAlpha" and row["horizon"] == "24h"]
    late_impacts = [row for row in impacts if row["handle"] == "LateFollower" and row["horizon"] == "24h"]

    assert len(early_impacts) == 1
    assert early_impacts[0]["is_positive"]
    assert by_account["EarlyAlpha"]["lead_score"] > by_account["LateFollower"]["lead_score"]
    assert late_impacts[0]["price_move_started_before_post"]
    assert not late_impacts[0]["is_positive"]


def test_backfill_x_history_stops_when_full_archive_unavailable(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("X_BEARER_TOKEN", "test-token")
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    upsert_event_cases(
        con,
        [
            {
                "case_id": "trump_china",
                "query": "Trump China visit",
                "market_slug": "will-trump-visit-china",
                "start_at": "2025-01-01T00:00:00+00:00",
                "end_at": "2025-01-02T00:00:00+00:00",
                "keywords": ["trump", "china", "visit"],
                "status": "active",
            }
        ],
    )

    class ForbiddenXClient:
        def __init__(self, token):
            self.token = token

        def full_archive_counts(self, query, start_time, end_time):
            response = requests.Response()
            response.status_code = 403
            raise requests.HTTPError("forbidden", response=response)

    monkeypatch.setattr("quant_sol.signals.cli.XApiClient", ForbiddenXClient)
    monkeypatch.setattr(
        "quant_sol.signals.cli.load_web3_accounts",
        lambda: [Web3AccountConfig("EarlyAlpha", "en", "global", "originators", "seed")],
    )

    result = CliRunner().invoke(app, ["backfill-x-history", "--case", "trump_china", "--daily-cap", "10"])

    assert result.exit_code == 0
    assert "full-archive search is unavailable" in result.stdout


def _post(post_id: str, handle: str, created_at: datetime, text: str) -> dict:
    return {
        "post_id": post_id,
        "handle": handle,
        "created_at": created_at.isoformat(),
        "text": text,
        "raw_json": {"id": post_id, "text": text},
    }
