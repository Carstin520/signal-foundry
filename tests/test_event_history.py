from datetime import datetime, timedelta, timezone

import requests
from typer.testing import CliRunner

from quant_sol.signals.cli import app
from quant_sol.signals.config import SemanticMatchingConfig, Web3AccountConfig, load_semantic_matching_config
from quant_sol.signals.history import (
    DEFAULT_MICRO_HORIZONS,
    case_keywords,
    direction_from_text,
    event_price_windows,
    matched_keywords,
    normalize_price_history,
    run_event_backtest,
    store_event_case_posts,
    write_event_backtest_report,
    x_case_query,
)
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.semantic import HashingSemanticEncoder, match_event_posts_semantically, match_event_posts_with_cloud_model
from quant_sol.signals.storage import (
    connect,
    insert_historical_price_ticks,
    upsert_social_posts,
    upsert_event_cases,
    upsert_live_burst_run,
    upsert_markets,
    upsert_x_posts,
)
from quant_sol.signals.models import SocialPost


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


def test_micro_horizon_defaults_are_short_price_in_windows() -> None:
    assert DEFAULT_MICRO_HORIZONS == ("1s", "10s", "30s", "1m", "5m", "10m", "30m", "2h")


def test_trump_china_direction_keywords_support_english_and_chinese() -> None:
    assert direction_from_text("Trump may visit Beijing for a Xi summit") == "bullish"
    assert direction_from_text("暂无计划，visit is unlikely and may be postponed") == "bearish"
    assert direction_from_text("Trump China discussion is noisy") == "watch_only"


def test_trump_china_indirect_catalyst_matching_requires_anchor_and_impact() -> None:
    keywords = case_keywords("Trump China visit")

    matched = matched_keywords(
        "White House says Iran nuclear situation remains unresolved before Trump Asia scheduling call",
        keywords,
    )

    assert "indirect_catalyst" in matched
    assert direction_from_text("Iran nuclear situation remains unresolved before Trump decision") == "bearish"
    assert not matched_keywords("Iran nuclear talks continue without a scheduling impact", keywords)


def test_trump_china_x_query_includes_indirect_catalysts() -> None:
    query = x_case_query("lrozen", case_keywords("Trump China visit"))

    assert "from:lrozen" in query
    assert "iran" in query
    assert "tariff" in query
    assert '"white house"' in query


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

    impacts, metrics = run_event_backtest(con, "trump_china", ["6h", "24h"], mode="event")

    by_account = {row["account"]: row for row in metrics}
    early_impacts = [row for row in impacts if row["handle"] == "EarlyAlpha" and row["horizon"] == "24h"]
    late_impacts = [row for row in impacts if row["handle"] == "LateFollower" and row["horizon"] == "24h"]

    assert len(early_impacts) == 1
    assert early_impacts[0]["is_positive"]
    assert by_account["EarlyAlpha"]["lead_score"] > by_account["LateFollower"]["lead_score"]
    assert late_impacts[0]["price_move_started_before_post"]
    assert not late_impacts[0]["is_positive"]


def test_ramp_backtest_uses_next_tick_entry_and_max_favorable_move(tmp_path) -> None:
    con = connect(tmp_path / "ramp.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.70),
            _tick(now + timedelta(minutes=35), 0.71),
            _tick(now + timedelta(hours=1), 0.84),
            _tick(now + timedelta(hours=6), 0.78),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "RampAlpha", now + timedelta(minutes=30), "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["6h"], mode="ramp")

    impact = impacts[0]
    account = metrics[0]
    assert impact["entry_mid"] == 0.71
    assert impact["entry_delay_seconds"] == 300
    assert round(impact["max_favorable_delta"], 3) == 0.13
    assert round(impact["close_delta"], 3) == 0.07
    assert impact["tradable_ramp"]
    assert impact["strong_ramp"]
    assert account["recommended_status"] == "ramp_source"


def test_event_window_planner_merges_overlapping_post_windows(tmp_path) -> None:
    con = connect(tmp_path / "windows.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    store_event_case_posts(
        con,
        "trump_china",
        [
            _post("p1", "Alpha", now + timedelta(minutes=30), "Trump may visit Beijing for a Xi summit"),
            _post("p2", "Alpha", now + timedelta(minutes=35), "Trump may visit Beijing soon"),
            _post("p3", "Beta", now + timedelta(hours=6), "Trump China visit odds are noisy"),
        ],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    windows = event_price_windows(con, "trump_china", pre="10m", post="2h")

    assert len(windows) == 2
    assert windows[0][0] == now + timedelta(minutes=20)
    assert windows[0][1] == now + timedelta(hours=2, minutes=35)


def test_micro_backtest_scores_sub_10m_price_in(tmp_path) -> None:
    con = connect(tmp_path / "micro.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(minutes=1)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(post_at + timedelta(seconds=1), 0.405, source="live_burst"),
            _tick(post_at + timedelta(seconds=10), 0.41, source="live_burst"),
            _tick(post_at + timedelta(seconds=30), 0.415, source="live_burst"),
            _tick(post_at + timedelta(minutes=1), 0.42, source="live_burst"),
            _tick(post_at + timedelta(minutes=5), 0.45, source="live_burst"),
            _tick(post_at + timedelta(minutes=10), 0.455, source="live_burst"),
            _tick(post_at + timedelta(minutes=30), 0.44, source="live_burst"),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "MicroAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["1m", "5m", "10m"], mode="micro")

    five_min = [row for row in impacts if row["horizon"] == "5m"][0]
    assert five_min["mode"] == "micro"
    assert five_min["is_positive"]
    assert five_min["tradable_ramp"]
    assert five_min["time_to_3pp_seconds"] == 299
    assert five_min["reward_to_risk"] > 1.5
    assert five_min["risk_adjusted_edge"] > 0
    assert metrics[0]["recommended_status"] == "needs_more_samples"
    assert metrics[0]["sub_10m_hit_rate"] == 1


def test_micro_backtest_marks_cost_erased_move_not_tradable(tmp_path) -> None:
    con = connect(tmp_path / "micro_cost.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(minutes=1)
    rows = [
        (now, 0.39, 0.41, 0.40, "live_burst"),
        (post_at + timedelta(seconds=1), 0.38, 0.42, 0.40, "live_burst"),
        (post_at + timedelta(minutes=5), 0.415, 0.455, 0.435, "live_burst"),
    ]
    for observed_at, bid, ask, mid, source in rows:
        con.execute(
            """
            insert into market_ticks
            (observed_at, market_slug, token_id, best_bid, best_ask, mid, spread,
             last_trade_price, liquidity, tick_source, ingested_at, raw_json)
            values (?, 'will-trump-visit-china', 'yes-token', ?, ?, ?, ?, null,
                    100000, ?, current_timestamp, '{}')
            """,
            [observed_at.isoformat(), bid, ask, mid, ask - bid, source],
        )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "CostAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["5m"], mode="micro")

    impact = impacts[0]
    assert impact["is_positive"]
    assert not impact["paper_trade_positive"]
    assert not impact["tradable_ramp"]
    assert impact["execution_cost"] > 0.03
    assert impact["net_max_favorable_delta"] < 0.03
    assert "cost_erased_move" in impact["risk_tags"]
    assert "high_execution_cost" in impact["risk_tags"]
    assert metrics[0]["recommended_status"] == "cost_erased_watch"


def test_micro_backtest_penalizes_bad_reward_to_risk(tmp_path) -> None:
    con = connect(tmp_path / "micro_rr.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(minutes=1)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40, source="live_burst"),
            _tick(post_at + timedelta(seconds=1), 0.40, source="live_burst"),
            _tick(post_at + timedelta(seconds=10), 0.35, source="live_burst"),
            _tick(post_at + timedelta(minutes=1), 0.43, source="live_burst"),
            _tick(post_at + timedelta(minutes=5), 0.48, source="live_burst"),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "RiskyAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["5m"], mode="micro")

    impact = impacts[0]
    assert impact["paper_trade_positive"]
    assert not impact["tradable_ramp"]
    assert impact["reward_to_risk"] < 1.5
    assert "poor_reward_to_risk" in impact["risk_tags"]
    assert "adverse_excursion" in impact["risk_tags"]
    assert metrics[0]["recommended_status"] == "needs_more_samples"


def test_micro_backtest_marks_minute_floor_for_sub_minute_history(tmp_path) -> None:
    con = connect(tmp_path / "minute_floor.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(minutes=1)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40),
            _tick(post_at, 0.40),
            _tick(post_at + timedelta(minutes=1), 0.44),
            _tick(post_at + timedelta(minutes=5), 0.46),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "MinuteAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["10s", "1m", "5m"], mode="micro")

    ten_sec = [row for row in impacts if row["horizon"] == "10s"][0]
    one_min = [row for row in impacts if row["horizon"] == "1m"][0]
    assert "minute_floor" in ten_sec["risk_tags"]
    assert "insufficient_resolution" in ten_sec["risk_tags"]
    assert not ten_sec["is_positive"]
    assert one_min["is_positive"]
    assert metrics[0]["recommended_status"] in {"micro_source", "watch", "needs_more_samples"}


def test_live_micro_evidence_report_uses_burst_runs(tmp_path) -> None:
    con = connect(tmp_path / "live_report.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(minutes=1)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.40, source="live_burst"),
            _tick(post_at + timedelta(seconds=1), 0.405, source="live_burst"),
            _tick(post_at + timedelta(seconds=10), 0.41, source="live_burst"),
            _tick(post_at + timedelta(seconds=30), 0.415, source="live_burst"),
            _tick(post_at + timedelta(minutes=1), 0.42, source="live_burst"),
            _tick(post_at + timedelta(minutes=5), 0.45, source="live_burst"),
            _tick(post_at + timedelta(minutes=10), 0.455, source="live_burst"),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "MicroAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )
    upsert_live_burst_run(
        con,
        {
            "case_id": "trump_china",
            "post_id": "p1",
            "handle": "MicroAlpha",
            "confidence": 0.91,
            "status": "completed",
            "planned_calls": 7,
            "ticks_written": 7,
        },
    )
    run_event_backtest(con, "trump_china", ["1s", "10s", "30s", "1m", "5m", "10m"], mode="micro")

    path = write_event_backtest_report(con, "trump_china", tmp_path)
    text = path.read_text(encoding="utf-8")

    assert "## Live Micro Evidence" in text
    assert "p1" in text
    assert "MicroAlpha" in text


def test_ramp_hit_survives_close_reversal(tmp_path) -> None:
    con = connect(tmp_path / "ramp_reversal.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.70),
            _tick(now + timedelta(minutes=2), 0.71),
            _tick(now + timedelta(hours=1), 0.84),
            _tick(now + timedelta(hours=6), 0.69),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "RampAlpha", now + timedelta(minutes=1), "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, _ = run_event_backtest(con, "trump_china", ["6h"], mode="ramp")

    assert impacts[0]["is_positive"]
    assert impacts[0]["max_favorable_delta"] > 0.08
    assert impacts[0]["close_delta"] < 0


def test_already_hot_post_can_still_be_tradable_ramp(tmp_path) -> None:
    con = connect(tmp_path / "already_hot.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    post_at = now + timedelta(hours=6)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.50),
            _tick(post_at - timedelta(minutes=5), 0.61),
            _tick(post_at + timedelta(minutes=2), 0.62),
            _tick(post_at + timedelta(hours=1), 0.67),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "HotAlpha", post_at, "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["1h"], mode="ramp")

    assert impacts[0]["already_hot_penalty"]
    assert impacts[0]["tradable_ramp"]
    assert "already_hot_penalty" in impacts[0]["risk_tags"]
    assert metrics[0]["recommended_status"] == "watch"


def test_slow_entry_tick_is_not_tradable_ramp(tmp_path) -> None:
    con = connect(tmp_path / "slow_entry.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.70),
            _tick(now + timedelta(minutes=30), 0.72),
            _tick(now + timedelta(hours=1), 0.78),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "SlowAlpha", now + timedelta(minutes=1), "Trump may visit Beijing for a Xi summit")],
        ["trump", "china", "visit", "beijing", "summit"],
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["1h"], mode="ramp")

    assert impacts[0]["is_positive"]
    assert not impacts[0]["tradable_ramp"]
    assert "slow_entry_tick" in impacts[0]["risk_tags"]
    assert metrics[0]["recommended_status"] == "noise_or_no_ramp"


def test_volatility_backtest_keeps_watch_only_posts_and_scores_abs_moves(tmp_path) -> None:
    con = connect(tmp_path / "volatility.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    insert_historical_price_ticks(
        con,
        [
            _tick(now, 0.55),
            _tick(now + timedelta(minutes=5), 0.56),
            _tick(now + timedelta(hours=1), 0.47),
            _tick(now + timedelta(hours=6), 0.50),
        ],
    )
    store_event_case_posts(
        con,
        "trump_china",
        [_post("p1", "VolSource", now + timedelta(minutes=1), "White House Iran talks create uncertainty for Trump")],
        case_keywords("Trump China visit"),
    )

    impacts, metrics = run_event_backtest(con, "trump_china", ["6h"], mode="volatility")

    assert impacts[0]["mode"] == "volatility"
    assert impacts[0]["is_positive"]
    assert impacts[0]["tradable_ramp"]
    assert round(impacts[0]["max_favorable_delta"], 3) == 0.09
    assert "direction_unknown" in impacts[0]["risk_tags"]
    assert "two_sided_volatility" in impacts[0]["risk_tags"]
    assert metrics[0]["recommended_status"] == "volatility_source"


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


def test_semantic_matcher_adds_multilingual_event_posts(tmp_path, monkeypatch) -> None:
    con = connect(tmp_path / "semantic.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    upsert_x_posts(
        con,
        [
            {
                "post_id": "s1",
                "handle": "FastSource",
                "created_at": (now + timedelta(minutes=5)).isoformat(),
                "text": "和平协议 停火 谈判 正在推动市场情绪",
                "public_metrics": {},
                "referenced_tweets": [],
                "lang": "zh",
                "raw_json": {},
            },
            {
                "post_id": "s2",
                "handle": "Noise",
                "created_at": (now + timedelta(minutes=6)).isoformat(),
                "text": "football celebrity entertainment update",
                "public_metrics": {},
                "referenced_tweets": [],
                "lang": "en",
                "raw_json": {},
            },
        ],
    )
    monkeypatch.setattr("quant_sol.signals.semantic._SentenceTransformerEncoder", lambda _name: HashingSemanticEncoder())
    config = SemanticMatchingConfig(
        model_name="test",
        similarity_threshold=0.2,
        max_posts=100,
        keyword_fallback=True,
        cloud_provider="openai",
        cloud_model="test",
        cloud_max_posts_per_request=20,
        cloud_api_key_env="OPENAI_API_KEY",
        case_seed_concepts={"default": ("和平协议 停火 谈判", "peace deal ceasefire talks")},
        case_exclude_concepts={"default": ("football celebrity entertainment",)},
    )

    result = match_event_posts_semantically(con, "trump_china", config)

    assert result.unavailable_reason is None
    assert result.matches_written == 1
    assert result.posts_added == 1
    stored = con.execute("select handle, matched_keywords from event_case_posts where case_id='trump_china' and post_id='s1'").fetchone()
    assert stored[0] == "FastSource"
    assert "semantic:" in stored[1]


def test_match_event_posts_warns_when_semantic_dependency_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    con = connect(tmp_path / "data" / "quant_sol.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    upsert_event_cases(
        con,
        [
            {
                "case_id": "trump_china",
                "query": "Trump China visit",
                "market_slug": "will-trump-visit-china",
                "start_at": now.isoformat(),
                "end_at": (now + timedelta(days=1)).isoformat(),
                "keywords": ["trump", "china", "visit"],
                "status": "active",
            }
        ],
    )
    monkeypatch.setattr(
        "quant_sol.signals.semantic._SentenceTransformerEncoder",
        lambda _name: (_ for _ in ()).throw(RuntimeError("missing model")),
    )

    result = CliRunner().invoke(app, ["match-event-posts", "--case", "trump_china"])

    assert result.exit_code == 0
    assert "semantic_model_unavailable" in result.stdout


def test_cloud_matcher_requires_api_key(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    con = connect(tmp_path / "cloud_missing.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    config = _semantic_config()

    result = match_event_posts_with_cloud_model(con, "trump_china", config, api_key="")

    assert result.unavailable_reason == "OPENAI_API_KEY is not set"


def test_cloud_matcher_writes_model_matches(tmp_path, monkeypatch) -> None:
    con = connect(tmp_path / "cloud.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    upsert_x_posts(
        con,
        [
            {
                "post_id": "c1",
                "handle": "CloudSource",
                "created_at": (now + timedelta(minutes=5)).isoformat(),
                "text": "Iran ceasefire talks could support a permanent peace agreement",
                "public_metrics": {},
                "referenced_tweets": [],
                "lang": "en",
                "raw_json": {},
            }
        ],
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "output_text": (
                    '{"matches":[{"post_id":"c1","match":true,"confidence":0.91,'
                    '"direction":"bullish","matched_concepts":["peace deal"]}]}'
                )
            }

    class FakeSession:
        def __init__(self):
            self.headers = {}
            self.payload = None

        def post(self, url, json, timeout):
            self.payload = json
            return FakeResponse()

    monkeypatch.setattr("quant_sol.signals.semantic.requests.Session", FakeSession)

    result = match_event_posts_with_cloud_model(con, "trump_china", _semantic_config(), api_key="test-key", base_url="https://example.test")

    assert result.unavailable_reason is None
    assert result.matches_written == 1
    assert result.posts_added == 1
    stored = con.execute("select direction, matched_keywords from event_case_posts where case_id='trump_china' and post_id='c1'").fetchone()
    assert stored[0] == "bullish"
    assert "cloud:peace deal" in stored[1]


def test_cloud_matcher_can_use_social_posts_candidates(tmp_path, monkeypatch) -> None:
    con = connect(tmp_path / "cloud_social.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    upsert_social_posts(
        con,
        [
            SocialPost(
                platform="x",
                handle="SocialSource",
                post_id="sp1",
                created_at=(now + timedelta(minutes=5)).isoformat(),
                text="Iran ceasefire talks could support a permanent peace agreement",
                url="https://x.com/SocialSource/status/sp1",
                raw={},
            )
        ],
    )

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "output_text": (
                    '{"matches":[{"post_id":"sp1","match":true,"confidence":0.9,'
                    '"direction":"bullish","matched_concepts":["peace deal"]}]}'
                )
            }

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, json, timeout):
            return FakeResponse()

    monkeypatch.setattr("quant_sol.signals.semantic.requests.Session", FakeSession)

    result = match_event_posts_with_cloud_model(con, "trump_china", _semantic_config(), api_key="test-key", base_url="https://example.test")

    assert result.matches_written == 1
    assert result.posts_added == 1


def test_cloud_matcher_returns_warning_on_rate_limit(tmp_path, monkeypatch) -> None:
    con = connect(tmp_path / "cloud_429.duckdb")
    now = datetime(2026, 5, 15, tzinfo=timezone.utc)
    _seed_case(con, now)
    upsert_x_posts(
        con,
        [
            {
                "post_id": "c1",
                "handle": "CloudSource",
                "created_at": (now + timedelta(minutes=5)).isoformat(),
                "text": "Iran ceasefire talks could support a permanent peace agreement",
                "public_metrics": {},
                "referenced_tweets": [],
                "lang": "en",
                "raw_json": {},
            }
        ],
    )

    class RateLimitedResponse:
        status_code = 429

        def raise_for_status(self):
            import requests

            raise requests.HTTPError("too many requests", response=self)

    class FakeSession:
        def __init__(self):
            self.headers = {}

        def post(self, url, json, timeout):
            return RateLimitedResponse()

    monkeypatch.setattr("quant_sol.signals.semantic.requests.Session", FakeSession)

    result = match_event_posts_with_cloud_model(con, "trump_china", _semantic_config(), api_key="test-key", base_url="https://example.test")

    assert result.matches_written == 0
    assert "status=429" in result.unavailable_reason


def test_semantic_config_supports_top_level_cloud(tmp_path) -> None:
    path = tmp_path / "semantic.yaml"
    path.write_text(
        """
model:
  name: local-model
  similarity_threshold: 0.61
cloud:
  provider: openai
  model: gpt-test
  max_posts_per_request: 7
  api_key_env: sk-test-inline-key
cases: {}
""",
        encoding="utf-8",
    )

    config = load_semantic_matching_config(path)

    assert config.cloud_provider == "openai"
    assert config.cloud_model == "gpt-test"
    assert config.cloud_max_posts_per_request == 7
    assert config.cloud_api_key_env == "sk-test-inline-key"


def _post(post_id: str, handle: str, created_at: datetime, text: str) -> dict:
    return {
        "post_id": post_id,
        "handle": handle,
        "created_at": created_at.isoformat(),
        "text": text,
        "raw_json": {"id": post_id, "text": text},
    }


def _semantic_config() -> SemanticMatchingConfig:
    return SemanticMatchingConfig(
        model_name="test",
        similarity_threshold=0.2,
        max_posts=100,
        keyword_fallback=True,
        cloud_provider="openai",
        cloud_model="gpt-5-nano",
        cloud_max_posts_per_request=20,
        cloud_api_key_env="OPENAI_API_KEY",
        case_seed_concepts={"default": ("和平协议 停火 谈判", "peace deal ceasefire talks")},
        case_exclude_concepts={"default": ("football celebrity entertainment",)},
    )


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


def _tick(observed_at: datetime, mid: float, source: str = "historical") -> dict:
    return {
        "observed_at": observed_at.isoformat(),
        "market_slug": "will-trump-visit-china",
        "token_id": "yes-token",
        "mid": mid,
        "liquidity": 100_000,
        "tick_source": source,
        "raw": {},
    }
