from quant_sol.signals.config import SocialHandle, load_market_rules
from quant_sol.signals.models import MarketRecord, SocialPost
from quant_sol.signals.scoring import score_recent, should_alert
from quant_sol.signals.storage import (
    connect,
    insert_market_tick,
    replace_wallet_activity,
    upsert_markets,
    upsert_signal_events,
    upsert_social_posts,
)


def test_score_recent_generates_explainable_signal(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="us-strikes-iran-by-june-30",
                event_slug="us-strikes-iran-by",
                question="Will the US strike Iran by June 30?",
                category="Politics",
                tags=["Politics", "Iran"],
                end_time="2026-06-30T00:00:00+00:00",
                resolution_source=None,
                clob_token_ids=["yes-token", "no-token"],
                liquidity=50_000,
                raw={},
            )
        ],
        updated_at="2026-05-15T00:00:00+00:00",
    )
    upsert_social_posts(
        con,
        [
            SocialPost(
                platform="x",
                handle="WhiteHouse",
                post_id="post-1",
                created_at="2026-05-15T12:00:00+00:00",
                text="Statement on Iran: no US strike after ceasefire talks.",
                url="https://x.com/WhiteHouse/status/post-1",
                raw={"id": "post-1"},
            )
        ],
    )
    insert_market_tick(con, "2026-05-15T11:59:00+00:00", "us-strikes-iran-by-june-30", "yes-token", 0.40, 0.42, None, 50_000, {})
    insert_market_tick(con, "2026-05-15T12:04:00+00:00", "us-strikes-iran-by-june-30", "yes-token", 0.28, 0.30, None, 50_000, {})
    replace_wallet_activity(
        con,
        "GCottrell93",
        [
                {
                    "eventSlug": "us-strikes-iran-by",
                    "side": "SELL",
                    "price": 0.35,
                    "size": 500_000,
                    "timestamp": "2026-05-15T12:03:00+00:00",
                }
        ],
        fetched_at="2026-05-15T12:05:00+00:00",
    )

    signals = score_recent(
        con,
        "2026-05-15T11:00:00+00:00",
        [SocialHandle(handle="WhiteHouse", category="us_government", source_score=25)],
        load_market_rules(),
    )
    upsert_signal_events(con, signals, generated_at="2026-05-15T12:06:00+00:00")

    assert len(signals) == 1
    signal = signals[0]
    assert signal.score >= 70
    assert signal.direction_hint == "yes_down"
    assert signal.wallet_flows
    assert should_alert(signal)


def test_realized_only_guard_risk_tag_from_signal_penalty(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="weak-market",
                event_slug=None,
                question="Will Iran sanctions happen?",
                category="Politics",
                tags=[],
                end_time=None,
                resolution_source=None,
                clob_token_ids=[],
                liquidity=1_000,
                raw={},
            )
        ],
    )
    upsert_social_posts(
        con,
        [
            SocialPost("x", "Unknown", "post-2", "2026-05-15T12:00:00+00:00", "Iran sanctions update", "", {})
        ],
    )

    signals = score_recent(con, "2026-05-15T11:00:00+00:00", [], load_market_rules())

    assert signals
    assert "social_only_no_price_move" in signals[0].risk_tags
