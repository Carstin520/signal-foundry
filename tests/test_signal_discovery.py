from datetime import datetime, timedelta, timezone

from quant_sol.signals.discovery import (
    discover_interesting_markets,
    market_interest_score,
    market_query_terms,
    rank_discovered_sources,
    x_query_for_market,
)
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.storage import connect, upsert_signal_discovery_sources


def test_market_discovery_scores_high_liquidity_narrative_market() -> None:
    now = datetime(2026, 5, 16, tzinfo=timezone.utc)
    market = MarketRecord(
        market_slug="us-iran-permanent-peace-deal-by-june-30",
        event_slug="us-iran-peace",
        question="US x Iran permanent peace deal by June 30?",
        category="Geopolitics",
        tags=["Iran", "Geopolitics"],
        end_time=(now + timedelta(days=45)).isoformat(),
        resolution_source=None,
        clob_token_ids=["yes-token"],
        liquidity=1_000_000,
        raw={},
    )

    score = market_interest_score(market, {"volume": 5_000_000}, now)

    assert score > 50
    assert {"iran", "peace", "deal"}.issubset(set(market_query_terms(market)))
    assert "-is:retweet" in x_query_for_market(market)


def test_rank_discovered_sources_rewards_repeated_engaged_posts() -> None:
    market = MarketRecord(
        market_slug="us-iran-peace",
        event_slug="us-iran-peace",
        question="US Iran peace deal?",
        category="Geopolitics",
        tags=["Iran"],
        end_time=None,
        resolution_source=None,
        clob_token_ids=["yes-token"],
        liquidity=1_000_000,
        raw={},
    )
    posts = [
        {
            "post_id": "p1",
            "handle": "FastReporter",
            "created_at": "2026-05-16T00:00:00+00:00",
            "matched_market_slug": "us-iran-peace",
            "public_metrics": {"like_count": 50, "retweet_count": 10, "reply_count": 5},
        },
        {
            "post_id": "p2",
            "handle": "FastReporter",
            "created_at": "2026-05-16T00:10:00+00:00",
            "matched_market_slug": "us-iran-peace",
            "public_metrics": {"like_count": 80, "retweet_count": 20, "reply_count": 3},
        },
        {
            "post_id": "p3",
            "handle": "LateNoise",
            "created_at": "2026-05-16T00:20:00+00:00",
            "matched_market_slug": "us-iran-peace",
            "public_metrics": {"like_count": 1, "retweet_count": 0, "reply_count": 0},
        },
    ]

    rows = rank_discovered_sources(posts, [{"matched_market_slug": "us-iran-peace"}], [{"record": market, "score": 80}])

    assert rows[0]["handle"] == "FastReporter"
    assert rows[0]["recommended_status"] == "candidate_signal_source"
    assert rows[0]["post_count"] == 2


def test_signal_discovery_sources_are_stored(tmp_path) -> None:
    con = connect(tmp_path / "discovery.duckdb")
    count = upsert_signal_discovery_sources(
        con,
        [
            {
                "run_id": "r1",
                "platform": "x",
                "handle": "FastReporter",
                "market_slug": "us-iran-peace",
                "first_seen_at": "2026-05-16T00:00:00+00:00",
                "post_count": 2,
                "engagement_score": 100,
                "discovery_score": 42,
                "recommended_status": "candidate_signal_source",
                "evidence": {"post_ids": ["p1", "p2"]},
            }
        ],
    )

    assert count == 1
    assert con.execute("select handle, discovery_score from signal_discovery_sources").fetchone() == ("FastReporter", 42.0)


def test_narrative_focus_filters_sports_markets(monkeypatch) -> None:
    now = datetime(2026, 5, 16, tzinfo=timezone.utc)
    rows = [
        {
            "slug": "will-iran-win-fifa-world-cup",
            "question": "Will Iran win the FIFA World Cup?",
            "category": "Sports",
            "endDate": (now + timedelta(days=60)).isoformat(),
            "liquidity": 5_000_000,
            "volume": 10_000_000,
            "clobTokenIds": ["yes-token"],
            "active": True,
            "closed": False,
        },
        {
            "slug": "us-iran-permanent-peace-deal",
            "question": "US x Iran permanent peace deal by 2026?",
            "category": "Geopolitics",
            "endDate": (now + timedelta(days=60)).isoformat(),
            "liquidity": 500_000,
            "volume": 1_000_000,
            "clobTokenIds": ["yes-token"],
            "active": True,
            "closed": False,
        },
    ]

    class FakeGamma:
        def list_markets(self, max_pages=2):
            return rows

    monkeypatch.setattr("quant_sol.signals.discovery.GammaMarketClient", lambda: FakeGamma())

    markets = discover_interesting_markets(max_markets=5, focus="narrative")

    assert [row["record"].market_slug for row in markets] == ["us-iran-permanent-peace-deal"]
