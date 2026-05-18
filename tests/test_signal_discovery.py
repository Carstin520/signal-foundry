from datetime import datetime, timedelta, timezone

from quant_sol.signals.discovery import (
    discover_kalshi_hot_markets,
    kalshi_interest_score,
    kalshi_cross_venue_candidates_for_markets,
    discover_interesting_markets,
    discover_signal_source_candidates,
    market_interest_score,
    market_query_terms,
    platform_watch_candidates_for_markets,
    public_seed_candidates_for_markets,
    rank_discovered_sources,
    write_latest_kalshi_targets,
    write_latest_polymarket_targets,
    write_signal_discovery_report,
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


def test_public_seed_preflight_marks_account_dependency_and_required_checks() -> None:
    market = MarketRecord(
        market_slug="will-elon-musk-post-about-doge-by-june-30",
        event_slug="elon-musk-doge",
        question="Will Elon Musk post about Doge by June 30?",
        category="Crypto",
        tags=["Musk", "Doge"],
        end_time=None,
        resolution_source=None,
        clob_token_ids=["yes-token"],
        liquidity=500_000,
        raw={},
    )

    rows = public_seed_candidates_for_markets(
        [
            {
                "record": market,
                "score": 75,
                "liquidity": 500_000,
                "volume": 2_000_000,
                "deadline_days": 30,
                "query_terms": ["elon", "musk", "doge"],
            }
        ],
        seed_config={
            "seeds": [
                {
                    "platform": "x",
                    "handle": "elonmusk",
                    "role": "account_dependency",
                    "priority": "core",
                    "themes": ["elon", "musk", "doge"],
                    "risk_tags": ["account_dependency"],
                }
            ]
        },
    )

    assert rows[0]["handle"] == "elonmusk"
    assert rows[0]["recommended_status"] == "public_seed_preflight"
    assert rows[0]["edge_classification"] == "narrative_fomo_edge"
    assert rows[0]["data_provenance"]["price_impact"] == "not_evaluated_in_preflight"
    assert rows[0]["tradability"]["status"] == "needs_live_validation"
    assert "account_dependency" in rows[0]["risk_tags"]
    assert "pre/post market ticks" in rows[0]["required_data"]


def test_platform_watch_keeps_reddit_low_confidence_and_discord_manual() -> None:
    market = MarketRecord(
        market_slug="iran-israel-ceasefire-by-june-30",
        event_slug="iran-israel-ceasefire",
        question="Iran Israel ceasefire by June 30?",
        category="Geopolitics",
        tags=["Iran", "Israel"],
        end_time=None,
        resolution_source=None,
        clob_token_ids=["yes-token"],
        liquidity=500_000,
        raw={},
    )

    rows = platform_watch_candidates_for_markets(
        [{"record": market, "score": 80, "query_terms": ["iran", "israel"], "liquidity": 500_000, "deadline_days": 30}],
        seed_config={
            "platform_watch": {
                "reddit": [{"name": "r/geopolitics", "themes": ["iran"], "risk_tags": ["low_confidence_context"]}],
                "discord": [{"name": "Faytuks News", "themes": ["iran"], "risk_tags": ["manual_public_watch"]}],
            }
        },
    )

    by_platform = {row["platform"]: row for row in rows}
    assert by_platform["reddit"]["recommended_status"] == "low_confidence_context"
    assert by_platform["discord"]["recommended_status"] == "manual_public_watch"
    assert by_platform["discord"]["data_provenance"]["platform_context"] == "inferred_authorized_channel_only"
    assert "no_private_scraping" in by_platform["discord"]["risk_tags"]


def test_discovery_dry_run_returns_public_seeds_before_x_or_reddit(monkeypatch, tmp_path) -> None:
    now = datetime(2026, 5, 16, tzinfo=timezone.utc)
    rows = [
        {
            "slug": "will-elon-musk-post-about-doge-by-june-30",
            "question": "Will Elon Musk post about Doge by June 30?",
            "category": "Crypto",
            "endDate": (now + timedelta(days=30)).isoformat(),
            "liquidity": 500_000,
            "volume": 2_000_000,
            "clobTokenIds": ["yes-token"],
            "active": True,
            "closed": False,
        }
    ]

    class FakeGamma:
        def list_markets(self, max_pages=2):
            return rows

    seed_path = tmp_path / "seeds.yaml"
    seed_path.write_text(
        """
seeds:
  - platform: x
    handle: elonmusk
    role: account_dependency
    priority: core
    themes: [elon, musk, doge]
platform_watch:
  reddit:
    - name: r/CryptoCurrency
      themes: [doge, crypto]
  discord:
    - name: Public crypto watch
      themes: [doge]
references:
  - label: example
    url: https://example.com
""",
        encoding="utf-8",
    )
    monkeypatch.setattr("quant_sol.signals.discovery.GammaMarketClient", lambda: FakeGamma())

    result = discover_signal_source_candidates(
        connect(tmp_path / "dry.duckdb"),
        dry_run=True,
        source_seed_path=seed_path,
        include_kalshi=False,
    )

    assert result["x_posts"] == []
    assert result["reddit_posts"] == []
    assert result["planned_x_calls"] == 1
    assert result["public_seed_candidates"][0]["handle"] == "elonmusk"
    assert result["platform_watch"]
    assert result["source_references"][0]["label"] == "example"


def test_kalshi_hot_markets_are_ranked_as_cross_venue_context() -> None:
    class FakeKalshi:
        def list_markets(self, limit=1000, max_pages=2, status="open", mve_filter="exclude"):
            return [
                {
                    "ticker": "KXFED-26JUN-RATE",
                    "event_ticker": "KXFED-26JUN",
                    "title": "Will the Fed cut rates by June?",
                    "volume_24h_fp": "250000",
                    "volume_fp": "1000000",
                    "liquidity_dollars": "50000",
                    "open_interest_fp": "300000",
                    "yes_bid_dollars": "0.4200",
                    "yes_ask_dollars": "0.4500",
                    "close_time": "2026-06-30T00:00:00Z",
                },
                {
                    "ticker": "KXLOW",
                    "title": "Dormant market",
                    "volume_24h_fp": "0",
                    "volume_fp": "0",
                    "open_interest_fp": "0",
                },
            ]

    rows = discover_kalshi_hot_markets(max_markets=5, client=FakeKalshi())

    assert rows[0]["ticker"] == "KXFED-26JUN-RATE"
    assert rows[0]["edge_classification"] == "cross_venue_context"
    assert rows[0]["data_provenance"]["source_activity"] == "observed_kalshi_public_market_api"
    assert "no_order_execution" in rows[0]["risk_tags"]


def test_kalshi_cross_venue_matches_require_rule_and_spread_checks() -> None:
    market = MarketRecord(
        market_slug="fed-cut-rates-by-june",
        event_slug="fed-rates",
        question="Will the Fed cut rates by June?",
        category="Macro",
        tags=["Fed", "rates"],
        end_time=None,
        resolution_source=None,
        clob_token_ids=["yes-token"],
        liquidity=500_000,
        raw={},
    )

    rows = kalshi_cross_venue_candidates_for_markets(
        [{"record": market, "score": 80, "query_terms": ["fed", "rates"], "liquidity": 500_000, "deadline_days": 30}],
        [
            {
                "ticker": "KXFED-26JUN-RATE",
                "title": "Will the Fed cut rates by June?",
                "heat_score": 70,
                "volume_24h": 250000,
                "spread": 0.03,
                "query_terms": ["fed", "rates", "june"],
            }
        ],
    )

    assert rows[0]["recommended_status"] == "cross_venue_context"
    assert rows[0]["tradability"]["status"] == "not_tradable_without_rule_mapping_and_spread_check"
    assert "venue_rule_mismatch" in rows[0]["risk_tags"]


def test_signal_discovery_report_includes_preflight_platforms_and_model_review(tmp_path) -> None:
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
    path = write_signal_discovery_report(
        {
            "markets": [{"record": market, "score": 80, "liquidity": 1_000_000, "volume": 5_000_000, "deadline_days": 30, "query_terms": ["iran"]}],
            "x_posts": [],
            "reddit_posts": [],
            "source_candidates": [],
            "public_seed_candidates": [
                {
                    "platform": "x",
                    "handle": "Faytuks",
                    "market_slug": "us-iran-peace",
                    "discovery_score": 65,
                    "recommended_status": "public_seed_preflight",
                    "edge_classification": "narrative_fomo_edge",
                    "risk_tags": ["social_only_preflight"],
                }
            ],
            "platform_watch": [
                {
                    "platform": "discord",
                    "handle": "Faytuks News",
                    "market_slug": "us-iran-peace",
                    "discovery_score": 30,
                    "recommended_status": "manual_public_watch",
                    "risk_tags": ["no_private_scraping"],
                }
            ],
            "kalshi_hot_markets": [
                {
                    "ticker": "KXIRAN-PEACE",
                    "category": "geopolitics",
                    "heat_score": 55,
                    "volume_24h": 100000,
                    "liquidity": 50000,
                    "spread": 0.02,
                    "query_terms": ["iran", "peace"],
                }
            ],
            "kalshi_cross_venue": [
                {
                    "kalshi_ticker": "KXIRAN-PEACE",
                    "market_slug": "us-iran-peace",
                    "discovery_score": 40,
                    "recommended_status": "cross_venue_context",
                    "risk_tags": ["cross_venue_context", "venue_rule_mismatch"],
                }
            ],
            "source_references": [{"label": "Polymarket API docs", "url": "https://docs.polymarket.com/api-reference/introduction"}],
        },
        tmp_path,
    )

    text = path.read_text(encoding="utf-8")
    assert "## Public Seed Preflight" in text
    assert "## Reddit And Discord Watch" in text
    assert "## Kalshi Hot Pool Context" in text
    assert "## Kalshi Cross-Venue Matches" in text
    assert "## Model Review Discipline" in text
    assert "no_private_scraping" in text
    assert "venue_rule_mismatch" in text
    assert "Polymarket API docs" in text


def test_latest_polymarket_targets_file_is_overwritten(tmp_path) -> None:
    path = tmp_path / "latest.md"
    path.write_text("old content", encoding="utf-8")
    markets = [
        {
            "record": MarketRecord(
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
            ),
            "score": 80,
            "liquidity": 1_000_000,
            "volume": 5_000_000,
            "deadline_days": 30,
            "query_terms": ["iran", "peace", "deal"],
        }
    ]

    write_latest_polymarket_targets(
        {
            "markets": markets,
            "x_posts": [{"post_id": "p1"}],
            "reddit_posts": [],
            "source_candidates": [
                {
                    "handle": "FastReporter",
                    "market_slug": "us-iran-peace",
                    "discovery_score": 70,
                }
            ],
        },
        path,
    )

    text = path.read_text(encoding="utf-8")
    assert "old content" not in text
    assert "`us-iran-peace`" in text
    assert "@FastReporter" in text


def test_latest_polymarket_targets_classifies_warnock_as_us_politics(tmp_path) -> None:
    path = tmp_path / "latest.md"
    write_latest_polymarket_targets(
        {
            "markets": [
                {
                    "record": MarketRecord(
                        market_slug="will-raphael-warnock-win-the-2028-democratic-presidential-nomination",
                        event_slug="democratic-presidential-nomination",
                        question="Will Raphael Warnock win the 2028 Democratic presidential nomination?",
                        category="unknown",
                        tags=[],
                        end_time=None,
                        resolution_source=None,
                        clob_token_ids=["yes-token"],
                        liquidity=1_000_000,
                        raw={},
                    ),
                    "score": 70,
                    "liquidity": 1_000_000,
                    "volume": 2_000_000,
                    "deadline_days": 900,
                    "query_terms": ["democratic", "nomination", "presidential", "raphael", "warnock"],
                }
            ],
            "source_candidates": [],
        },
        path,
    )

    assert "| 1 | us_politics |" in path.read_text(encoding="utf-8")


def test_latest_kalshi_targets_file_is_overwritten(tmp_path) -> None:
    path = tmp_path / "latest-kalshi.md"
    path.write_text("old content", encoding="utf-8")
    row = {
        "ticker": "KXCHINATAIWAN-26",
        "event_ticker": "KXCHINATAIWAN",
        "title": "Will China invade Taiwan in 2026?",
        "yes_bid_dollars": "0.1200",
        "yes_ask_dollars": "0.1500",
        "volume_24h": "250000",
        "open_interest": "500000",
        "close_time": "2026-06-30T00:00:00Z",
    }

    write_latest_kalshi_targets(
        {
            "markets": [
                {
                    "record": row,
                    "score": kalshi_interest_score(row, datetime(2026, 5, 16, tzinfo=timezone.utc)),
                    "volume": 250000,
                    "open_interest": 500000,
                    "spread": 0.03,
                    "deadline_days": 45,
                    "query_terms": ["china", "taiwan", "invade"],
                    "category": "geopolitics",
                }
            ]
        },
        path,
    )

    text = path.read_text(encoding="utf-8")
    assert "old content" not in text
    assert "`KXCHINATAIWAN-26`" in text
    assert "| 1 | geopolitics |" in text


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
