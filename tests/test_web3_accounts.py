from datetime import datetime, timedelta, timezone
from typing import List, Optional

from typer.testing import CliRunner

from quant_sol.signals.accounts import match_web3_narratives, rank_accounts
from quant_sol.signals.cli import app
from quant_sol.signals.config import Web3NarrativeKeywords
from quant_sol.signals.models import MarketRecord
from quant_sol.signals.storage import connect, insert_market_tick, upsert_markets, upsert_x_accounts, upsert_x_follow_graph, upsert_x_posts


KEYWORDS = Web3NarrativeKeywords(
    groups={
        "airdrop": ("airdrop", "claim", "空投", "积分"),
        "listings": ("listing", "coinbase", "上币"),
        "ecosystems": ("HYPE", "Hyperliquid"),
    },
    role_weights={
        "originators": 25,
        "fast_curators": 22,
        "amplifiers": 18,
        "market_translators": 20,
        "upstream_sources": 24,
        "confirmation_sources": 0,
        "noise_or_late": 2,
    },
)


def test_web3_keyword_matcher_supports_english_and_chinese() -> None:
    assert "airdrop" in match_web3_narratives("Monad airdrop claim window is live", KEYWORDS)
    assert "airdrop" in match_web3_narratives("这个项目的空投积分叙事开始升温", KEYWORDS)
    assert "listings" in match_web3_narratives("Coinbase listing rumor and 上币预期", KEYWORDS)


def test_rank_accounts_rewards_earliest_source_and_builds_source_chain(tmp_path) -> None:
    con = connect(tmp_path / "accounts.duckdb")
    _seed_accounts(con)
    now = datetime.now(timezone.utc).replace(microsecond=0)
    upsert_x_posts(
        con,
        [
            _post("p1", "UpstreamAlpha", now - timedelta(hours=5), "Monad 空投 eligibility looks active", 8),
            _post(
                "p2",
                "BigInfluencer",
                now - timedelta(hours=3),
                "Monad airdrop claim narrative is spreading",
                90,
                referenced_tweets=[{"type": "quoted", "id": "p1"}],
            ),
            _post("p3", "SpamPump", now - timedelta(minutes=58), "Monad airdrop airdrop airdrop", 1),
            _post("p4", "SpamPump", now - timedelta(minutes=48), "Monad airdrop pump again", 1),
            _post("p5", "SpamPump", now - timedelta(minutes=38), "Monad airdrop final call", 1),
            _post("p6", "OfficialProject", now - timedelta(minutes=20), "Official Coinbase listing announcement", 25),
        ],
    )
    upsert_x_follow_graph(
        con,
        [{"source_handle": "BigInfluencer", "target_handle": "UpstreamAlpha", "relationship": "following"}],
    )

    rows = rank_accounts(con, "30d", KEYWORDS)
    by_account = {row["account"]: row for row in rows}

    assert by_account["UpstreamAlpha"]["speed_score"] > by_account["BigInfluencer"]["speed_score"]
    assert by_account["UpstreamAlpha"]["final_score"] > by_account["BigInfluencer"]["final_score"]
    assert by_account["UpstreamAlpha"]["source_chain_score"] > 0
    assert by_account["SpamPump"]["cascade_score"] <= 0.5
    assert by_account["SpamPump"]["frequency_score"] <= 8
    assert by_account["OfficialProject"]["recommended_status"] == "confirmation_source"
    assert by_account["OfficialProject"]["final_score"] == 0
    assert by_account["UpstreamAlpha"]["recommended_status"] == "insufficient_market_data"

    chains = con.execute(
        """
        select downstream_account, upstream_account, evidence_type
        from account_source_chains
        where downstream_account = 'BigInfluencer' and upstream_account = 'UpstreamAlpha'
        order by evidence_type
        """
    ).fetchall()
    assert ("BigInfluencer", "UpstreamAlpha", "following_lead") in chains
    assert ("BigInfluencer", "UpstreamAlpha", "post_reference_lead") in chains


def test_rank_accounts_uses_market_ticks_for_impact_score(tmp_path) -> None:
    con = connect(tmp_path / "accounts.duckdb")
    upsert_x_accounts(
        con,
        [
            {"handle": "Lookonchain", "language": "en", "region": "global", "role": "originators", "priority": "seed", "status": "active"},
        ],
    )
    now = datetime.now(timezone.utc).replace(microsecond=0)
    upsert_x_posts(
        con,
        [
            _post("hype-1", "Lookonchain", now - timedelta(hours=24), "Whales are buying HYPE longs on Hyperliquid", 30),
        ],
    )
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug="will-hype-hit-50-this-month",
                event_slug="hype-price",
                question="Will HYPE hit $50 this month?",
                category="Crypto",
                tags=["Crypto", "Hyperliquid"],
                end_time=(now + timedelta(days=20)).isoformat(),
                resolution_source=None,
                clob_token_ids=["yes-token"],
                liquidity=100_000,
                raw={},
            )
        ],
    )
    insert_market_tick(con, (now - timedelta(hours=25)).isoformat(), "will-hype-hit-50-this-month", "yes-token", 0.19, 0.21, None, 100_000, {})
    insert_market_tick(con, (now - timedelta(hours=1)).isoformat(), "will-hype-hit-50-this-month", "yes-token", 0.27, 0.29, None, 100_000, {})

    rows = rank_accounts(con, "30d", KEYWORDS)
    account = {row["account"]: row for row in rows}["Lookonchain"]

    assert account["sample_size"] == 1
    assert account["market_impact_score"] > 0
    assert account["hit_rate_24h"] == 1
    assert account["recommended_status"] != "insufficient_market_data"


def test_fast_web3_accounts_can_match_non_web3_markets(tmp_path) -> None:
    con = connect(tmp_path / "accounts.duckdb")
    upsert_x_accounts(
        con,
        [
            {"handle": "FastCryptoCurator", "language": "en", "region": "global", "role": "fast_curators", "priority": "seed", "status": "active"},
        ],
    )
    now = datetime.now(timezone.utc).replace(microsecond=0)
    upsert_x_posts(
        con,
        [
            _post("politics-1", "FastCryptoCurator", now - timedelta(hours=24), "Trump nominee odds are starting to rally after donor chatter", 12),
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
    insert_market_tick(con, (now - timedelta(hours=25)).isoformat(), "will-trump-be-republican-nominee", "yes-token", 0.29, 0.31, None, 100_000, {})
    insert_market_tick(con, (now - timedelta(hours=1)).isoformat(), "will-trump-be-republican-nominee", "yes-token", 0.36, 0.38, None, 100_000, {})

    rows = rank_accounts(con, "30d", KEYWORDS)
    account = {row["account"]: row for row in rows}["FastCryptoCurator"]
    matched = con.execute("select market_slug, narrative_key, entity, confidence from account_market_mentions").fetchall()

    assert len(matched) == 1
    assert matched[0][:3] == ("will-trump-be-republican-nominee", "market:will-trump-be-republican-nominee", "nominee")
    assert round(matched[0][3], 2) == 0.83
    assert account["sample_size"] == 1
    assert account["market_impact_score"] > 0


def test_sync_accounts_without_x_token_uses_fallback_warning(tmp_path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("X_BEARER_TOKEN", raising=False)

    result = CliRunner().invoke(app, ["sync-accounts", "--watchlist", "web3", "--backfill", "7d"])

    assert result.exit_code == 0
    assert "X_BEARER_TOKEN is not set" in result.stdout


def _seed_accounts(con) -> None:
    upsert_x_accounts(
        con,
        [
            {"handle": "UpstreamAlpha", "language": "en", "region": "global", "role": "originators", "priority": "seed", "status": "active"},
            {"handle": "BigInfluencer", "language": "en", "region": "global", "role": "amplifiers", "priority": "seed", "status": "active"},
            {"handle": "SpamPump", "language": "mixed", "region": "global", "role": "fast_curators", "priority": "watch", "status": "active"},
            {"handle": "OfficialProject", "language": "en", "region": "global", "role": "confirmation_sources", "priority": "seed", "status": "active"},
        ],
    )


def _post(
    post_id: str,
    handle: str,
    created_at: datetime,
    text: str,
    reposts: int,
    referenced_tweets: Optional[List[dict]] = None,
) -> dict:
    return {
        "post_id": post_id,
        "handle": handle,
        "created_at": created_at.isoformat(),
        "text": text,
        "public_metrics": {"retweet_count": reposts, "quote_count": 0},
        "referenced_tweets": referenced_tweets or [],
        "lang": "en",
        "raw_json": {"id": post_id, "text": text},
    }
