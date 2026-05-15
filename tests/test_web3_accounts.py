from datetime import datetime, timedelta, timezone

from typer.testing import CliRunner

from quant_sol.signals.accounts import match_web3_narratives, rank_accounts
from quant_sol.signals.cli import app
from quant_sol.signals.config import Web3NarrativeKeywords
from quant_sol.signals.storage import connect, upsert_x_accounts, upsert_x_follow_graph, upsert_x_posts


KEYWORDS = Web3NarrativeKeywords(
    groups={
        "airdrop": ("airdrop", "claim", "空投", "积分"),
        "listings": ("listing", "coinbase", "上币"),
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
            _post("p2", "BigInfluencer", now - timedelta(hours=3), "Monad airdrop claim narrative is spreading", 90),
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
    assert by_account["OfficialProject"]["recommended_status"] != "ranked"

    chains = con.execute(
        """
        select downstream_account, upstream_account, evidence_type
        from account_source_chains
        where downstream_account = 'BigInfluencer' and upstream_account = 'UpstreamAlpha'
        """
    ).fetchall()
    assert chains == [("BigInfluencer", "UpstreamAlpha", "following_lead")]


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


def _post(post_id: str, handle: str, created_at: datetime, text: str, reposts: int) -> dict:
    return {
        "post_id": post_id,
        "handle": handle,
        "created_at": created_at.isoformat(),
        "text": text,
        "public_metrics": {"retweet_count": reposts, "quote_count": 0},
        "referenced_tweets": [],
        "lang": "en",
        "raw_json": {"id": post_id, "text": text},
    }
