from pathlib import Path

from quant_sol.signals.clients import is_political_market, market_record_from_gamma
from quant_sol.signals.config import load_fomo_config, load_market_rules, load_social_handles, load_wallet_watchlist, parse_duration
from quant_sol.signals.matching import match_post_to_market


def test_signal_config_loads_watchlists() -> None:
    handles = load_social_handles()
    wallets = load_wallet_watchlist()
    white_house = next(handle for handle in handles if handle.handle == "WhiteHouse")

    assert white_house.category == "confirmation_sources"
    assert white_house.source_score == 0
    assert wallets["GCottrell93"] == "0x94a428cfa4f84b264e01f70d93d02bc96cb36356"
    assert parse_duration("24h") == 86400
    assert load_fomo_config().alert_threshold == 75


def test_market_classifier_and_matcher_detect_geopolitical_market() -> None:
    rules = load_market_rules()
    market = market_record_from_gamma(
        {
            "slug": "us-strikes-iran-by-june-30",
            "question": "Will the US strike Iran by June 30?",
            "tags": [{"label": "Politics"}],
            "clobTokenIds": '["1", "2"]',
        }
    )

    assert is_political_market(market, rules.keywords)

    mention = match_post_to_market(
        "post-1",
        "The White House says there were no US strikes on Iran after ceasefire talks.",
        {
            "market_slug": market.market_slug,
            "event_slug": market.event_slug,
            "question": market.question,
            "category": market.category,
            "tags": market.tags,
        },
        rules,
    )

    assert mention is not None
    assert mention.market_slug == "us-strikes-iran-by-june-30"
    assert "iran" in mention.entities
    assert mention.confidence >= 0.25
