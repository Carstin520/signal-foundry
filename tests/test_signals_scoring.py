from quant_sol.signals.config import SocialHandle, load_fomo_config, load_market_rules
from quant_sol.signals.models import MarketRecord, SocialPost
from quant_sol.signals.scoring import evaluate_signal_outcomes, score_recent, should_alert
from quant_sol.signals.storage import (
    connect,
    insert_market_tick,
    upsert_markets,
    upsert_signal_events,
    upsert_social_posts,
)


HANDLES = [
    SocialHandle(handle="ReporterA", category="elite_information", source_score=25),
    SocialHandle(handle="PollModel", category="polling_models", source_score=22),
    SocialHandle(handle="MarketGuy", category="market_participants", source_score=18),
    SocialHandle(handle="WhiteHouse", category="confirmation_sources", source_score=0),
]


def test_fomo_divergence_signal_for_long_deadline_nomination_market(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    _seed_market(
        con,
        slug="republican-presidential-nominee-2028",
        question="Will Trump be the Republican presidential nominee in 2028?",
        end_time="2026-07-15T00:00:00+00:00",
        liquidity=100_000,
    )
    _seed_ticks(con, "republican-presidential-nominee-2028", current_mid=0.26, baseline_mid=0.25)
    upsert_social_posts(
        con,
        [
            SocialPost("x", "ReporterA", "p1", "2026-05-15T07:00:00+00:00", "Trump nomination support starts to surge among donors.", "", {}),
            SocialPost("x", "PollModel", "p2", "2026-05-15T11:20:00+00:00", "New poll: Trump leads and is the favorite for the nomination.", "", {}),
            SocialPost("x", "MarketGuy", "p3", "2026-05-15T12:00:00+00:00", "Prediction market accounts are not pricing the Trump nomination surge yet.", "", {}),
        ],
    )

    signals = score_recent(con, "2026-05-14T12:00:00+00:00", HANDLES, load_market_rules(), load_fomo_config())
    upsert_signal_events(con, signals, generated_at="2026-05-15T12:01:00+00:00")

    assert len(signals) == 1
    signal = signals[0]
    assert signal.score >= 75
    assert signal.direction_hint == "yes_up"
    assert signal.price_window["fomo_capacity"] == 20
    assert signal.evidence["edge_classification"] == "narrative_fomo_edge"
    assert signal.evidence["tradability"]["status"] == "tradable_candidate"
    assert signal.evidence["tradability"]["cost_first_failure"] == "none"
    assert signal.evidence["participant_lens"]["retail"] == "candidate"
    assert signal.evidence["participant_lens"]["institution"] == "candidate"
    assert signal.evidence["data_provenance"]["market_price"] == "observed"
    assert signal.evidence["data_provenance"]["narrative_direction"] == "model_derived_keyword_rules"
    assert "already_priced_in" not in signal.risk_tags
    assert should_alert(signal)


def test_near_deadline_fact_market_is_rejected_even_with_strong_narrative(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    _seed_market(
        con,
        slug="us-strikes-iran-by-tomorrow",
        question="Will the US strike Iran by tomorrow?",
        end_time="2026-05-15T23:00:00+00:00",
        liquidity=100_000,
    )
    _seed_ticks(con, "us-strikes-iran-by-tomorrow", current_mid=0.24, baseline_mid=0.23)
    upsert_social_posts(
        con,
        [
            SocialPost("x", "ReporterA", "p1", "2026-05-15T10:00:00+00:00", "US strike risk on Iran is surging.", "", {}),
            SocialPost("x", "PollModel", "p2", "2026-05-15T11:00:00+00:00", "Iran strike risk is the favorite topic in markets.", "", {}),
            SocialPost("x", "MarketGuy", "p3", "2026-05-15T12:00:00+00:00", "Traders are starting to support the US strike Iran thesis.", "", {}),
        ],
    )

    signal = score_recent(con, "2026-05-15T00:00:00+00:00", HANDLES, load_market_rules(), load_fomo_config())[0]

    assert "near_deadline_rejected" in signal.risk_tags
    assert not should_alert(signal)


def test_official_confirmation_and_already_moved_signal_does_not_alert(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    _seed_market(
        con,
        slug="us-sanctions-iran-in-2026",
        question="Will the US impose sanctions on Iran in 2026?",
        end_time="2026-07-15T00:00:00+00:00",
        liquidity=100_000,
    )
    _seed_ticks(con, "us-sanctions-iran-in-2026", current_mid=0.45, baseline_mid=0.25)
    upsert_social_posts(
        con,
        [
            SocialPost("x", "WhiteHouse", "p1", "2026-05-15T12:00:00+00:00", "The White House confirms new Iran sanctions.", "", {}),
        ],
    )

    signal = score_recent(con, "2026-05-15T00:00:00+00:00", HANDLES, load_market_rules(), load_fomo_config())[0]

    assert "confirmed_news" in signal.risk_tags
    assert "already_priced_in" in signal.risk_tags
    assert not should_alert(signal)


def test_low_liquidity_single_handle_pump_does_not_alert(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    _seed_market(
        con,
        slug="small-market-trump-nomination",
        question="Will Trump win a nomination market?",
        end_time="2026-07-15T00:00:00+00:00",
        liquidity=1_000,
    )
    _seed_ticks(con, "small-market-trump-nomination", current_mid=0.22, baseline_mid=0.21, liquidity=1_000)
    upsert_social_posts(
        con,
        [
            SocialPost("x", "ReporterA", "p1", "2026-05-15T10:00:00+00:00", "Trump nomination support surge.", "", {}),
            SocialPost("x", "ReporterA", "p2", "2026-05-15T11:00:00+00:00", "Trump nomination favorite again.", "", {}),
            SocialPost("x", "ReporterA", "p3", "2026-05-15T12:00:00+00:00", "Trump nomination lead keeps growing.", "", {}),
        ],
    )

    signal = score_recent(con, "2026-05-15T00:00:00+00:00", HANDLES, load_market_rules(), load_fomo_config())[0]

    assert "not_executable" in signal.risk_tags
    assert "thin_liquidity" in signal.risk_tags
    assert "low_liquidity_pump" in signal.risk_tags
    assert signal.evidence["edge_classification"] == "liquidity_constrained_narrative"
    assert signal.evidence["tradability"]["status"] == "blocked"
    assert signal.evidence["tradability"]["cost_first_failure"] == "liquidity"
    assert signal.evidence["participant_lens"]["retail"] == "blocked"
    assert not should_alert(signal)


def test_evaluate_writes_positive_signal_outcome(tmp_path) -> None:
    con = connect(tmp_path / "signals.duckdb")
    _seed_market(
        con,
        slug="republican-presidential-nominee-2028",
        question="Will Trump be the Republican presidential nominee in 2028?",
        end_time="2026-07-15T00:00:00+00:00",
        liquidity=100_000,
    )
    _seed_ticks(con, "republican-presidential-nominee-2028", current_mid=0.26, baseline_mid=0.25)
    upsert_social_posts(
        con,
        [
            SocialPost("x", "ReporterA", "p1", "2026-05-15T10:00:00+00:00", "Trump nomination support surge.", "", {}),
            SocialPost("x", "PollModel", "p2", "2026-05-15T11:00:00+00:00", "Trump leads and is the favorite nomination candidate.", "", {}),
            SocialPost("x", "MarketGuy", "p3", "2026-05-15T12:00:00+00:00", "The market has not priced Trump nomination support.", "", {}),
        ],
    )
    signals = score_recent(con, "2026-05-15T00:00:00+00:00", HANDLES, load_market_rules(), load_fomo_config())
    upsert_signal_events(con, signals, generated_at="2026-05-15T12:01:00+00:00")
    insert_market_tick(con, "2026-05-16T12:01:00+00:00", "republican-presidential-nominee-2028", "yes-token", 0.34, 0.36, None, 100_000, {})

    outcomes = evaluate_signal_outcomes(con, "24h", load_fomo_config())

    assert len(outcomes) == 1
    assert round(outcomes[0]["delta"], 3) >= 0.09
    assert outcomes[0]["overshoot"]


def _seed_market(con, slug: str, question: str, end_time: str, liquidity: float) -> None:
    upsert_markets(
        con,
        [
            MarketRecord(
                market_slug=slug,
                event_slug=slug,
                question=question,
                category="Politics",
                tags=["Politics"],
                end_time=end_time,
                resolution_source=None,
                clob_token_ids=["yes-token", "no-token"],
                liquidity=liquidity,
                raw={},
            )
        ],
        updated_at="2026-05-15T00:00:00+00:00",
    )


def _seed_ticks(con, slug: str, current_mid: float, baseline_mid: float, liquidity: float = 100_000) -> None:
    insert_market_tick(con, "2026-05-14T12:00:00+00:00", slug, "yes-token", baseline_mid - 0.01, baseline_mid + 0.01, None, liquidity, {})
    insert_market_tick(con, "2026-05-15T06:00:00+00:00", slug, "yes-token", baseline_mid - 0.01, baseline_mid + 0.01, None, liquidity, {})
    insert_market_tick(con, "2026-05-15T11:59:00+00:00", slug, "yes-token", current_mid - 0.01, current_mid + 0.01, None, liquidity, {})
