from __future__ import annotations

import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence, Tuple

import duckdb

from .config import Web3AccountConfig, Web3NarrativeKeywords, load_web3_accounts, load_web3_keywords, parse_duration
from .storage import (
    upsert_account_impact_metrics,
    upsert_account_market_mentions,
    upsert_account_market_outcomes,
    upsert_account_narrative_mentions,
    upsert_account_source_chains,
    upsert_x_accounts,
    upsert_x_follow_graph,
    upsert_x_posts,
)
from .utils import to_datetime, utc_now_iso, words


def account_rows_from_config(accounts: Sequence[Web3AccountConfig]) -> List[dict]:
    return [
        {
            "handle": account.handle,
            "language": account.language,
            "region": account.region,
            "role": account.role,
            "priority": account.priority,
            "status": "active",
            "notes": account.notes,
        }
        for account in accounts
    ]


def import_accounts_csv(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as handle:
        return upsert_x_accounts(con, [dict(row) for row in csv.DictReader(handle)])


def import_posts_csv(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    rows = []
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            rows.append(
                {
                    "post_id": row.get("post_id") or str(abs(hash((row.get("handle"), row.get("created_at"), row.get("text"))))),
                    "handle": (row.get("handle") or "").lstrip("@"),
                    "created_at": row.get("created_at") or utc_now_iso(),
                    "text": row.get("text") or "",
                    "public_metrics": _json_or_empty(row.get("public_metrics")),
                    "referenced_tweets": _json_or_empty(row.get("referenced_tweets"), default=[]),
                    "lang": row.get("lang"),
                    "raw_json": dict(row),
                }
            )
    return upsert_x_posts(con, rows)


def import_follow_graph_csv(con: duckdb.DuckDBPyConnection, path: Path) -> int:
    with path.open(newline="", encoding="utf-8") as handle:
        return upsert_x_follow_graph(con, [dict(row) for row in csv.DictReader(handle)])


def rank_accounts(
    con: duckdb.DuckDBPyConnection,
    lookback: str,
    keywords: Optional[Web3NarrativeKeywords] = None,
) -> List[dict]:
    keyword_config = keywords or load_web3_keywords()
    since = (datetime.now(timezone.utc) - timedelta(seconds=parse_duration(lookback))).replace(microsecond=0).isoformat()
    accounts = _accounts(con)
    posts = _posts(con, since)
    ticks = _market_ticks(con, since)
    markets = _markets(con)
    keyword_mentions = _build_mentions(posts, keyword_config)
    direct_market_mentions = _direct_market_mentions(posts, markets)
    mentions = _dedupe_mentions(keyword_mentions + _mentions_from_market_mentions(direct_market_mentions))
    mention_rows = _mention_rows(mentions)
    chains = _source_chains(mentions, con)
    market_mentions = _dedupe_market_mentions(_market_mentions(keyword_mentions, markets, keyword_config) + direct_market_mentions)
    outcomes = _market_outcomes(market_mentions, ticks)
    metrics = _impact_metrics(accounts, mentions, chains, market_mentions, outcomes, lookback, keyword_config)
    _clear_account_analysis_outputs(con)
    upsert_account_narrative_mentions(con, mention_rows)
    upsert_account_market_mentions(con, market_mentions)
    upsert_account_market_outcomes(con, outcomes)
    upsert_account_source_chains(con, chains)
    upsert_account_impact_metrics(con, metrics)
    return sorted(metrics, key=lambda item: item["final_score"], reverse=True)


def write_account_report(con: duckdb.DuckDBPyConnection, lookback: str, report_root: Path) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    rows = _latest_account_metrics(con, lookback)
    chains = _latest_source_chains(con)
    path = report_root / f"account_rankings_{lookback}.md"
    lines = [
        f"# Signal Account Ranking ({lookback})",
        "",
        "## Alpha Candidates",
        "",
        "| Rank | Account | Final | Speed | Freq | Cascade | Market | Chain | False FOMO | Status |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    alpha_rows = [
        row for row in rows
        if row["recommended_status"] not in {"confirmation_source", "insufficient_market_data", "insufficient_x_data"}
    ]
    if not alpha_rows:
        lines.append("| n/a | No account metrics | 0 | 0 | 0 | 0 | 0 | 0 | 0 | n/a |")
    for idx, row in enumerate(alpha_rows, start=1):
        lines.append(
            f"| {idx} | @{row['account']} | {_fmt(row['final_score'])} | {_fmt(row['speed_score'])} | "
            f"{_fmt(row['frequency_score'])} | {_fmt(row['cascade_score'])} | {_fmt(row['market_impact_score'])} | "
            f"{_fmt(row['source_chain_score'])} | {_fmt(row['false_fomo_rate'])} | {row['recommended_status']} |"
        )
    lines.extend(["", "## Confirmation / Discovery Sources", ""])
    for row in [item for item in rows if item["recommended_status"] == "confirmation_source"]:
        lines.append(f"- @{row['account']}: confirmation/discovery source, excluded from alpha score.")
    lines.extend(["", "## Insufficient Market Data", ""])
    for row in [item for item in rows if item["recommended_status"] == "insufficient_market_data"]:
        lines.append(f"- @{row['account']}: needs matched Polymarket ticks before alpha ranking.")
    lines.extend(["", "## Insufficient X Data", ""])
    for row in [item for item in rows if item["recommended_status"] == "insufficient_x_data"]:
        lines.append(f"- @{row['account']}: no matched market/narrative posts in the lookback window.")
    lines.extend(["", "## Source Chain Candidates", ""])
    if not chains:
        lines.append("- None.")
    for chain in chains[:20]:
        lines.append(
            f"- @{chain['upstream_account']} -> @{chain['downstream_account']}: "
            f"{_fmt(chain['lead_time_minutes'])} min lead, evidence {chain['evidence_type']}, "
            f"confidence {_fmt(chain['confidence'])}, "
            f"narratives {chain['shared_narratives']}"
        )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def evaluate_account_source(
    con: duckdb.DuckDBPyConnection,
    handle: str,
    lookback: str,
    keywords: Optional[Web3NarrativeKeywords] = None,
) -> dict:
    normalized = handle.lstrip("@")
    keyword_config = keywords or load_web3_keywords()
    since = (datetime.now(timezone.utc) - timedelta(seconds=parse_duration(lookback))).replace(microsecond=0).isoformat()
    accounts = _accounts(con)
    profile = accounts.get(normalized) or _default_profile(normalized)
    posts = [post for post in _posts(con, since) if str(post.get("handle") or "").lower() == normalized.lower()]
    markets = _markets(con)
    ticks = _market_ticks(con, since)
    keyword_mentions = _build_mentions(posts, keyword_config)
    direct_market_mentions = _direct_market_mentions(posts, markets)
    mentions = _dedupe_mentions(keyword_mentions + _mentions_from_market_mentions(direct_market_mentions))
    market_mentions = _dedupe_market_mentions(_market_mentions(keyword_mentions, markets, keyword_config) + direct_market_mentions)
    outcomes = _market_outcomes(market_mentions, ticks)
    metrics = _impact_metrics({normalized: profile}, mentions, [], market_mentions, outcomes, lookback, keyword_config)
    metric = metrics[0] if metrics else _empty_metric(normalized, lookback)
    narrative_counts = Counter(mention["narrative_key"] for mention in mentions)
    market_counts = Counter(mention["market_slug"] for mention in market_mentions)
    classified_posts = _classified_posts(posts, mentions, market_mentions, outcomes)
    return {
        "handle": normalized,
        "lookback": lookback,
        "generated_at": utc_now_iso(),
        "profile": profile,
        "post_count": len(posts),
        "mention_count": len(mentions),
        "market_link_count": len(market_mentions),
        "outcome_count": len(outcomes),
        "metric": metric,
        "narrative_counts": dict(narrative_counts.most_common()),
        "market_counts": dict(market_counts.most_common()),
        "market_mentions": sorted(market_mentions, key=lambda row: float(row.get("confidence") or 0), reverse=True),
        "outcomes": outcomes,
        "classified_posts": classified_posts,
        "data_provenance": {
            "profile": "observed_x_api_or_csv" if normalized in accounts else "inferred_default_profile",
            "posts": "observed_x_posts",
            "market_links": "model_derived_keyword_rules",
            "price_impact": "observed_market_ticks" if outcomes else "insufficient_tick_data",
            "source_quality": "model_derived_account_ranking",
        },
        "tradability": _account_tradability(metric, outcomes),
        "participant_lens": _account_participant_lens(metric, outcomes),
        "required_data": [
            "fresh X profile and timeline",
            "matched Polymarket markets",
            "pre/post market ticks",
            "bid/ask spread and liquidity snapshots",
            "enough repeated samples for account-level confidence",
        ],
        "failure_mode": _account_failure_mode(metric, len(posts), len(market_mentions), len(outcomes)),
    }


def write_account_source_evaluation_report(result: Mapping[str, object], report_root: Path) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    handle = str(result.get("handle") or "account").lstrip("@")
    path = report_root / f"account_source_eval_{handle}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.md"
    metric = result.get("metric") if isinstance(result.get("metric"), Mapping) else {}
    profile = result.get("profile") if isinstance(result.get("profile"), Mapping) else {}
    tradability = result.get("tradability") if isinstance(result.get("tradability"), Mapping) else {}
    lens = result.get("participant_lens") if isinstance(result.get("participant_lens"), Mapping) else {}
    provenance = result.get("data_provenance") if isinstance(result.get("data_provenance"), Mapping) else {}
    lines = [
        f"# Account Source Evaluation: @{handle}",
        "",
        f"- Generated at: {result.get('generated_at')}",
        f"- Lookback: {result.get('lookback')}",
        f"- Posts evaluated: {result.get('post_count')}",
        f"- Narrative/market mentions: {result.get('mention_count')}",
        f"- Market links: {result.get('market_link_count')}",
        f"- Price outcome rows: {result.get('outcome_count')}",
        "",
        "## Profile",
        "",
        f"- Role: {profile.get('role') or 'ad_hoc_candidate'}",
        f"- Priority: {profile.get('priority') or 'ad_hoc'}",
        f"- Followers: {_fmt(profile.get('followers'))}",
        f"- Following: {_fmt(profile.get('following'))}",
        f"- Verified: {profile.get('verified')}",
        f"- Status: {profile.get('status') or 'active'}",
        "",
        "## Scorecard",
        "",
        "| Final | Speed | Frequency | Cascade | Market Impact | Chain | False FOMO | Sample Size | Status |",
        "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
        (
            f"| {_fmt(metric.get('final_score'))} | {_fmt(metric.get('speed_score'))} | "
            f"{_fmt(metric.get('frequency_score'))} | {_fmt(metric.get('cascade_score'))} | "
            f"{_fmt(metric.get('market_impact_score'))} | {_fmt(metric.get('source_chain_score'))} | "
            f"{_fmt(metric.get('false_fomo_rate'))} | {metric.get('sample_size') or 0} | "
            f"{metric.get('recommended_status') or 'insufficient_x_data'} |"
        ),
        "",
        "## Narrative Coverage",
        "",
        "| Narrative | Count |",
        "| --- | ---: |",
    ]
    narrative_counts = result.get("narrative_counts") if isinstance(result.get("narrative_counts"), Mapping) else {}
    if not narrative_counts:
        lines.append("| n/a | 0 |")
    for key, count in narrative_counts.items():
        lines.append(f"| {key} | {count} |")
    lines.extend(["", "## Matched Markets", "", "| Market | Count |", "| --- | ---: |"])
    market_counts = result.get("market_counts") if isinstance(result.get("market_counts"), Mapping) else {}
    if not market_counts:
        lines.append("| n/a | 0 |")
    for key, count in market_counts.items():
        lines.append(f"| `{key}` | {count} |")
    lines.extend(
        [
            "",
            "## Recent Classified Posts",
            "",
            "| Created At | Post | Classification | Markets |",
            "| --- | --- | --- | --- |",
        ]
    )
    for row in list(result.get("classified_posts") or [])[:20]:
        lines.append(
            f"| {row.get('created_at')} | `{row.get('post_id')}` | {row.get('classification')} | "
            f"{', '.join(f'`{market}`' for market in row.get('markets') or []) or 'n/a'} |"
        )
    if not result.get("classified_posts"):
        lines.append("| n/a | n/a | no_posts | n/a |")
    lines.extend(
        [
            "",
            "## Price Impact Samples",
            "",
            "| Post | Market | Horizon | Entry | Future | Delta | Max Fav | Max Adv | Positive |",
            "| --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |",
        ]
    )
    outcomes = list(result.get("outcomes") or [])
    if not outcomes:
        lines.append("| n/a | n/a | n/a | 0 | 0 | 0 | 0 | 0 | false |")
    for row in outcomes[:30]:
        lines.append(
            f"| `{row.get('post_id')}` | `{row.get('market_slug')}` | {row.get('horizon')} | "
            f"{_fmt(row.get('entry_mid'))} | {_fmt(row.get('future_mid'))} | {_fmt(row.get('delta'))} | "
            f"{_fmt(row.get('max_favorable_delta'))} | {_fmt(row.get('max_adverse_delta'))} | {row.get('is_positive')} |"
        )
    lines.extend(
        [
            "",
            "## Model Review",
            "",
            f"- Edge classification: {metric.get('recommended_status') or 'insufficient_x_data'}",
            f"- Tradability: status={tradability.get('status')}, first_failure={tradability.get('cost_first_failure')}",
            f"- Participant lens: retail={lens.get('retail')}, institution={lens.get('institution')}, market_maker={lens.get('market_maker')}",
            f"- Provenance: profile={provenance.get('profile')}, posts={provenance.get('posts')}, market_links={provenance.get('market_links')}, price_impact={provenance.get('price_impact')}",
            f"- Failure mode: {result.get('failure_mode')}",
            "- This is a read-only source evaluation. It does not add the account to a live watchlist or create any execution path.",
        ]
    )
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def export_account_seed_csv(accounts: Sequence[Web3AccountConfig], path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=["handle", "language", "region", "role", "priority", "notes"])
        writer.writeheader()
        for account in accounts:
            writer.writerow(
                {
                    "handle": account.handle,
                    "language": account.language,
                    "region": account.region,
                    "role": account.role,
                    "priority": account.priority,
                    "notes": account.notes,
                }
            )
    return path


def match_web3_narratives(text: str, keywords: Web3NarrativeKeywords) -> List[str]:
    lowered = text.lower()
    matched = []
    for group, terms in keywords.groups.items():
        if any(term.lower() in lowered for term in terms):
            matched.append(group)
    return matched


def match_web3_terms(text: str, keywords: Web3NarrativeKeywords) -> Mapping[str, Tuple[str, ...]]:
    lowered = text.lower()
    matches = {}
    for group, terms in keywords.groups.items():
        group_matches = tuple(term for term in terms if term.lower() in lowered)
        if group_matches:
            matches[group] = group_matches
    return matches


def _build_mentions(posts: Sequence[dict], keywords: Web3NarrativeKeywords) -> List[dict]:
    mentions = []
    for post in posts:
        term_matches = match_web3_terms(str(post.get("text") or ""), keywords)
        for narrative, terms in term_matches.items():
            mentions.append(
                {
                    "account": post["handle"],
                    "narrative_key": narrative,
                    "post_id": post["post_id"],
                    "created_at": post["created_at"],
                    "public_metrics": post.get("public_metrics") or {},
                    "referenced_tweets": post.get("referenced_tweets") or [],
                    "text": post.get("text") or "",
                    "entities": _entities_from_terms(terms),
                    "direction": _direction_from_text(post.get("text") or ""),
                }
            )
    return mentions


def _direct_market_mentions(posts: Sequence[dict], markets: Sequence[dict]) -> List[dict]:
    linked = []
    for post in posts:
        post_id = str(post.get("post_id") or "")
        post_text = str(post.get("text") or "")
        if not post_id or not post_text:
            continue
        post_terms = words(post_text)
        for market in markets:
            market_terms = _market_terms(market)
            overlap = sorted((post_terms & market_terms) - _GENERIC_MARKET_TERMS)
            if not overlap:
                continue
            strong = [term for term in overlap if term in _strong_terms(market)]
            if len(overlap) < 2 and not strong:
                continue
            confidence = min(0.95, 0.55 + len(overlap) * 0.08 + (0.12 if strong else 0))
            entity = strong[0] if strong else overlap[0]
            linked.append(
                {
                    "account": str(post.get("handle") or "").lstrip("@"),
                    "post_id": post_id,
                    "market_slug": market["market_slug"],
                    "narrative_key": f"market:{market['market_slug']}",
                    "entity": entity,
                    "confidence": confidence,
                    "direction": _direction_from_text(post_text),
                    "post_created_at": post.get("created_at"),
                    "referenced_tweets": post.get("referenced_tweets") or [],
                }
            )
    return linked


def _mentions_from_market_mentions(market_mentions: Sequence[dict]) -> List[dict]:
    mentions = []
    for item in market_mentions:
        mentions.append(
            {
                "account": item["account"],
                "narrative_key": item["narrative_key"],
                "post_id": item["post_id"],
                "created_at": item["post_created_at"],
                "public_metrics": {},
                "referenced_tweets": item.get("referenced_tweets") or [],
                "text": "",
                "entities": [item.get("entity")] if item.get("entity") else [],
                "direction": item.get("direction") or "watch_only",
            }
        )
    return mentions


def _dedupe_mentions(mentions: Sequence[dict]) -> List[dict]:
    seen = set()
    result = []
    for mention in mentions:
        key = (mention.get("account"), mention.get("post_id"), mention.get("narrative_key"))
        if key in seen:
            continue
        seen.add(key)
        result.append(mention)
    return result


def _dedupe_market_mentions(market_mentions: Sequence[dict]) -> List[dict]:
    seen = set()
    result = []
    for mention in sorted(market_mentions, key=lambda item: float(item.get("confidence") or 0), reverse=True):
        key = (mention.get("post_id"), mention.get("market_slug"), mention.get("narrative_key"), mention.get("entity") or "")
        if key in seen:
            continue
        seen.add(key)
        result.append(mention)
    return result


def _clear_account_analysis_outputs(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("delete from account_narrative_mentions")
    con.execute("delete from account_market_mentions")
    con.execute("delete from account_market_outcomes")
    con.execute("delete from account_source_chains")


def _mention_rows(mentions: Sequence[dict]) -> List[dict]:
    grouped = defaultdict(list)
    for mention in mentions:
        grouped[(mention["account"], mention["narrative_key"])].append(mention)
    return [
        {
            "account": account,
            "narrative_key": narrative,
            "first_seen_at": min(item["created_at"] for item in items),
            "post_count": len(items),
            "matched_markets": [],
        }
        for (account, narrative), items in grouped.items()
    ]


def _impact_metrics(
    accounts: Mapping[str, dict],
    mentions: Sequence[dict],
    chains: Sequence[dict],
    market_mentions: Sequence[dict],
    outcomes: Sequence[dict],
    lookback: str,
    keywords: Web3NarrativeKeywords,
) -> List[dict]:
    by_account = defaultdict(list)
    by_narrative = defaultdict(list)
    for mention in mentions:
        by_account[mention["account"]].append(mention)
        by_narrative[mention["narrative_key"]].append(mention)

    speed = _speed_scores(by_narrative)
    cascade = _cascade_scores(by_narrative)
    market = _market_impact_scores(outcomes)
    outcome_stats = _outcome_stats(outcomes)
    linked_counts = Counter(row["account"] for row in market_mentions if float(row.get("confidence") or 0) >= 0.6)
    chain_score = Counter()
    for chain in chains:
        if chain.get("evidence_type") == "following_lead":
            multiplier = 20
        elif chain.get("evidence_type") == "post_reference_lead":
            multiplier = 16
        else:
            multiplier = 4
        chain_score[chain["upstream_account"]] += float(chain.get("confidence") or 0) * multiplier
    rows = []
    for account, profile in accounts.items():
        account_mentions = by_account.get(account, [])
        role = profile.get("role") or "fast_curators"
        stats = outcome_stats.get(account, {})
        sample_size = int(stats.get("sample_size") or 0)
        market_link_coverage = linked_counts.get(account, 0) / max(1, len(account_mentions))
        frequency_score = min(20.0, len(account_mentions) * 2.5)
        if _single_account_spam(account, by_narrative):
            frequency_score = min(frequency_score, 8.0)
        false_fomo_rate = float(stats.get("false_fomo_rate") or 0)
        role_weight = keywords.role_weights.get(role, 10)
        if role == "confirmation_sources":
            final_score = 0.0
        else:
            final_score = (
                speed.get(account, 0.0) * 0.20
                + frequency_score * 0.12
                + cascade.get(account, 0.0) * 0.14
                + market.get(account, 0.0) * 0.34
                + min(20.0, chain_score.get(account, 0.0)) * 0.12
                + role_weight * 0.04
                + min(10.0, market_link_coverage * 10) * 0.04
                - false_fomo_rate * 12
            )
        rows.append(
            {
                "account": account,
                "lookback": lookback,
                "speed_score": round(speed.get(account, 0.0), 4),
                "frequency_score": round(frequency_score, 4),
                "cascade_score": round(cascade.get(account, 0.0), 4),
                "market_impact_score": round(market.get(account, 0.0), 4),
                "source_chain_score": round(min(20.0, chain_score.get(account, 0.0)), 4),
                "false_fomo_rate": round(false_fomo_rate, 4),
                "final_score": round(max(0.0, min(100.0, final_score)), 4),
                "recommended_status": _recommended_status(role, final_score, false_fomo_rate, len(account_mentions), sample_size),
                "sample_size": sample_size,
                "market_link_coverage": round(market_link_coverage, 4),
                "hit_rate_6h": stats.get("hit_rate_6h"),
                "hit_rate_24h": stats.get("hit_rate_24h"),
                "hit_rate_72h": stats.get("hit_rate_72h"),
                "avg_favorable_move": stats.get("avg_favorable_move"),
                "avg_adverse_move": stats.get("avg_adverse_move"),
            }
        )
    return rows


def _speed_scores(by_narrative: Mapping[str, Sequence[dict]]) -> Counter:
    scores = Counter()
    for mentions in by_narrative.values():
        ordered = sorted(mentions, key=lambda item: item["created_at"])
        if not ordered:
            continue
        first_time = to_datetime(ordered[0]["created_at"])
        if first_time is None:
            continue
        for idx, mention in enumerate(ordered):
            ts = to_datetime(mention["created_at"])
            if ts is None:
                continue
            lead_minutes = (ts - first_time).total_seconds() / 60
            if idx == 0:
                scores[mention["account"]] += 25
            elif lead_minutes <= 60:
                scores[mention["account"]] += 16
            elif lead_minutes <= 360:
                scores[mention["account"]] += 8
            else:
                scores[mention["account"]] += 2
    return _cap_counter(scores, 25)


def _cascade_scores(by_narrative: Mapping[str, Sequence[dict]]) -> Counter:
    scores = Counter()
    for mentions in by_narrative.values():
        ordered = sorted(mentions, key=lambda item: item["created_at"])
        for mention in ordered:
            ts = to_datetime(mention["created_at"])
            if ts is None:
                continue
            later_handles = {
                item["account"]
                for item in ordered
                if item["account"] != mention["account"]
                and to_datetime(item["created_at"]) is not None
                and 0 < (to_datetime(item["created_at"]) - ts).total_seconds() <= 6 * 3600
            }
            metrics = mention.get("public_metrics") or {}
            reposts = float(metrics.get("retweet_count") or metrics.get("repost_count") or 0)
            quotes = float(metrics.get("quote_count") or 0)
            scores[mention["account"]] += min(18, len(later_handles) * 4 + (reposts + quotes) / 25)
    return _cap_counter(scores, 20)


def _market_impact_scores(outcomes: Sequence[dict]) -> Counter:
    scores = Counter()
    if not outcomes:
        return scores
    by_account = defaultdict(list)
    for outcome in outcomes:
        if outcome.get("horizon") == "24h":
            by_account[outcome["account"]].append(outcome)
    for account, rows in by_account.items():
        if not rows:
            continue
        positives = sum(1 for row in rows if row.get("is_positive"))
        avg_favorable = sum(float(row.get("max_favorable_delta") or 0) for row in rows) / len(rows)
        scores[account] = min(20, 12 * positives / len(rows) + min(8, avg_favorable * 100))
    return scores


def _source_chains(mentions: Sequence[dict], con: duckdb.DuckDBPyConnection) -> List[dict]:
    following_edges = _following_edges(con)
    by_narrative = defaultdict(list)
    for mention in mentions:
        by_narrative[mention["narrative_key"]].append(mention)
    chain_map = {}
    for narrative, rows in by_narrative.items():
        ordered = sorted(rows, key=lambda item: item["created_at"])
        for later in ordered:
            later_ts = to_datetime(later["created_at"])
            if later_ts is None:
                continue
            for earlier in ordered:
                if earlier["account"] == later["account"]:
                    continue
                earlier_ts = to_datetime(earlier["created_at"])
                if earlier_ts is None or earlier_ts >= later_ts:
                    continue
                lead = (later_ts - earlier_ts).total_seconds() / 60
                if lead > 24 * 60:
                    continue
                for evidence in _chain_evidence_types(later, earlier, following_edges):
                    key = (later["account"], earlier["account"], evidence)
                    current = chain_map.setdefault(
                        key,
                        {
                            "downstream_account": later["account"],
                            "upstream_account": earlier["account"],
                            "evidence_type": evidence,
                            "lead_time_minutes": lead,
                            "shared_narratives": set(),
                            "confidence": 0.0,
                        },
                    )
                    current["lead_time_minutes"] = min(current["lead_time_minutes"], lead)
                    current["shared_narratives"].add(narrative)
                    current["confidence"] += _chain_confidence_increment(evidence)
    chains = []
    for item in chain_map.values():
        cap = _chain_confidence_cap(item["evidence_type"])
        chains.append(
            {
                **item,
                "shared_narratives": sorted(item["shared_narratives"]),
                "confidence": min(cap, item["confidence"]),
            }
        )
    return sorted(chains, key=lambda item: item["confidence"], reverse=True)


def _chain_evidence_types(later: Mapping[str, object], earlier: Mapping[str, object], following_edges: set) -> List[str]:
    evidence = []
    if _references_post(later.get("referenced_tweets"), str(earlier.get("post_id") or "")):
        evidence.append("post_reference_lead")
    if (later["account"], earlier["account"]) in following_edges:
        evidence.append("following_lead")
    return evidence or ["same_narrative_lead"]


def _references_post(referenced_tweets: object, post_id: str) -> bool:
    if not post_id:
        return False
    if isinstance(referenced_tweets, str):
        referenced_tweets = _loads(referenced_tweets, [])
    if not isinstance(referenced_tweets, list):
        return False
    for item in referenced_tweets:
        if isinstance(item, Mapping) and str(item.get("id") or "") == post_id:
            return True
        if str(item) == post_id:
            return True
    return False


def _chain_confidence_increment(evidence_type: str) -> float:
    if evidence_type == "following_lead":
        return 0.25
    if evidence_type == "post_reference_lead":
        return 0.18
    return 0.04


def _chain_confidence_cap(evidence_type: str) -> float:
    if evidence_type == "following_lead":
        return 1.0
    if evidence_type == "post_reference_lead":
        return 0.8
    return 0.35


def _market_mentions(mentions: Sequence[dict], markets: Sequence[dict], keywords: Web3NarrativeKeywords) -> List[dict]:
    linked = []
    for mention in mentions:
        entities = mention.get("entities") or []
        for market in markets:
            market_text = _market_haystack(market)
            entity_hits = [entity for entity in entities if entity and entity.lower() in market_text]
            if entity_hits:
                confidence = 0.9
            else:
                group_terms = keywords.groups.get(mention["narrative_key"], ())
                confidence = 0.5 if any(term.lower() in market_text for term in group_terms) else 0.0
            if confidence < 0.45:
                continue
            linked.append(
                {
                    "account": mention["account"],
                    "post_id": mention["post_id"],
                    "market_slug": market["market_slug"],
                    "narrative_key": mention["narrative_key"],
                    "entity": entity_hits[0] if entity_hits else mention["narrative_key"],
                    "confidence": confidence,
                    "direction": mention.get("direction") or "watch_only",
                    "post_created_at": mention["created_at"],
                }
            )
    return linked


def _market_outcomes(market_mentions: Sequence[dict], ticks: Sequence[dict]) -> List[dict]:
    if not market_mentions or not ticks:
        return []
    ticks_by_market = defaultdict(list)
    for tick in sorted(ticks, key=lambda item: item["observed_at"]):
        if tick.get("market_slug") and tick.get("mid") is not None:
            ticks_by_market[tick["market_slug"]].append(tick)
    outcomes = []
    seen = set()
    for mention in market_mentions:
        if float(mention.get("confidence") or 0) < 0.6:
            continue
        created_at = to_datetime(mention.get("post_created_at"))
        if created_at is None:
            continue
        market_ticks = ticks_by_market.get(mention["market_slug"], [])
        entry = _nearest_tick(market_ticks, created_at, before=True)
        if entry is None:
            continue
        entry_mid = float(entry["mid"])
        direction_sign = _direction_sign(mention.get("direction"))
        for horizon, hours in (("6h", 6), ("24h", 24), ("72h", 72)):
            key = (mention["post_id"], mention["market_slug"], horizon)
            if key in seen:
                continue
            seen.add(key)
            end_at = created_at + timedelta(hours=hours)
            window_ticks = [
                tick for tick in market_ticks
                if to_datetime(tick["observed_at"]) is not None and created_at <= to_datetime(tick["observed_at"]) <= end_at
            ]
            if not window_ticks:
                continue
            future_mid = float(window_ticks[-1]["mid"])
            signed_moves = [(float(tick["mid"]) - entry_mid) * direction_sign for tick in window_ticks]
            if direction_sign == 0:
                signed_moves = [abs(float(tick["mid"]) - entry_mid) for tick in window_ticks]
                delta = abs(future_mid - entry_mid)
            else:
                delta = (future_mid - entry_mid) * direction_sign
            outcomes.append(
                {
                    "account": mention["account"],
                    "post_id": mention["post_id"],
                    "market_slug": mention["market_slug"],
                    "horizon": horizon,
                    "entry_mid": entry_mid,
                    "future_mid": future_mid,
                    "delta": delta,
                    "max_favorable_delta": max(signed_moves),
                    "max_adverse_delta": min(signed_moves),
                    "is_positive": delta >= 0.03 or max(signed_moves) >= 0.03,
                }
            )
    return outcomes


def _outcome_stats(outcomes: Sequence[dict]) -> Mapping[str, dict]:
    grouped = defaultdict(list)
    for outcome in outcomes:
        grouped[outcome["account"]].append(outcome)
    stats = {}
    for account, rows in grouped.items():
        by_horizon = defaultdict(list)
        for row in rows:
            by_horizon[row["horizon"]].append(row)
        all_rows = by_horizon.get("24h") or rows
        positives = sum(1 for row in all_rows if row.get("is_positive"))
        stats[account] = {
            "sample_size": len(all_rows),
            "false_fomo_rate": 1 - positives / len(all_rows) if all_rows else 0,
            "hit_rate_6h": _hit_rate(by_horizon.get("6h", [])),
            "hit_rate_24h": _hit_rate(by_horizon.get("24h", [])),
            "hit_rate_72h": _hit_rate(by_horizon.get("72h", [])),
            "avg_favorable_move": _avg(row.get("max_favorable_delta") for row in all_rows),
            "avg_adverse_move": _avg(row.get("max_adverse_delta") for row in all_rows),
        }
    return stats


def _accounts(con: duckdb.DuckDBPyConnection) -> Mapping[str, dict]:
    rows = con.execute("select handle, language, role, region, priority, followers, following from x_accounts").fetchall()
    columns = [desc[0] for desc in con.description]
    return {row[0]: dict(zip(columns, row)) for row in rows}


def _markets(con: duckdb.DuckDBPyConnection) -> List[dict]:
    rows = con.execute(
        """
        select market_slug, event_slug, question, category, tags, end_time, liquidity
        from markets
        """
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["tags"] = _loads(item.get("tags"), [])
        result.append(item)
    return result


def _posts(con: duckdb.DuckDBPyConnection, since_iso: str) -> List[dict]:
    rows = con.execute(
        """
        select post_id, handle, created_at, text, public_metrics, referenced_tweets, lang
        from x_posts
        where created_at >= ?
        order by created_at
        """,
        [since_iso],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    result = []
    for row in rows:
        item = dict(zip(columns, row))
        item["public_metrics"] = _loads(item.get("public_metrics"), {})
        item["referenced_tweets"] = _loads(item.get("referenced_tweets"), [])
        item["created_at"] = str(item["created_at"])
        result.append(item)
    return result


def _market_ticks(con: duckdb.DuckDBPyConnection, since_iso: str) -> List[dict]:
    rows = con.execute(
        """
        select observed_at, market_slug, mid
        from market_ticks
        where observed_at >= ? and mid is not null
        order by observed_at
        """,
        [since_iso],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _following_edges(con: duckdb.DuckDBPyConnection) -> set:
    rows = con.execute(
        "select source_handle, target_handle from x_follow_graph where relationship = 'following'"
    ).fetchall()
    return {(row[0], row[1]) for row in rows}


def _latest_account_metrics(con: duckdb.DuckDBPyConnection, lookback: str) -> List[dict]:
    rows = con.execute(
        """
        select account, speed_score, frequency_score, cascade_score, market_impact_score,
               source_chain_score, false_fomo_rate, final_score, recommended_status
        from account_impact_metrics
        where lookback = ?
        order by final_score desc
        """,
        [lookback],
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _latest_source_chains(con: duckdb.DuckDBPyConnection) -> List[dict]:
    rows = con.execute(
        """
        select downstream_account, upstream_account, evidence_type, lead_time_minutes, shared_narratives, confidence
        from account_source_chains
        order by confidence desc, lead_time_minutes asc
        """
    ).fetchall()
    columns = [desc[0] for desc in con.description]
    return [dict(zip(columns, row)) for row in rows]


def _default_profile(handle: str) -> dict:
    return {
        "handle": handle,
        "language": "mixed",
        "role": "elite_information",
        "region": "global",
        "priority": "ad_hoc",
        "followers": None,
        "following": None,
        "verified": None,
        "status": "active",
    }


def _empty_metric(handle: str, lookback: str) -> dict:
    return {
        "account": handle,
        "lookback": lookback,
        "speed_score": 0.0,
        "frequency_score": 0.0,
        "cascade_score": 0.0,
        "market_impact_score": 0.0,
        "source_chain_score": 0.0,
        "false_fomo_rate": 0.0,
        "final_score": 0.0,
        "recommended_status": "insufficient_x_data",
        "sample_size": 0,
        "market_link_coverage": 0.0,
    }


def _classified_posts(
    posts: Sequence[dict],
    mentions: Sequence[dict],
    market_mentions: Sequence[dict],
    outcomes: Sequence[dict],
) -> List[dict]:
    mentions_by_post = defaultdict(list)
    markets_by_post = defaultdict(list)
    outcomes_by_post = defaultdict(list)
    for mention in mentions:
        mentions_by_post[str(mention.get("post_id") or "")].append(mention)
    for mention in market_mentions:
        markets_by_post[str(mention.get("post_id") or "")].append(mention)
    for outcome in outcomes:
        outcomes_by_post[str(outcome.get("post_id") or "")].append(outcome)
    rows = []
    for post in sorted(posts, key=lambda row: str(row.get("created_at") or ""), reverse=True):
        post_id = str(post.get("post_id") or "")
        post_outcomes = outcomes_by_post.get(post_id, [])
        if any(outcome.get("is_positive") for outcome in post_outcomes):
            classification = "price_validated_candidate"
        elif post_outcomes:
            classification = "false_or_unproven_fomo"
        elif markets_by_post.get(post_id):
            classification = "matched_market_no_tick_data"
        elif mentions_by_post.get(post_id):
            classification = "narrative_context_only"
        else:
            classification = "non_actionable_context"
        rows.append(
            {
                "post_id": post_id,
                "created_at": str(post.get("created_at") or ""),
                "classification": classification,
                "markets": sorted({str(row.get("market_slug") or "") for row in markets_by_post.get(post_id, []) if row.get("market_slug")}),
            }
        )
    return rows


def _account_tradability(metric: Mapping[str, object], outcomes: Sequence[dict]) -> dict:
    sample_size = int(metric.get("sample_size") or 0)
    if sample_size == 0:
        return {
            "status": "insufficient_price_evidence",
            "cost_first_failure": "no_matched_tick_outcomes",
            "required_check": "collect market ticks around matched posts before promotion",
        }
    false_fomo = float(metric.get("false_fomo_rate") or 0)
    if false_fomo >= 0.5:
        status = "blocked"
        failure = "false_fomo"
    else:
        status = "research_candidate"
        failure = "unknown_until_spread_slippage_check"
    return {
        "status": status,
        "cost_first_failure": failure,
        "sample_size": sample_size,
        "positive_outcomes": sum(1 for outcome in outcomes if outcome.get("is_positive")),
        "required_check": "evaluate 1m/5m/10m path with spread, liquidity, and already-moved filters",
    }


def _account_participant_lens(metric: Mapping[str, object], outcomes: Sequence[dict]) -> dict:
    sample_size = int(metric.get("sample_size") or 0)
    if sample_size == 0:
        return {
            "retail": "watch_only_until_price_path_exists",
            "institution": "insufficient_repeatability",
            "market_maker": "context_only_no_adverse_selection_signal",
        }
    if float(metric.get("false_fomo_rate") or 0) >= 0.5:
        return {
            "retail": "blocked_by_false_fomo",
            "institution": "blocked_by_low_hit_rate",
            "market_maker": "possible_noise_flow_only",
        }
    return {
        "retail": "candidate_after_simple_entry_exit_check",
        "institution": "candidate_after_capacity_and_sample_size_check",
        "market_maker": "watch_for_adverse_selection_and_inventory_pressure",
    }


def _account_failure_mode(metric: Mapping[str, object], post_count: int, market_links: int, outcomes: int) -> str:
    if post_count == 0:
        return "no public posts in the selected lookback or missing X/CSV input"
    if market_links == 0:
        return "posts did not strongly map to configured Polymarket markets"
    if outcomes == 0:
        return "market links exist but local ticks are insufficient for price-path validation"
    if float(metric.get("false_fomo_rate") or 0) >= 0.5:
        return "matched posts did not repeatedly precede favorable price movement"
    return "sample may still be too small or cost-sensitive after spread and slippage"


def _nearest_tick(ticks: Sequence[dict], target: datetime, before: bool) -> Optional[dict]:
    candidates = [tick for tick in ticks if to_datetime(tick["observed_at"]) is not None]
    if before:
        candidates = [tick for tick in candidates if to_datetime(tick["observed_at"]) <= target]
        return candidates[-1] if candidates else None
    candidates = [tick for tick in candidates if to_datetime(tick["observed_at"]) >= target]
    return candidates[0] if candidates else None


def _false_fomo_rate(mentions: Sequence[dict], market_score: float) -> float:
    if not mentions:
        return 0.0
    if market_score > 0:
        return 0.0
    return min(1.0, len(mentions) / 10)


def _single_account_spam(account: str, by_narrative: Mapping[str, Sequence[dict]]) -> bool:
    for mentions in by_narrative.values():
        if len(mentions) >= 3 and {mention["account"] for mention in mentions} == {account}:
            return True
    return False


def _recommended_status(role: str, final_score: float, false_fomo_rate: float, mention_count: int, sample_size: int) -> str:
    if role == "confirmation_sources":
        return "confirmation_source"
    if mention_count == 0:
        return "insufficient_x_data"
    if sample_size == 0:
        return "insufficient_market_data"
    if false_fomo_rate >= 0.6:
        return "rejected"
    if final_score >= 18:
        return "ranked"
    if final_score >= 8:
        return "watch"
    return "noise_or_late"


def _entities_from_terms(terms: Sequence[str]) -> List[str]:
    generic = {
        "listing",
        "listed",
        "airdrop",
        "points",
        "claim",
        "eligibility",
        "regulation",
        "approval",
        "enforcement",
        "hack",
        "exploit",
        "stolen",
        "drained",
        "compromised",
        "上线",
        "上币",
        "空投",
        "积分",
        "领取",
        "资格",
        "监管",
        "批准",
        "诉讼",
        "执法",
        "被盗",
        "黑客",
        "漏洞",
        "攻击",
        "被攻击",
    }
    return sorted({term for term in terms if term.lower() not in generic and len(term) >= 2})


def _direction_from_text(text: str) -> str:
    lowered = text.lower()
    bullish = ("buy", "bought", "long", "pump", "rally", "surge", "accumulate", "流入", "买入", "做多", "反弹", "拉盘")
    bearish = ("sell", "sold", "short", "dump", "hack", "exploit", "stolen", "drain", "被盗", "攻击", "做空", "卖出", "暴跌")
    bull_hits = sum(1 for term in bullish if term in lowered)
    bear_hits = sum(1 for term in bearish if term in lowered)
    if bull_hits > bear_hits:
        return "bullish"
    if bear_hits > bull_hits:
        return "bearish"
    return "watch_only"


def _direction_sign(direction: object) -> int:
    if direction == "bullish":
        return 1
    if direction == "bearish":
        return -1
    return 0


def _market_haystack(market: Mapping[str, object]) -> str:
    tags = market.get("tags") or []
    if not isinstance(tags, list):
        tags = _loads(tags, [])
    return " ".join(
        [
            str(market.get("market_slug") or ""),
            str(market.get("event_slug") or ""),
            str(market.get("question") or ""),
            str(market.get("category") or ""),
            " ".join(str(tag) for tag in tags),
        ]
    ).lower()


def _market_terms(market: Mapping[str, object]) -> set:
    return words(_market_haystack(market)) - _GENERIC_MARKET_TERMS


def _strong_terms(market: Mapping[str, object]) -> set:
    terms = _market_terms(market)
    slug_terms = words(str(market.get("market_slug") or ""))
    tag_terms = words(" ".join(str(tag) for tag in (market.get("tags") or [] if isinstance(market.get("tags"), list) else [])))
    strong = {term for term in terms if term in slug_terms or term in tag_terms}
    strong.update({term for term in terms if term.isupper() or any(char.isdigit() for char in term)})
    return strong


def _hit_rate(rows: Sequence[dict]) -> Optional[float]:
    if not rows:
        return None
    return sum(1 for row in rows if row.get("is_positive")) / len(rows)


def _avg(values: Iterable[object]) -> Optional[float]:
    floats = []
    for value in values:
        if value is None:
            continue
        try:
            floats.append(float(value))
        except (TypeError, ValueError):
            continue
    if not floats:
        return None
    return sum(floats) / len(floats)


_GENERIC_MARKET_TERMS = {
    "will",
    "market",
    "markets",
    "prediction",
    "predict",
    "price",
    "before",
    "after",
    "this",
    "that",
    "the",
    "and",
    "are",
    "for",
    "from",
    "with",
    "into",
    "onto",
    "about",
    "after",
    "before",
    "during",
    "across",
    "still",
    "just",
    "has",
    "have",
    "had",
    "was",
    "were",
    "been",
    "being",
    "his",
    "her",
    "their",
    "its",
    "you",
    "your",
    "they",
    "them",
    "who",
    "what",
    "when",
    "where",
    "why",
    "how",
    "all",
    "new",
    "old",
    "full",
    "more",
    "most",
    "less",
    "than",
    "then",
    "now",
    "ago",
    "next",
    "month",
    "year",
    "week",
    "today",
    "tomorrow",
    "2025",
    "2026",
    "2027",
    "2028",
    "yes",
    "not",
    "hit",
    "reach",
    "above",
    "below",
    "over",
    "under",
    "open",
    "close",
    "closed",
    "trade",
    "trading",
    "token",
    "crypto",
    "polymarket",
}


def _cap_counter(counter: Counter, cap: float) -> Counter:
    return Counter({key: min(cap, value) for key, value in counter.items()})


def _json_or_empty(value: object, default=None):
    if default is None:
        default = {}
    if not value:
        return default
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return default


def _loads(value: object, default):
    if isinstance(value, (dict, list)):
        return value
    if not isinstance(value, str):
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _fmt(value: object) -> str:
    try:
        return f"{float(value):.2f}"
    except (TypeError, ValueError):
        return "n/a"
