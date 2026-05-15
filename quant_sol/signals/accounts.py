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
    upsert_account_narrative_mentions,
    upsert_account_source_chains,
    upsert_x_accounts,
    upsert_x_follow_graph,
    upsert_x_posts,
)
from .utils import to_datetime, utc_now_iso


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
    mentions = _build_mentions(posts, keyword_config)
    mention_rows = _mention_rows(mentions)
    chains = _source_chains(mentions, con)
    metrics = _impact_metrics(accounts, mentions, chains, ticks, lookback, keyword_config)
    upsert_account_narrative_mentions(con, mention_rows)
    upsert_account_source_chains(con, chains)
    upsert_account_impact_metrics(con, metrics)
    return sorted(metrics, key=lambda item: item["final_score"], reverse=True)


def write_account_report(con: duckdb.DuckDBPyConnection, lookback: str, report_root: Path) -> Path:
    report_root.mkdir(parents=True, exist_ok=True)
    rows = _latest_account_metrics(con, lookback)
    chains = _latest_source_chains(con)
    path = report_root / f"account_rankings_{lookback}.md"
    lines = [
        f"# Web3 X Account Ranking ({lookback})",
        "",
        "| Rank | Account | Final | Speed | Freq | Cascade | Market | Chain | False FOMO | Status |",
        "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |",
    ]
    if not rows:
        lines.append("| n/a | No account metrics | 0 | 0 | 0 | 0 | 0 | 0 | 0 | n/a |")
    for idx, row in enumerate(rows, start=1):
        lines.append(
            f"| {idx} | @{row['account']} | {_fmt(row['final_score'])} | {_fmt(row['speed_score'])} | "
            f"{_fmt(row['frequency_score'])} | {_fmt(row['cascade_score'])} | {_fmt(row['market_impact_score'])} | "
            f"{_fmt(row['source_chain_score'])} | {_fmt(row['false_fomo_rate'])} | {row['recommended_status']} |"
        )
    lines.extend(["", "## Source Chain Candidates", ""])
    if not chains:
        lines.append("- None.")
    for chain in chains[:20]:
        lines.append(
            f"- @{chain['upstream_account']} -> @{chain['downstream_account']}: "
            f"{_fmt(chain['lead_time_minutes'])} min lead, confidence {_fmt(chain['confidence'])}, "
            f"narratives {chain['shared_narratives']}"
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


def _build_mentions(posts: Sequence[dict], keywords: Web3NarrativeKeywords) -> List[dict]:
    mentions = []
    for post in posts:
        narratives = match_web3_narratives(str(post.get("text") or ""), keywords)
        for narrative in narratives:
            mentions.append(
                {
                    "account": post["handle"],
                    "narrative_key": narrative,
                    "post_id": post["post_id"],
                    "created_at": post["created_at"],
                    "public_metrics": post.get("public_metrics") or {},
                }
            )
    return mentions


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
    ticks: Sequence[dict],
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
    market = _market_impact_scores(by_account, ticks)
    chain_score = Counter()
    for chain in chains:
        chain_score[chain["upstream_account"]] += float(chain.get("confidence") or 0) * 20
    rows = []
    for account, profile in accounts.items():
        account_mentions = by_account.get(account, [])
        frequency_score = min(20.0, len(account_mentions) * 2.5)
        if _single_account_spam(account, by_narrative):
            frequency_score = min(frequency_score, 8.0)
        false_fomo_rate = _false_fomo_rate(account_mentions, market.get(account, 0.0))
        role = profile.get("role") or "fast_curators"
        role_weight = keywords.role_weights.get(role, 10)
        final_score = (
            speed.get(account, 0.0) * 0.28
            + frequency_score * 0.16
            + cascade.get(account, 0.0) * 0.18
            + market.get(account, 0.0) * 0.18
            + min(20.0, chain_score.get(account, 0.0)) * 0.15
            + role_weight * 0.05
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
                "recommended_status": _recommended_status(final_score, false_fomo_rate, len(account_mentions)),
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


def _market_impact_scores(by_account: Mapping[str, Sequence[dict]], ticks: Sequence[dict]) -> Counter:
    scores = Counter()
    if not ticks:
        return scores
    sorted_ticks = sorted(ticks, key=lambda item: item["observed_at"])
    for account, mentions in by_account.items():
        favorable = 0
        total = 0
        for mention in mentions:
            ts = to_datetime(mention["created_at"])
            if ts is None:
                continue
            before = _nearest_tick(sorted_ticks, ts, before=True)
            after = _nearest_tick(sorted_ticks, ts + timedelta(hours=24), before=True)
            if before is None or after is None:
                continue
            total += 1
            move = float(after["mid"]) - float(before["mid"])
            if abs(move) >= 0.03:
                favorable += 1
        if total:
            scores[account] = min(20, 20 * favorable / total)
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
                evidence = "following_lead" if (later["account"], earlier["account"]) in following_edges else "same_narrative_lead"
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
                current["confidence"] += 0.25 if evidence == "following_lead" else 0.12
    chains = []
    for item in chain_map.values():
        chains.append(
            {
                **item,
                "shared_narratives": sorted(item["shared_narratives"]),
                "confidence": min(1.0, item["confidence"]),
            }
        )
    return sorted(chains, key=lambda item: item["confidence"], reverse=True)


def _accounts(con: duckdb.DuckDBPyConnection) -> Mapping[str, dict]:
    rows = con.execute("select handle, language, role, region, priority, followers, following from x_accounts").fetchall()
    columns = [desc[0] for desc in con.description]
    return {row[0]: dict(zip(columns, row)) for row in rows}


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


def _recommended_status(final_score: float, false_fomo_rate: float, mention_count: int) -> str:
    if mention_count == 0:
        return "watch"
    if false_fomo_rate >= 0.6:
        return "rejected"
    if final_score >= 18:
        return "ranked"
    if final_score >= 8:
        return "watch"
    return "noise_or_late"


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
