from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import requests

from .config import (
    GAMMA_API_BASE_URL,
    POLYMARKET_CLOB_BASE_URL,
    POLYMARKET_CLOB_WS_URL,
    X_API_BASE_URL,
)
from .models import MarketRecord, SocialPost
from .utils import first_float, first_text, parse_timestamp, utc_now_iso


class GammaMarketClient:
    def __init__(self, base_url: str = GAMMA_API_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "signal-foundry-research-os/0.1"})

    def list_markets(self, limit: int = 500, max_pages: int = 3) -> List[dict]:
        markets: List[dict] = []
        for page in range(max_pages):
            response = self.session.get(
                f"{self.base_url}/markets",
                params={"limit": limit, "offset": page * limit, "closed": "false"},
                timeout=self.timeout,
            )
            response.raise_for_status()
            payload = response.json()
            rows = _coerce_rows(payload)
            markets.extend(rows)
            if len(rows) < limit:
                break
        return markets

    def discover_political_markets(self, keywords: Sequence[str], max_pages: int = 3) -> List[MarketRecord]:
        return self.discover_markets(keywords, max_pages=max_pages, category="politics")

    def discover_markets(self, keywords: Sequence[str], max_pages: int = 3, category: str = "crypto") -> List[MarketRecord]:
        records: List[MarketRecord] = []
        seen: set = set()
        for item in self.list_markets(max_pages=max_pages):
            record = market_record_from_gamma(item)
            if not record.market_slug or record.market_slug in seen:
                continue
            if is_market_match(record, keywords, category):
                records.append(record)
                seen.add(record.market_slug)
        return records


class DataApiClient:
    def __init__(self, base_url: str = "https://data-api.polymarket.com", timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "signal-foundry-research-os/0.1"})

    def activity(self, address: str, limit: int = 500, max_rows: int = 5000) -> List[dict]:
        rows: List[dict] = []
        offset = 0
        while len(rows) < max_rows:
            response = self.session.get(
                f"{self.base_url}/activity",
                params={"user": address, "limit": limit, "offset": offset},
                timeout=self.timeout,
            )
            if response.status_code == 400 and rows:
                break
            response.raise_for_status()
            page = _coerce_rows(response.json())
            rows.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return rows[:max_rows]


class XApiClient:
    def __init__(self, bearer_token: str, base_url: str = X_API_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {bearer_token}", "Accept": "application/json"})

    def validate_handle(self, handle: str) -> bool:
        response = self.session.get(f"{self.base_url}/users/by/username/{handle}", timeout=self.timeout)
        return response.status_code == 200 and bool(response.json().get("data"))

    def user_id(self, handle: str) -> Optional[str]:
        response = self.session.get(f"{self.base_url}/users/by/username/{handle}", timeout=self.timeout)
        if response.status_code != 200:
            return None
        data = response.json().get("data") or {}
        return str(data.get("id")) if data.get("id") else None

    def user_profile(self, handle: str) -> Optional[dict]:
        response = self.session.get(
            f"{self.base_url}/users/by/username/{handle}",
            params={"user.fields": "created_at,description,verified,public_metrics,location"},
            timeout=self.timeout,
        )
        if response.status_code != 200:
            return None
        data = response.json().get("data") or {}
        return data if isinstance(data, dict) else None

    def backfill_user_posts(self, handle: str, seconds: int, max_results: int = 100) -> List[SocialPost]:
        user_id = self.user_id(handle)
        if not user_id:
            return []
        start_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        response = self.session.get(
            f"{self.base_url}/users/{user_id}/tweets",
            params={
                "max_results": max(5, min(max_results, 100)),
                "start_time": start_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "tweet.fields": "created_at,author_id,entities,referenced_tweets,public_metrics",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return [post_from_x(handle, item) for item in payload.get("data") or [] if isinstance(item, dict)]

    def backfill_user_post_dicts(self, handle: str, seconds: int, max_results: int = 100) -> List[dict]:
        user_id = self.user_id(handle)
        if not user_id:
            return []
        start_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        response = self.session.get(
            f"{self.base_url}/users/{user_id}/tweets",
            params={
                "max_results": max(5, min(max_results, 100)),
                "start_time": start_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "tweet.fields": "created_at,author_id,entities,referenced_tweets,public_metrics,lang",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return [post_dict_from_x(handle, item) for item in payload.get("data") or [] if isinstance(item, dict)]

    def following(self, handle: str, max_results: int = 100) -> List[dict]:
        user_id = self.user_id(handle)
        if not user_id:
            return []
        response = self.session.get(
            f"{self.base_url}/users/{user_id}/following",
            params={"max_results": max(1, min(max_results, 1000)), "user.fields": "username,verified,public_metrics"},
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        return [item for item in payload.get("data") or [] if isinstance(item, dict)]

    def recent_search(self, query: str, seconds: int, max_results: int = 100) -> List[dict]:
        start_time = datetime.now(timezone.utc) - timedelta(seconds=seconds)
        response = self.session.get(
            f"{self.base_url}/tweets/search/recent",
            params={
                "query": query,
                "max_results": max(10, min(max_results, 100)),
                "start_time": start_time.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
                "tweet.fields": "created_at,author_id,entities,referenced_tweets,public_metrics,lang",
                "expansions": "author_id",
                "user.fields": "username,verified,public_metrics",
            },
            timeout=self.timeout,
        )
        response.raise_for_status()
        payload = response.json()
        users = {
            str(user.get("id")): user.get("username")
            for user in (payload.get("includes") or {}).get("users", [])
            if isinstance(user, dict)
        }
        rows = []
        for item in payload.get("data") or []:
            if isinstance(item, dict):
                rows.append(post_dict_from_x(users.get(str(item.get("author_id")), str(item.get("author_id"))), item))
        return rows

    def recent_counts(self, query: str) -> dict:
        response = self.session.get(f"{self.base_url}/tweets/counts/recent", params={"query": query}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def filtered_stream_rules(self) -> dict:
        response = self.session.get(f"{self.base_url}/tweets/search/stream/rules", timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def set_filtered_stream_rule(self, value: str, tag: str = "signal-foundry") -> dict:
        response = self.session.post(
            f"{self.base_url}/tweets/search/stream/rules",
            json={"add": [{"value": value[:1024], "tag": tag}]},
            timeout=self.timeout,
        )
        response.raise_for_status()
        return response.json()


class CLOBClient:
    def __init__(self, base_url: str = POLYMARKET_CLOB_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "User-Agent": "signal-foundry-research-os/0.1"})

    def midpoint(self, token_id: str) -> Optional[float]:
        response = self.session.get(f"{self.base_url}/midpoint", params={"token_id": token_id}, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        return first_float(payload, "mid", "midpoint")


class CLOBWebSocket:
    def __init__(self, url: str = POLYMARKET_CLOB_WS_URL) -> None:
        self.url = url

    def stream(
        self,
        asset_ids: Sequence[str],
        on_message: Callable[[dict], None],
        seconds: int = 60,
    ) -> None:
        import websocket

        ws = websocket.create_connection(self.url, timeout=15)
        try:
            ws.send(json.dumps({"assets_ids": list(asset_ids), "type": "market", "custom_feature_enabled": True}))
            deadline = time.time() + seconds
            while time.time() < deadline:
                raw = ws.recv()
                if raw == "PING":
                    ws.send("PONG")
                    continue
                try:
                    payload = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    on_message(payload)
                elif isinstance(payload, list):
                    for item in payload:
                        if isinstance(item, dict):
                            on_message(item)
        finally:
            ws.close()


def post_from_x(handle: str, item: dict) -> SocialPost:
    post_id = str(item.get("id"))
    return SocialPost(
        platform="x",
        handle=handle,
        post_id=post_id,
        created_at=parse_timestamp(item.get("created_at")) or utc_now_iso(),
        text=str(item.get("text") or ""),
        url=f"https://x.com/{handle}/status/{post_id}",
        raw=item,
    )


def post_dict_from_x(handle: str, item: dict) -> dict:
    post_id = str(item.get("id"))
    return {
        "post_id": post_id,
        "handle": handle,
        "created_at": parse_timestamp(item.get("created_at")) or utc_now_iso(),
        "text": str(item.get("text") or ""),
        "public_metrics": item.get("public_metrics") or {},
        "referenced_tweets": item.get("referenced_tweets") or [],
        "lang": item.get("lang"),
        "raw_json": item,
    }


def market_record_from_gamma(item: dict) -> MarketRecord:
    question = first_text(item, "question", "title", "description", "slug") or ""
    slug = first_text(item, "slug", "marketSlug", "conditionId") or ""
    tags = _extract_tags(item)
    return MarketRecord(
        market_slug=slug,
        event_slug=first_text(item, "eventSlug", "event_slug"),
        question=question,
        category=first_text(item, "category", "eventCategory") or _category_from_tags(tags),
        tags=tags,
        end_time=parse_timestamp(first_text(item, "endDate", "end_date", "closedTime", "resolutionDate")),
        resolution_source=first_text(item, "resolutionSource", "resolution_source"),
        clob_token_ids=_extract_token_ids(item),
        liquidity=first_float(item, "liquidity", "liquidityNum"),
        raw=item,
    )


def is_political_market(market: MarketRecord, keywords: Sequence[str]) -> bool:
    haystack = " ".join([market.question, market.market_slug, market.event_slug or "", market.category or "", " ".join(market.tags)]).lower()
    political_markers = {
        "politic",
        "election",
        "trump",
        "vance",
        "iran",
        "israel",
        "gaza",
        "ukraine",
        "russia",
        "nato",
        "war",
        "ceasefire",
        "strike",
        "sanction",
        "tariff",
        "fed",
    }
    return any(marker in haystack for marker in political_markers) or any(keyword in haystack for keyword in keywords)


def is_crypto_market(market: MarketRecord, keywords: Sequence[str]) -> bool:
    haystack = " ".join([market.question, market.market_slug, market.event_slug or "", market.category or "", " ".join(market.tags)]).lower()
    crypto_markers = {
        "bitcoin",
        "btc",
        "ethereum",
        "eth",
        "solana",
        "sol",
        "crypto",
        "binance",
        "coinbase",
        "hype",
        "hyperliquid",
        "base",
        "airdrop",
        "token",
        "stablecoin",
        "memecoin",
        "meme",
        "etf",
        "sec",
        "hack",
        "exploit",
    }
    lowered_keywords = {str(keyword).lower() for keyword in keywords}
    return any(marker in haystack for marker in crypto_markers) or any(keyword in haystack for keyword in lowered_keywords)


def is_market_match(market: MarketRecord, keywords: Sequence[str], category: str) -> bool:
    if category == "politics":
        return is_political_market(market, keywords)
    if category in {"crypto", "web3"}:
        return is_crypto_market(market, keywords)
    haystack = " ".join([market.question, market.market_slug, market.event_slug or "", market.category or "", " ".join(market.tags)]).lower()
    return any(str(keyword).lower() in haystack for keyword in keywords)


def _coerce_rows(payload: object) -> List[dict]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
    return []


def _extract_tags(item: dict) -> List[str]:
    tags = item.get("tags")
    if not isinstance(tags, list):
        return []
    labels: List[str] = []
    for tag in tags:
        if isinstance(tag, dict):
            text = first_text(tag, "label", "name", "slug")
            if text:
                labels.append(text)
        elif isinstance(tag, str):
            labels.append(tag)
    return labels


def _category_from_tags(tags: Sequence[str]) -> str:
    return tags[0] if tags else "unknown"


def _extract_token_ids(item: dict) -> List[str]:
    raw = item.get("clobTokenIds") or item.get("clob_token_ids") or item.get("tokens")
    if isinstance(raw, str):
        try:
            decoded = json.loads(raw)
        except json.JSONDecodeError:
            return [raw] if raw.isdigit() else []
        return [str(value) for value in decoded if value]
    if isinstance(raw, list):
        token_ids = []
        for value in raw:
            if isinstance(value, dict):
                token = first_text(value, "token_id", "tokenId", "id")
                if token:
                    token_ids.append(token)
            elif value:
                token_ids.append(str(value))
        return token_ids
    return []
