from typing import Dict, Iterable, List
from urllib.parse import urlencode

import requests

from .config import DATA_API_BASE_URL, GAMMA_API_BASE_URL, WalletConfig
from .models import EndpointPayload


WALLET_ENDPOINTS = ("positions", "closed-positions", "activity", "value")


class PolymarketClient:
    def __init__(self, base_url: str = DATA_API_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def endpoint_url(self, endpoint: str, params: Dict[str, object]) -> str:
        return f"{self.base_url}/{endpoint}?{urlencode(params)}"

    def get_endpoint(self, endpoint: str, wallet: WalletConfig, limit: int = 500) -> EndpointPayload:
        if not wallet.address:
            raise ValueError(f"wallet {wallet.label} has no resolved address")

        params: Dict[str, object] = {"user": wallet.address}
        if endpoint in {"positions", "closed-positions", "activity"}:
            payload = self._get_paginated(endpoint, params, limit=limit)
        else:
            payload = self._get_json(endpoint, params)
        return EndpointPayload(endpoint=endpoint, params=params, payload=payload)

    def fetch_wallet(self, wallet: WalletConfig) -> List[EndpointPayload]:
        return [self.get_endpoint(endpoint, wallet) for endpoint in WALLET_ENDPOINTS]

    def _get_paginated(self, endpoint: str, base_params: Dict[str, object], limit: int) -> List[object]:
        rows: List[object] = []
        offset = 0
        while True:
            params = {**base_params, "limit": limit, "offset": offset}
            try:
                payload = self._get_json(endpoint, params)
            except requests.HTTPError as exc:
                status_code = exc.response.status_code if exc.response is not None else None
                if rows and status_code == 400:
                    break
                raise
            page = _coerce_rows(payload)
            rows.extend(page)
            if len(page) < limit:
                break
            offset += limit
        return rows

    def _get_json(self, endpoint: str, params: Dict[str, object]) -> object:
        response = self.session.get(
            f"{self.base_url}/{endpoint}",
            params=params,
            timeout=self.timeout,
            headers={"Accept": "application/json", "User-Agent": "quant-sol-wallet-collector/0.1"},
        )
        response.raise_for_status()
        return response.json()


class GammaClient:
    def __init__(self, base_url: str = GAMMA_API_BASE_URL, timeout: int = 30) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.session = requests.Session()

    def get_event_by_slug(self, slug: str) -> object:
        response = self.session.get(
            f"{self.base_url}/events",
            params={"slug": slug},
            timeout=self.timeout,
            headers={"Accept": "application/json", "User-Agent": "quant-sol-wallet-collector/0.1"},
        )
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, list) and payload:
            return payload[0]
        return {}


def _coerce_rows(payload: object) -> List[object]:
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "results", "items"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
    return []


def endpoint_urls_for_wallet(wallet: WalletConfig) -> Iterable[str]:
    client = PolymarketClient()
    if not wallet.address:
        return []
    return (
        client.endpoint_url(endpoint, {"user": wallet.address})
        for endpoint in WALLET_ENDPOINTS
    )
