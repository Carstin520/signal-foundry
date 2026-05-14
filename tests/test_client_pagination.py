import requests

from quant_sol.wallets.client import PolymarketClient


class FakeResponse:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code


class FakePaginatedClient(PolymarketClient):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    def _get_json(self, endpoint, params):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self.calls == 1:
            return [{"id": i} for i in range(2)]
        exc = requests.HTTPError("bad request at terminal offset")
        exc.response = FakeResponse(400)  # type: ignore[assignment]
        raise exc


def test_paginated_fetch_keeps_rows_when_terminal_offset_returns_400() -> None:
    client = FakePaginatedClient()

    rows = client._get_paginated("activity", {"user": "0xabc"}, limit=2)

    assert rows == [{"id": 0}, {"id": 1}]
    assert client.calls == 2

