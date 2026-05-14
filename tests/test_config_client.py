from quant_sol.wallets.client import PolymarketClient, endpoint_urls_for_wallet
from quant_sol.wallets.config import WATCHLIST, is_valid_address


def test_wallet_address_validation_skips_partial_address() -> None:
    assert is_valid_address(WATCHLIST["aviato"].address)
    assert not WATCHLIST["majorexploiter"].is_resolved
    assert not is_valid_address("0x0197...9f3c")


def test_endpoint_urls_are_public_read_only_and_keyless() -> None:
    urls = list(endpoint_urls_for_wallet(WATCHLIST["aviato"]))

    assert len(urls) == 4
    assert all(url.startswith("https://data-api.polymarket.com/") for url in urls)
    assert all("user=0x2a019dc0089ea8c6edbbafc8a7cc9ba77b4b6397" in url for url in urls)
    assert all("key" not in url.lower() for url in urls)
    assert all("secret" not in url.lower() for url in urls)


def test_client_builds_expected_url() -> None:
    client = PolymarketClient()

    url = client.endpoint_url("positions", {"user": WATCHLIST["aviato"].address})

    assert url == (
        "https://data-api.polymarket.com/positions?"
        "user=0x2a019dc0089ea8c6edbbafc8a7cc9ba77b4b6397"
    )

