from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional


DATA_ROOT = Path("data")
DB_PATH = DATA_ROOT / "quant_sol.duckdb"
RAW_ROOT = DATA_ROOT / "raw" / "polymarket"
REPORT_ROOT = DATA_ROOT / "reports"

DATA_API_BASE_URL = "https://data-api.polymarket.com"
GAMMA_API_BASE_URL = "https://gamma-api.polymarket.com"


@dataclass(frozen=True)
class WalletConfig:
    label: str
    address: Optional[str]
    status: str
    notes: str

    @property
    def is_resolved(self) -> bool:
        return self.status == "resolved" and is_valid_address(self.address)


def is_valid_address(address: Optional[str]) -> bool:
    if address is None:
        return False
    if len(address) != 42 or not address.startswith("0x"):
        return False
    try:
        int(address[2:], 16)
    except ValueError:
        return False
    return True


WATCHLIST: Dict[str, WalletConfig] = {
    "aviato": WalletConfig(
        label="aviato",
        address="0x2a019dc0089ea8c6edbbafc8a7cc9ba77b4b6397",
        status="resolved",
        notes="X/Polyrating high win-rate candidate; needs independent metric reconciliation.",
    ),
    "Annica": WalletConfig(
        label="Annica",
        address="0x689ae12e11aa489adb3605afd8f39040ff52779e",
        status="resolved",
        notes="Musk/tweet-related trader candidate; analyze realized vs unrealized PnL separately.",
    ),
    "reachingthesky": WalletConfig(
        label="reachingthesky",
        address="0xefbc5fec8d7b0acdc8911bdd9a98d6964308f9a2",
        status="resolved",
        notes="Soccer whale candidate; useful for asymmetric sizing research.",
    ),
    "GCottrell93": WalletConfig(
        label="GCottrell93",
        address="0x94a428cfa4f84b264e01f70d93d02bc96cb36356",
        status="resolved",
        notes="Political/geopolitical bettor candidate with large PnL and tail-risk examples.",
    ),
    "majorexploiter": WalletConfig(
        label="majorexploiter",
        address=None,
        status="unresolved_wallet",
        notes="Only profile/partial address found: @majorexploiter, 0x0197...9f3c.",
    ),
}

