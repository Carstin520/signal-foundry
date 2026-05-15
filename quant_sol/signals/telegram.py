from __future__ import annotations

import os
from typing import Optional

import requests

from .config import TELEGRAM_API_BASE_URL
from .models import SignalScore


def format_alert(signal: SignalScore) -> str:
    post = signal.source_posts[0] if signal.source_posts else {}
    price = signal.price_window
    flows = signal.wallet_flows[:3]
    flow_lines = "\n".join(
        f"- {flow.get('wallet')} {flow.get('side')} ${float(flow.get('notional') or 0):,.0f} at {flow.get('activity_ts')}"
        for flow in flows
    ) or "- none"
    text = str(post.get("text") or "")
    if len(text) > 220:
        text = text[:217] + "..."
    return (
        f"[FOMO Divergence] {signal.market_slug} score={signal.score}\n"
        f"Confidence: {signal.confidence} | Narrative direction: {price.get('narrative_direction')}\n"
        f"Market probability: {price.get('current_market_probability')}\n"
        f"Narrative velocity: {price.get('narrative_velocity')} | FOMO capacity: {price.get('fomo_capacity')}\n"
        f"Market move: 6h={price.get('market_move_6h')} 24h={price.get('market_move_24h')}\n"
        f"Deadline days: {price.get('deadline_days')} | Confirmation: {price.get('confirmation_status')}\n"
        f"Source: @{post.get('handle')} {post.get('created_at')}\n"
        f"{text}\n"
        f"{post.get('url')}\n\n"
        f"Execution: spread={price.get('spread')} liquidity={price.get('liquidity')} price_band={price.get('price_band')}\n"
        f"Wallet flow:\n{flow_lines}\n"
        f"Risk tags: {', '.join(signal.risk_tags) or 'none'}"
    )


class TelegramClient:
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None, base_url: str = TELEGRAM_API_BASE_URL) -> None:
        self.bot_token = bot_token or os.getenv("TELEGRAM_BOT_TOKEN")
        self.chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
        self.base_url = base_url.rstrip("/")

    def send_message(self, text: str, dry_run: bool = False) -> tuple[str, Optional[str]]:
        if dry_run:
            return "dry_run", None
        if not self.bot_token or not self.chat_id:
            return "missing_config", "TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID are required"
        response = requests.post(
            f"{self.base_url}/bot{self.bot_token}/sendMessage",
            json={"chat_id": self.chat_id, "text": text, "disable_web_page_preview": True},
            timeout=20,
        )
        if response.status_code >= 400:
            return "failed", response.text
        return "sent", None
