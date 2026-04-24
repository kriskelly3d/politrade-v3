"""utils.py — Shared utilities: config, logging, Discord alerts."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

_CONFIG: Optional[dict] = None
_CONFIG_PATH = Path(__file__).parent / "config.json"


def load_config() -> dict:
    global _CONFIG
    if _CONFIG is None:
        with open(_CONFIG_PATH) as f:
            _CONFIG = json.load(f)
    return _CONFIG


def setup_logger(name: str = "politrade") -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    fh = logging.FileHandler(log_dir / "politrade.log")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    return logger


logger = setup_logger()


def send_discord_alert(
    title: str,
    description: str = "",
    color: int = 0x3dbc7e,
    fields: Optional[list] = None,
) -> None:
    import urllib.request, urllib.error
    webhook = os.environ.get("DISCORD_WEBHOOK", "")
    if not webhook or "CHANGE_ME" in webhook:
        return
    payload = json.dumps({
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "fields": fields or [],
        }]
    }).encode()
    try:
        req = urllib.request.Request(
            webhook,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        logger.debug(f"Discord alert failed: {e}")


def parse_amount_range(raw: str) -> tuple[float, float, float]:
    """Return (low, high, mid) from a disclosure amount string."""
    import re
    raw = raw.replace(",", "").replace("$", "").strip()
    nums = re.findall(r"\d+(?:\.\d+)?", raw)
    if len(nums) >= 2:
        lo, hi = float(nums[0]), float(nums[1])
        return lo, hi, (lo + hi) / 2
    if len(nums) == 1:
        v = float(nums[0])
        return v, v, v
    return 0.0, 0.0, 0.0


def normalize_transaction_type(raw: str) -> Optional[str]:
    r = raw.lower().strip()
    if any(k in r for k in ("purchase", "buy")):
        return "BUY"
    if any(k in r for k in ("sale", "sell")):
        return "SELL"
    return None
