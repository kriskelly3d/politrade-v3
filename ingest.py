"""
ingest.py — Async disclosure fetchers (httpx-powered)
======================================================

All network I/O is async with httpx. Two sources fetch concurrently:
  1. Capitol Trades HTML scraper  (both chambers, ticker-level)
  2. Senate Stock Watcher GitHub  (senate only, ticker-level fallback)

Verified working April 2026. Dead sources not included.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from typing import Optional

import httpx

from utils import setup_logger, load_config

logger = setup_logger()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/json,*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

FALSE_TICKERS = {
    "US","NA","ETF","FUND","TRUST","DEBT","INC","CORP","PLC","LP","LTD",
    "LLC","CO","GROUP","FIRST","GLOBAL","NORTH","SOUTH","EAST","WEST",
    "TRADR","VIPER","INDEX","MARKETS","FTSE","MSCI","BOND","EQUITY",
    "GROWTH","VALUE","LARGE","SMALL","MID","SHORT","LONG","HIGH","NEW",
}


def _disc_id(politician: str, ticker: str, tx_type: str, tx_date: str, amount: str) -> str:
    key = f"{politician}|{ticker}|{tx_type}|{tx_date}|{amount}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _parse_relative_date(text: str) -> datetime:
    text = text.lower().strip()
    now = datetime.now()
    if "yesterday" in text:
        return now - timedelta(days=1)
    if "today" in text or re.match(r"^\d+:\d+", text):
        return now
    m = re.search(r"(\d+)\s*days?\s*ago", text)
    if m:
        return now - timedelta(days=int(m.group(1)))
    m = re.search(r"(\d+)\s*hours?\s*ago", text)
    if m:
        return now - timedelta(hours=int(m.group(1)))
    for fmt in ("%d %b %Y", "%b %d %Y", "%d %B %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            pass
    return now


def _parse_absolute_date(text: str) -> Optional[datetime]:
    for fmt in ("%d %b %Y", "%b %d %Y", "%d %B %Y", "%Y-%m-%d", "%m/%d/%Y"):
        try:
            return datetime.strptime(text.strip(), fmt)
        except ValueError:
            pass
    return None


def _expand_amount(raw: str) -> str:
    mapping = {
        "1K-15K":    "$1,001 - $15,000",
        "15K-50K":   "$15,001 - $50,000",
        "50K-100K":  "$50,001 - $100,000",
        "100K-250K": "$100,001 - $250,000",
        "250K-500K": "$250,001 - $500,000",
        "500K-1M":   "$500,001 - $1,000,000",
        "1M-5M":     "$1,000,001 - $5,000,000",
        "5M+":       "Over $5,000,000",
    }
    raw = raw.replace("–", "-").replace("—", "-").strip()
    for short, full in mapping.items():
        if short.lower() in raw.lower():
            return full
    return raw


# ── Capitol Trades (primary) ──────────────────────────────────────────────────

async def _fetch_capitol_page(client: httpx.AsyncClient, page: int) -> list[dict]:
    try:
        r = await client.get(
            "https://www.capitoltrades.com/trades",
            params={"page": page, "pageSize": 96},
            headers={**HEADERS, "Referer": "https://www.capitoltrades.com/"},
            timeout=30,
        )
        r.raise_for_status()
    except Exception as e:
        logger.warning(f"Capitol Trades page {page} error: {e}")
        return []

    from bs4 import BeautifulSoup
    soup = BeautifulSoup(r.text, "lxml")
    table = soup.find("table")
    if not table:
        return []

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
    cutoff = datetime.now() - timedelta(days=7)
    results = []
    hit_old = False

    for row in rows:
        cells = row.find_all("td")
        if len(cells) < 8:
            continue
        try:
            name_tag = cells[0].find("a")
            name = (name_tag.get_text(strip=True) if name_tag
                    else cells[0].get_text(strip=True)).strip()
            cell_text = cells[0].get_text(" ", strip=True)
            chamber = "senate" if "Senate" in cell_text else "house"

            issuer_text = cells[1].get_text(" ", strip=True)
            m = re.search(r"\b([A-Z]{1,5}):US\b", issuer_text)
            ticker = m.group(1) if m else ""
            if not ticker or ticker in FALSE_TICKERS:
                continue

            pub_text = cells[2].get_text(" ", strip=True)
            pub_date = _parse_relative_date(pub_text)
            if pub_date < cutoff:
                hit_old = True
                continue

            traded_text = cells[3].get_text(" ", strip=True)
            trade_date = _parse_absolute_date(traded_text)
            trade_date_str = trade_date.strftime("%Y-%m-%d") if trade_date else traded_text

            tx_raw = cells[6].get_text(strip=True).lower()
            if "buy" in tx_raw or "purchase" in tx_raw:
                tx_type = "purchase"
            elif "sell" in tx_raw or "sale" in tx_raw:
                tx_type = "sale_full"
            else:
                tx_type = tx_raw

            amount_range = _expand_amount(cells[7].get_text(strip=True))

            results.append({
                "disclosure_id":    _disc_id(name, ticker, tx_type, trade_date_str, amount_range),
                "chamber":          chamber,
                "politician":       name,
                "ticker":           ticker,
                "asset_description": issuer_text,
                "transaction_type": tx_type,
                "transaction_date": trade_date_str,
                "disclosure_date":  pub_date.strftime("%Y-%m-%d"),
                "amount_range":     amount_range,
                "source":           "capitol_trades",
            })
        except Exception as e:
            logger.debug(f"Capitol Trades row error: {e}")

    if hit_old and len(rows) < 50:
        return results + [{"__stop": True}]
    return results


async def fetch_capitol_trades(days_back: int = 7) -> list[dict]:
    logger.info("Fetching Capitol Trades (async)...")
    results: list[dict] = []
    async with httpx.AsyncClient() as client:
        for page in range(1, 11):
            items = await _fetch_capitol_page(client, page)
            stop = any(i.get("__stop") for i in items)
            results.extend(i for i in items if not i.get("__stop"))
            if stop:
                break
            await asyncio.sleep(1.0)
    logger.info(f"Capitol Trades: {len(results)} disclosures.")
    return results


# ── Senate Stock Watcher GitHub (fallback) ────────────────────────────────────

async def fetch_senate_github(days_back: int = 7) -> list[dict]:
    logger.info("Fetching Senate GitHub data...")
    base = "https://raw.githubusercontent.com/timothycarambat/senate-stock-watcher-data/master/data"
    cutoff = datetime.now() - timedelta(days=days_back)
    now = datetime.now()
    months = [(now.year, now.month)]
    if now.month == 1:
        months.append((now.year - 1, 12))
    else:
        months.append((now.year, now.month - 1))

    results = []
    async with httpx.AsyncClient() as client:
        for year, month in months:
            url = f"{base}/{year}/{month:02d}/{year}-{month:02d}.json"
            try:
                r = await client.get(url, headers=HEADERS, timeout=20)
                if r.status_code != 200:
                    logger.warning(f"Senate GitHub {year}-{month:02d}: HTTP {r.status_code}")
                    continue
                monthly = r.json()
            except Exception as e:
                logger.warning(f"Senate GitHub {year}-{month:02d} error: {e}")
                continue

            for filing in monthly:
                try:
                    first = filing.get("first_name", "").strip()
                    last  = filing.get("last_name",  "").strip()
                    name  = f"{first} {last}".strip() or "Unknown"
                    filed = filing.get("date_recieved", "")
                    for tx in filing.get("transactions", []):
                        ticker = tx.get("ticker", "").strip().upper()
                        if not ticker or ticker == "--" or not re.match(r"^[A-Z]{1,5}$", ticker):
                            continue
                        tx_date = tx.get("transaction_date", filed)
                        try:
                            tx_dt = datetime.strptime(tx_date, "%m/%d/%Y")
                        except ValueError:
                            try:
                                tx_dt = datetime.strptime(tx_date, "%Y-%m-%d")
                            except ValueError:
                                tx_dt = datetime.now()
                        if tx_dt < cutoff:
                            continue
                        tx_type_raw = tx.get("type", "").lower()
                        if "purchase" in tx_type_raw or "buy" in tx_type_raw:
                            tx_type = "purchase"
                        elif "sale" in tx_type_raw or "sell" in tx_type_raw:
                            tx_type = "sale_full"
                        else:
                            tx_type = tx_type_raw
                        amount = tx.get("amount", "Unknown")
                        results.append({
                            "disclosure_id":    _disc_id(name, ticker, tx_type, tx_date, amount),
                            "chamber":          "senate",
                            "politician":       name,
                            "ticker":           ticker,
                            "asset_description": tx.get("asset_description", ""),
                            "transaction_type": tx_type,
                            "transaction_date": tx_date,
                            "disclosure_date":  filed,
                            "amount_range":     amount,
                            "source":           "senate_github",
                        })
                except Exception as e:
                    logger.debug(f"Senate GitHub filing parse error: {e}")

    logger.info(f"Senate GitHub: {len(results)} disclosures.")
    return results


# ── Orchestrator ──────────────────────────────────────────────────────────────

async def fetch_all(days_back: int = 7) -> list[dict]:
    """Fetch all sources concurrently, deduplicate, return actionable disclosures."""
    capitol_task  = asyncio.create_task(fetch_capitol_trades(days_back))
    senate_task   = asyncio.create_task(fetch_senate_github(days_back))
    capitol_data, senate_data = await asyncio.gather(capitol_task, senate_task)

    seen: set[str] = set()
    combined: list[dict] = []
    for item in capitol_data + senate_data:
        if item.get("ticker") in ("PENDING", "", None):
            continue
        did = item.get("disclosure_id", "")
        if did and did not in seen:
            seen.add(did)
            combined.append(item)

    logger.info(f"Total unique actionable disclosures: {len(combined)}")
    return combined
