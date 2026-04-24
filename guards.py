"""
guards.py — PoliTrade 3.0 Pre-Trade Guard Suite
================================================

All 5 guards run sequentially in OrderManager.execute().
Guard 3 is Slippage 2.0: price move + bid-ask spread + volume check.

Guard order (do NOT change):
  1. Daily cap
  2. Position exists (sells only)
  3. Slippage 2.0
  4. Buying power (live mode only)
  5. Market hours (advisory, non-blocking)
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta
from typing import Optional

import pytz
import httpx

from utils import setup_logger, load_config

logger = setup_logger()
EASTERN = pytz.timezone("America/New_York")


# ── Price + market data helpers ───────────────────────────────────────────────

_price_cache: dict[str, dict] = {}
_hist_cache:  dict[str, float] = {}
_quote_cache: dict[str, dict] = {}
PRICE_TTL = 60


def _yf_quote(ticker: str) -> Optional[dict]:
    """Fetch real-time quote with bid/ask/volume from Yahoo Finance v10."""
    try:
        r = httpx.get(
            f"https://query1.finance.yahoo.com/v10/finance/quoteSummary/{ticker}",
            params={"modules": "price"},
            headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
            timeout=10,
        )
        if r.status_code == 200:
            pd = (r.json().get("quoteSummary", {})
                         .get("result", [{}])[0]
                         .get("price", {}))
            last = pd.get("regularMarketPrice", {}).get("raw", 0)
            bid  = pd.get("regularMarketDayLow",  {}).get("raw", last)
            ask  = pd.get("regularMarketDayHigh", {}).get("raw", last)
            vol  = pd.get("regularMarketVolume",  {}).get("raw", 0)
            # Prefer bid/ask from summary if available
            bid2 = pd.get("bid", {}).get("raw", bid) if isinstance(pd.get("bid"), dict) else bid
            ask2 = pd.get("ask", {}).get("raw", ask) if isinstance(pd.get("ask"), dict) else ask
            if last and last > 0:
                return {"last": last, "bid": bid2, "ask": ask2, "volume": vol}
    except Exception as e:
        logger.debug(f"YF quote error {ticker}: {e}")

    # Fallback: v8 chart endpoint
    try:
        r = httpx.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"interval": "1d", "range": "2d"},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if r.status_code == 200:
            result = r.json()["chart"]["result"][0]
            closes = result["indicators"]["quote"][0]["close"]
            vol    = result["indicators"]["quote"][0].get("volume", [0])
            price  = next((c for c in reversed(closes) if c), None)
            volume = next((v for v in reversed(vol) if v), 0)
            if price:
                return {"last": price, "bid": price * 0.999, "ask": price * 1.001, "volume": volume}
    except Exception as e:
        logger.debug(f"YF v8 fallback error {ticker}: {e}")

    return None


def get_quote(ticker: str) -> Optional[dict]:
    now = time.monotonic()
    c = _quote_cache.get(ticker)
    if c and (now - c["ts"]) < PRICE_TTL:
        return c["data"]
    q = _yf_quote(ticker)
    if q:
        _quote_cache[ticker] = {"data": q, "ts": now}
    return q


def get_price(ticker: str) -> Optional[float]:
    q = get_quote(ticker)
    return q["last"] if q else None


def get_historical_close(ticker: str, date_str: str) -> Optional[float]:
    key = f"{ticker}|{date_str}"
    if key in _hist_cache:
        return _hist_cache[key]
    try:
        dt    = datetime.strptime(date_str, "%Y-%m-%d")
        start = int((dt - timedelta(days=7)).timestamp())
        end   = int((dt + timedelta(days=2)).timestamp())
        r     = httpx.get(
            f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}",
            params={"interval": "1d", "period1": start, "period2": end},
            headers={"User-Agent": "Mozilla/5.0"},
            timeout=10,
        )
        if r.status_code == 200:
            result = r.json()["chart"]["result"][0]
            closes     = result["indicators"]["quote"][0]["close"]
            timestamps = result["timestamps"]
            target_ts  = dt.timestamp()
            best, best_diff = None, float("inf")
            for ts, cl in zip(timestamps, closes):
                if cl is None:
                    continue
                diff = abs(ts - target_ts)
                if diff < best_diff:
                    best, best_diff = cl, diff
            if best:
                _hist_cache[key] = best
                return best
    except Exception as e:
        logger.debug(f"Historical price error {ticker} {date_str}: {e}")
    return None


# ── Guard results ─────────────────────────────────────────────────────────────

class GuardResult:
    __slots__ = ("passed", "reason", "guard_name")

    def __init__(self, passed: bool, reason: str = "", guard_name: str = ""):
        self.passed     = passed
        self.reason     = reason
        self.guard_name = guard_name

    def __bool__(self) -> bool:
        return self.passed


OK = lambda name: GuardResult(True, "", name)
BLOCKED = lambda name, reason: GuardResult(False, reason, name)


# ── Guard 1: Daily order cap ──────────────────────────────────────────────────

def guard_daily_cap(mode: str, cfg: dict) -> GuardResult:
    import db
    cap   = cfg.get("engine", {}).get("max_daily_orders", 10)
    today = db.count_orders_today(mode)
    if today >= cap:
        return BLOCKED("daily_cap", f"Daily cap reached ({today}/{cap} orders in {mode} mode)")
    return OK("daily_cap")


# ── Guard 2: Position exists (sells only) ─────────────────────────────────────

def guard_position_exists(pilot_name: str, ticker: str, action: str) -> GuardResult:
    if action != "SELL":
        return OK("position_exists")
    import db
    pos = db.get_position(pilot_name, ticker)
    if not pos:
        return BLOCKED(
            "position_exists",
            f"No open position for {pilot_name}/{ticker} — cannot mirror sell"
        )
    return OK("position_exists")


# ── Guard 3: Slippage 2.0 ─────────────────────────────────────────────────────

def guard_slippage_v2(
    disclosure_id: str, pilot_name: str,
    ticker: str, action: str, disclosure_date: str,
    cfg: dict,
) -> tuple[GuardResult, float]:
    """
    Slippage 2.0: three sub-checks:
      a) Price move since politician's trade date (original guard)
      b) Bid-ask spread % (liquidity proxy)
      c) Daily volume (retail viability proxy)

    Returns (GuardResult, current_price).
    """
    g_cfg     = cfg.get("guards", {})
    threshold = g_cfg.get("slippage_pct", 10.0)
    v2_cfg    = g_cfg.get("slippage_v2", {})
    v2_on     = v2_cfg.get("enabled", True)

    quote = get_quote(ticker)
    if not quote:
        logger.warning(f"No quote for {ticker} — allowing through (cannot verify)")
        return OK("slippage"), 1.0

    current_price = quote["last"]
    bid   = quote.get("bid", current_price)
    ask   = quote.get("ask", current_price)
    vol   = int(quote.get("volume", 0))
    mid   = (bid + ask) / 2 if bid and ask else current_price
    spread_pct = ((ask - bid) / mid * 100) if mid > 0 else 0.0

    # --- 3a: Price move vs politician's trade date ---
    move_pct = 0.0
    hist = get_historical_close(ticker, disclosure_date) if disclosure_date else None
    if hist and hist > 0:
        move_pct = ((current_price - hist) / hist) * 100
        price_blocked = (
            (action == "BUY"  and move_pct >  threshold) or
            (action == "SELL" and move_pct < -threshold)
        )
        if price_blocked:
            reason = (
                f"{ticker} moved {move_pct:+.1f}% since pol traded on {disclosure_date} "
                f"(threshold={threshold:.0f}%, then=${hist:.2f}, now=${current_price:.2f})"
            )
            import db
            db.log_slippage_event(
                disclosure_id, pilot_name, ticker, action, disclosure_date,
                hist, current_price, move_pct, spread_pct, vol, threshold, reason
            )
            return BLOCKED("slippage_price", reason), current_price

    # --- 3b: Spread check ---
    max_spread    = v2_cfg.get("max_spread_pct", 0.5)
    soft_spread   = v2_cfg.get("soft_block_spread_pct", 1.0)
    if v2_on and spread_pct > soft_spread:
        reason = (
            f"{ticker} spread is {spread_pct:.2f}% (bid=${bid:.2f} ask=${ask:.2f}) "
            f"— retail fill quality compromised (threshold={soft_spread:.1f}%)"
        )
        import db
        db.log_slippage_event(
            disclosure_id, pilot_name, ticker, action, disclosure_date,
            hist or current_price, current_price, move_pct, spread_pct, vol, threshold, reason
        )
        return BLOCKED("slippage_spread", reason), current_price
    elif v2_on and spread_pct > max_spread:
        logger.warning(f"Wide spread on {ticker}: {spread_pct:.2f}% — proceeding with caution")

    # --- 3c: Volume check ---
    min_vol = v2_cfg.get("min_volume", 100_000)
    if v2_on and 0 < vol < min_vol:
        logger.warning(
            f"Low volume on {ticker}: {vol:,} shares "
            f"(min={min_vol:,}) — trade may have poor fill"
        )

    logger.debug(
        f"Slippage 2.0 OK: {ticker} move={move_pct:+.1f}% "
        f"spread={spread_pct:.3f}% vol={vol:,}"
    )
    return OK("slippage"), current_price


# ── Guard 4: Buying power ─────────────────────────────────────────────────────

def guard_buying_power(
    action: str, shares: float, price: float, buying_power: float, mode: str, cfg: dict
) -> GuardResult:
    if action != "BUY" or mode == "paper":
        return OK("buying_power")
    reserve  = cfg.get("engine", {}).get("min_buying_power_usd", 2000)
    required = shares * price
    if buying_power < (required + reserve):
        return BLOCKED(
            "buying_power",
            f"Insufficient buying power: need ${required:,.0f} + ${reserve:,.0f} reserve, "
            f"have ${buying_power:,.0f}"
        )
    return OK("buying_power")


# ── Guard 5: Market hours (advisory only) ─────────────────────────────────────

def guard_market_hours() -> GuardResult:
    now = datetime.now(EASTERN)
    if now.weekday() >= 5:
        logger.info(f"Market hours advisory: {now.strftime('%A')} — market closed, order queued")
    elif not (9 <= now.hour < 16 or (now.hour == 9 and now.minute >= 30)):
        logger.info(f"Market hours advisory: {now.strftime('%H:%M ET')} — outside RTH")
    return OK("market_hours")  # Always pass — orders route correctly via Schwab
