"""
broker.py — Schwab API Wrapper + Remote Auth Support
=====================================================

Key design choices vs v2:
  - Token is stored in BOTH local file AND Supabase token_store
    so GitHub Actions and the Streamlit dashboard share the same token.
  - Remote auth flow: Streamlit calls start_auth_url() → user pastes
    callback URL → complete_auth(url) → token persisted everywhere.
  - In paper mode all Schwab calls are skipped; Yahoo Finance prices used.
  - Token expiry warning fires at refresh_token_warn_days before expiry.
"""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from utils import setup_logger, load_config, send_discord_alert
import guards

logger = setup_logger()

try:
    import schwab
    from schwab import auth as schwab_auth
    SCHWAB_AVAILABLE = True
except ImportError:
    SCHWAB_AVAILABLE = False
    logger.warning("schwab-py not installed — run: pip install schwab-py")


# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class OrderResult:
    success:        bool
    order_id:       Optional[str] = None
    fill_price:     float         = 0.0
    shares:         int           = 0
    status:         str           = "UNKNOWN"
    error:          Optional[str] = None
    mode:           str           = "paper"
    current_price:  float         = 0.0

    def __str__(self) -> str:
        if self.success:
            return f"[{self.mode.upper()}] OK {self.shares}sh @ ${self.fill_price:.2f}"
        return f"[{self.mode.upper()}] FAIL: {self.error}"


@dataclass
class AccountState:
    buying_power: float = 0.0
    cash_balance: float = 0.0
    total_value:  float = 0.0
    positions:    dict  = field(default_factory=dict)
    fetched_at:   float = 0.0

    def age(self) -> float:
        return time.monotonic() - self.fetched_at if self.fetched_at else 9999.0


_PAPER_ACCOUNT = AccountState(
    buying_power=50_000, cash_balance=50_000, total_value=50_000,
    fetched_at=time.monotonic()
)


# ── SchwabBroker ──────────────────────────────────────────────────────────────

class SchwabBroker:

    TOKEN_FILE   = Path("data/schwab_token.json")
    CACHE_TTL    = 60

    def __init__(self, mode: str = "paper"):
        self.mode          = mode
        self.cfg           = load_config()
        self._client       = None
        self._account_hash: Optional[str]          = None
        self._acct_cache:   Optional[AccountState] = None

    # ── Token persistence (local + Supabase) ──────────────────────────────

    def _save_token_everywhere(self, token_json: str) -> None:
        """Persist token to local file AND Supabase token_store."""
        self.TOKEN_FILE.parent.mkdir(exist_ok=True)
        self.TOKEN_FILE.write_text(token_json)
        try:
            import db
            db.store_token("schwab_token", token_json)
            logger.info("Token saved to local file + Supabase.")
        except Exception as e:
            logger.warning(f"Supabase token save failed (local file OK): {e}")

    def _load_token_from_supabase(self) -> bool:
        """Pull token from Supabase into local file if local file is missing."""
        if self.TOKEN_FILE.exists():
            return True
        try:
            import db
            token_json = db.get_token("schwab_token")
            if token_json:
                self.TOKEN_FILE.parent.mkdir(exist_ok=True)
                self.TOKEN_FILE.write_text(token_json)
                logger.info("Token restored from Supabase to local file.")
                return True
        except Exception as e:
            logger.warning(f"Supabase token load failed: {e}")
        return False

    def _check_token_expiry(self) -> None:
        """Warn via Discord if refresh token expires within warn_days."""
        try:
            data = json.loads(self.TOKEN_FILE.read_text()) if self.TOKEN_FILE.exists() else {}
            token = data.get("token", data)
            created = data.get("creation_timestamp", 0)
            if not created:
                return
            refresh_ttl = self.cfg.get("schwab", {}).get("refresh_token_ttl_seconds", 604800)
            warn_days   = self.cfg.get("schwab", {}).get("refresh_token_warn_days", 2)
            expires_ts  = created + refresh_ttl
            secs_left   = expires_ts - time.time()
            if secs_left < warn_days * 86400:
                days_left = max(0, secs_left / 86400)
                send_discord_alert(
                    title=f"⚠️ Schwab Token Expires in {days_left:.1f} Days",
                    description=(
                        "Go to **Command Center → Auth Refresh** in the Streamlit dashboard "
                        "or run `python engine.py --auth` to renew before it expires."
                    ),
                    color=0xFF6600,
                )
                logger.warning(f"Schwab refresh token expires in {days_left:.1f} days!")
        except Exception:
            pass

    # ── Authentication ────────────────────────────────────────────────────

    def authenticate(self) -> bool:
        if self.mode == "paper":
            logger.info("Paper mode — Schwab auth skipped.")
            return True
        if not SCHWAB_AVAILABLE:
            logger.error("schwab-py not installed.")
            return False

        sc = self.cfg.get("schwab", {})
        key    = os.environ.get("SCHWAB_APP_KEY",    sc.get("app_key", ""))
        secret = os.environ.get("SCHWAB_APP_SECRET", sc.get("app_secret", ""))
        cb_url = sc.get("redirect_uri", "https://127.0.0.1")

        self._load_token_from_supabase()

        try:
            if self.TOKEN_FILE.exists():
                logger.info("Loading Schwab token from file...")
                self._client = schwab_auth.client_from_token_file(
                    token_path=str(self.TOKEN_FILE),
                    api_key=key, app_secret=secret,
                )
                logger.info("Token loaded — schwab-py will auto-refresh.")
            else:
                logger.info("No token found — starting manual OAuth flow...")
                self._client = schwab_auth.client_from_manual_flow(
                    api_key=key, app_secret=secret,
                    callback_url=cb_url,
                    token_path=str(self.TOKEN_FILE),
                )

            self._account_hash = self._get_account_hash()
            if self._account_hash:
                hash_prefix = self._account_hash[:8]
                logger.info(f"Schwab authenticated OK  hash={hash_prefix}...")
                self._check_token_expiry()
                return True

            logger.error("Auth succeeded but no account hash found.")
            return False

        except KeyboardInterrupt:
            logger.info("Auth cancelled.")
            return False
        except Exception as e:
            logger.error(f"Schwab auth failed: {e}")
            return False

    def start_auth_url(self) -> str:
        """
        Remote auth step 1: return the OAuth URL for the user to open in browser.
        Called from the Streamlit Command Center.
        """
        if not SCHWAB_AVAILABLE:
            return ""
        sc = self.cfg.get("schwab", {})
        key    = os.environ.get("SCHWAB_APP_KEY",    sc.get("app_key", ""))
        secret = os.environ.get("SCHWAB_APP_SECRET", sc.get("app_secret", ""))
        cb_url = sc.get("redirect_uri", "https://127.0.0.1")
        try:
            from schwab.auth import _client_from_access_functions as _
            import urllib.parse, secrets as _secrets
            state = _secrets.token_urlsafe(16)
            params = {
                "response_type": "code",
                "client_id":     key,
                "redirect_uri":  cb_url,
                "scope":         "api",
                "state":         state,
            }
            url = "https://api.schwabapi.com/v1/oauth/authorize?" + urllib.parse.urlencode(params)
            return url
        except Exception as e:
            logger.error(f"start_auth_url failed: {e}")
            return ""

    def complete_auth(self, redirected_url: str) -> bool:
        """
        Remote auth step 2: given the URL the browser was redirected to,
        exchange the code for tokens and persist everywhere.
        Called from the Streamlit Command Center after user pastes the URL.
        """
        if not SCHWAB_AVAILABLE:
            return False
        sc = self.cfg.get("schwab", {})
        key    = os.environ.get("SCHWAB_APP_KEY",    sc.get("app_key", ""))
        secret = os.environ.get("SCHWAB_APP_SECRET", sc.get("app_secret", ""))
        cb_url = sc.get("redirect_uri", "https://127.0.0.1")
        try:
            # schwab-py's manual flow writes to token_path on success
            token_path = str(self.TOKEN_FILE)
            self.TOKEN_FILE.parent.mkdir(exist_ok=True)

            import urllib.parse
            parsed = urllib.parse.urlparse(redirected_url)
            params = urllib.parse.parse_qs(parsed.query)
            code = params.get("code", [None])[0]
            if not code:
                logger.error("No 'code' param in redirected URL.")
                return False

            import httpx as _httpx
            r = _httpx.post(
                "https://api.schwabapi.com/v1/oauth/token",
                data={
                    "grant_type":   "authorization_code",
                    "code":         code,
                    "redirect_uri": cb_url,
                },
                auth=(key, secret),
                timeout=15,
            )
            if r.status_code != 200:
                logger.error(f"Token exchange failed: {r.status_code} {r.text[:200]}")
                return False

            token_data = r.json()
            payload = {
                "creation_timestamp": int(time.time()),
                "token": token_data,
            }
            token_json = json.dumps(payload, indent=2)
            self._save_token_everywhere(token_json)
            logger.info("Remote auth complete — token saved.")
            return True

        except Exception as e:
            logger.error(f"complete_auth failed: {e}")
            return False

    def is_authenticated(self) -> bool:
        return self.mode == "paper" or (
            self._client is not None and self._account_hash is not None
        )

    # ── Account ────────────────────────────────────────────────────────────

    def _get_account_hash(self) -> Optional[str]:
        try:
            r = self._client.get_account_numbers()
            if r.status_code == 200:
                accounts = r.json()
                return accounts[0].get("hashValue") if accounts else None
        except Exception as e:
            logger.error(f"get_account_numbers error: {e}")
        return None

    def get_account(self, force: bool = False) -> AccountState:
        if self.mode == "paper":
            return _PAPER_ACCOUNT
        if self._acct_cache and self._acct_cache.age() < self.CACHE_TTL and not force:
            return self._acct_cache
        if not self.is_authenticated():
            return self._acct_cache or AccountState()
        try:
            r = self._client.get_account(
                self._account_hash,
                fields=self._client.Account.Fields.POSITIONS,
            )
            if r.status_code != 200:
                return self._acct_cache or AccountState()
            data     = r.json()
            acct     = data.get("securitiesAccount", {})
            balances = acct.get("currentBalances", {})
            positions = {
                pos["instrument"]["symbol"]: {
                    "quantity": pos.get("longQuantity", 0),
                    "avg_cost": pos.get("averagePrice", 0),
                }
                for pos in acct.get("positions", [])
                if pos.get("instrument", {}).get("symbol")
            }
            state = AccountState(
                buying_power=balances.get("buyingPower",      0),
                cash_balance=balances.get("cashBalance",      0),
                total_value =balances.get("liquidationValue", 0),
                positions   =positions,
                fetched_at  =time.monotonic(),
            )
            self._acct_cache = state
            return state
        except Exception as e:
            logger.error(f"get_account error: {e}")
            return self._acct_cache or AccountState()

    # ── Order placement ───────────────────────────────────────────────────

    def place_market(self, ticker: str, action: str, shares: int) -> OrderResult:
        if self.mode == "paper":
            price = guards.get_price(ticker) or 1.0
            logger.info(f"[PAPER] MARKET {action} {shares} {ticker} @ ~${price:.2f}")
            return OrderResult(
                success=True, shares=shares, fill_price=price,
                status="PAPER_FILLED", mode="paper", current_price=price
            )
        if not SCHWAB_AVAILABLE or not self.is_authenticated():
            return OrderResult(success=False, error="Not authenticated", mode="live")
        try:
            spec = (schwab.orders.equities.equity_buy_market(ticker, shares)
                    if action == "BUY"
                    else schwab.orders.equities.equity_sell_market(ticker, shares))
            r = self._client.place_order(self._account_hash, spec)
            if r.status_code in (200, 201):
                oid   = r.headers.get("Location", "").split("/")[-1]
                price = guards.get_price(ticker) or 0.0
                return OrderResult(
                    success=True, order_id=oid, shares=shares,
                    fill_price=price, status="SUBMITTED", mode="live", current_price=price
                )
            return OrderResult(success=False, error=f"HTTP {r.status_code}", mode="live")
        except Exception as e:
            return OrderResult(success=False, error=str(e), mode="live")

    def place_limit(self, ticker: str, action: str, shares: int, limit_price: float) -> OrderResult:
        if self.mode == "paper":
            logger.info(f"[PAPER] LIMIT {action} {shares} {ticker} @ ${limit_price:.2f}")
            return OrderResult(
                success=True, shares=shares, fill_price=limit_price,
                status="PAPER_FILLED", mode="paper", current_price=limit_price
            )
        if not SCHWAB_AVAILABLE or not self.is_authenticated():
            return OrderResult(success=False, error="Not authenticated", mode="live")
        try:
            spec = (schwab.orders.equities.equity_buy_limit(ticker, shares, limit_price)
                    if action == "BUY"
                    else schwab.orders.equities.equity_sell_limit(ticker, shares, limit_price))
            r = self._client.place_order(self._account_hash, spec)
            if r.status_code in (200, 201):
                oid = r.headers.get("Location", "").split("/")[-1]
                return OrderResult(
                    success=True, order_id=oid, shares=shares,
                    fill_price=limit_price, status="SUBMITTED", mode="live",
                    current_price=limit_price,
                )
            return OrderResult(success=False, error=f"HTTP {r.status_code}", mode="live")
        except Exception as e:
            return OrderResult(success=False, error=str(e), mode="live")


# ── OrderManager — sizing + guard orchestration + logging ─────────────────────

class OrderManager:

    def __init__(self, broker: SchwabBroker):
        self.broker = broker
        self.cfg    = load_config()

    def _size(
        self, ticker: str, action: str, pilot_name: str,
        current_price: float, pilot_cfg: dict,
    ) -> int:
        if action == "SELL":
            import db
            pos = db.get_position(pilot_name, ticker)
            if not pos:
                return 0
            held = int(pos.get("shares", 0))
            return held  # Always sell full position (mirror politician's exit)

        alloc  = pilot_cfg.get("allocation_pct", 0.03)
        bp     = self.broker.get_account().buying_power
        target = bp * alloc
        if current_price <= 0:
            return 0
        shares = math.floor(target / current_price)
        logger.info(
            f"Sizing {ticker}: ${bp:,.0f} × {alloc*100:.1f}% "
            f"= ${target:,.0f} ÷ ${current_price:.2f} = {shares} shares"
        )
        return max(shares, 1)

    def execute(
        self,
        disclosure_id: str,
        pilot_name:    str,
        ticker:        str,
        action:        str,
        tx_subtype:    str,
        disclosure_date: str,
        pilot_cfg:     dict,
        mode:          str,
    ) -> OrderResult:
        import db
        from utils import send_discord_alert

        # Guard 1: daily cap
        g = guards.guard_daily_cap(mode, self.cfg)
        if not g:
            return OrderResult(success=False, error=g.reason, mode=mode)

        # Guard 2: position exists
        g = guards.guard_position_exists(pilot_name, ticker, action)
        if not g:
            return OrderResult(success=False, error=g.reason, mode=mode)

        # Guard 3: slippage 2.0 (also fetches current price)
        g, current_price = guards.guard_slippage_v2(
            disclosure_id, pilot_name, ticker, action, disclosure_date, self.cfg
        )
        if not g:
            return OrderResult(success=False, error=g.reason, mode=mode)

        # Size
        shares = self._size(ticker, action, pilot_name, current_price, pilot_cfg)
        if shares <= 0:
            return OrderResult(
                success=False,
                error=f"Position sizing returned 0 shares for {ticker}",
                mode=mode,
            )

        # Guard 4: buying power (live only)
        g = guards.guard_buying_power(
            action, shares, current_price,
            self.broker.get_account().buying_power, mode, self.cfg
        )
        if not g:
            return OrderResult(success=False, error=g.reason, mode=mode)

        # Guard 5: market hours (advisory)
        guards.guard_market_hours()

        # Determine order type + limit price
        order_type = pilot_cfg.get("order_type", "MARKET")
        limit_price: Optional[float] = None
        if order_type == "LIMIT":
            offset      = pilot_cfg.get("limit_offset_pct", 0.005)
            limit_price = round(
                current_price * (1 + offset if action == "BUY" else 1 - offset), 2
            )

        # Place
        result = (
            self.broker.place_limit(ticker, action, shares, limit_price)
            if limit_price
            else self.broker.place_market(ticker, action, shares)
        )
        if result.fill_price == 0:
            result.fill_price = limit_price or current_price
        result.current_price = current_price

        if result.success:
            # Log order
            db.log_order(
                disclosure_id=disclosure_id,
                pilot_name=pilot_name,
                ticker=ticker,
                action=action,
                shares=shares,
                order_type=order_type,
                mode=mode,
                fill_price=result.fill_price,
                schwab_order_id=result.order_id,
                status=result.status,
            )
            # Update positions
            if action == "BUY":
                db.open_position(
                    pilot_name=pilot_name,
                    ticker=ticker,
                    shares=shares,
                    entry_price=result.fill_price,
                    disclosure_date=disclosure_date,
                    schwab_order_id=result.order_id or "",
                    mode=mode,
                )
            else:
                partial = "partial" in tx_subtype.lower()
                db.close_position(pilot_name, ticker, result.fill_price, partial=partial)

            db.record_daily_order(ticker, pilot_name, mode)

            # Discord alert
            color = 0x3dbc7e if action == "BUY" else 0xe05c5c
            send_discord_alert(
                title=f"{'🟢 BUY' if action=='BUY' else '🔴 SELL'} {ticker} [{mode.upper()}]",
                description=f"**{pilot_name}** trade mirrored",
                color=color,
                fields=[
                    {"name": "Ticker",  "value": ticker,                    "inline": True},
                    {"name": "Shares",  "value": str(shares),               "inline": True},
                    {"name": "Price",   "value": f"${result.fill_price:.2f}","inline": True},
                    {"name": "Order",   "value": result.order_id or "paper","inline": True},
                    {"name": "Mode",    "value": mode.upper(),              "inline": True},
                ],
            )

        return result
