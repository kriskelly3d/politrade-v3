"""
engine.py — PoliTrade 3.0 Async State Machine
==============================================

State machine: IDLE → SCAN → ANALYZE → RISK_CHECK → EXECUTE → IDLE

CLI:
  python engine.py              # Run one full cycle (GitHub Actions)
  python engine.py --loop       # Continuous loop (local dev)
  python engine.py --test       # Fetch & display disclosures, no orders
  python engine.py --auth       # Schwab OAuth (manual terminal flow)
  python engine.py --status     # Print portfolio status
  python engine.py --rebalance  # Run drift report and exit
"""

from __future__ import annotations

import argparse
import asyncio
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Optional

import pytz

import db
import ingest
from broker import SchwabBroker, OrderManager
from rebalancer import Rebalancer
from utils import load_config, setup_logger, normalize_transaction_type, parse_amount_range, send_discord_alert

logger = setup_logger()
EASTERN = pytz.timezone("America/New_York")


# ── State Machine ─────────────────────────────────────────────────────────────

class State(Enum):
    IDLE       = auto()
    SCAN       = auto()   # Fetch disclosures from all sources
    ANALYZE    = auto()   # Filter & match against pilots
    RISK_CHECK = auto()   # Guards run here (per-disclosure)
    EXECUTE    = auto()   # Place orders
    ERROR      = auto()


@dataclass
class CycleStats:
    state:              State = State.IDLE
    disclosures_fetched: int  = 0
    disclosures_new:     int  = 0
    orders_placed:       int  = 0
    orders_blocked:      int  = 0
    rebalance_exits:     int  = 0
    errors:              list = field(default_factory=list)
    started_at:          Optional[datetime] = None
    finished_at:         Optional[datetime] = None

    @property
    def duration_ms(self) -> int:
        if self.started_at and self.finished_at:
            return int((self.finished_at - self.started_at).total_seconds() * 1000)
        return 0


# ── Pilot matching ────────────────────────────────────────────────────────────

def _match_pilot(politician: str, pilots: dict) -> Optional[tuple[str, dict]]:
    norm = politician.lower().strip()
    last = norm.split()[-1] if norm else ""
    for name, cfg in pilots.items():
        pnorm = name.lower()
        plast = pnorm.split()[-1]
        if norm == pnorm or norm in pnorm or pnorm in norm or (last and last == plast):
            return name, cfg
    return None


# ── Main engine ───────────────────────────────────────────────────────────────

class Engine:

    def __init__(self):
        self.cfg          = load_config()
        self.engine_cfg   = self.cfg.get("engine", {})
        self.pilots       = {
            name: pcfg
            for name, pcfg in self.cfg.get("pilots", {}).items()
            if pcfg.get("enabled", True)
        }
        self.default_mode = self.engine_cfg.get("default_mode", "paper")
        self.broker       = SchwabBroker(mode=self.default_mode)
        self.order_mgr    = OrderManager(self.broker)
        self.rebalancer   = Rebalancer(self.order_mgr, mode=self.default_mode)
        self.stats        = CycleStats()

    async def run_cycle(self) -> CycleStats:
        self.stats = CycleStats(started_at=datetime.now(EASTERN))
        t0 = time.monotonic()

        # ── SCAN ──────────────────────────────────────────────────────────
        self.stats.state = State.SCAN
        logger.info(f"[SCAN] Fetching disclosures (mode={self.default_mode})...")
        try:
            days_back = self.engine_cfg.get("days_back", 7)
            all_disclosures = await ingest.fetch_all(days_back)
            self.stats.disclosures_fetched = len(all_disclosures)
        except Exception as e:
            logger.error(f"[SCAN] Ingestion failed: {e}")
            self.stats.errors.append(str(e))
            self.stats.state = State.ERROR
            return self.stats

        # ── ANALYZE ───────────────────────────────────────────────────────
        self.stats.state = State.ANALYZE
        new_disclosures: list[dict] = []
        sell_disclosures: list[dict] = []

        for disc in all_disclosures:
            did = disc.get("disclosure_id", "")
            if db.is_processed(did):
                continue

            # Store in DB immediately so parallel runs don't double-process
            db.upsert_disclosure(disc, action="pending")
            self.stats.disclosures_new += 1

            politician = disc.get("politician", "")
            match = _match_pilot(politician, self.pilots)
            if not match:
                db.mark_processed(did, "filtered_no_pilot")
                continue

            pilot_name, pilot_cfg = match
            action = normalize_transaction_type(disc.get("transaction_type", ""))
            if not action:
                db.mark_processed(did, "filtered_bad_tx")
                continue

            mode = pilot_cfg.get("mode", self.default_mode)

            if action == "SELL" and not self.engine_cfg.get("mirror_sells", True):
                db.mark_processed(did, "skipped_sell_mirror_off")
                continue

            _, _, amount_mid = parse_amount_range(disc.get("amount_range", ""))
            enriched = {**disc, "pilot_name": pilot_name, "action": action,
                        "amount_mid": amount_mid, "mode": mode}

            new_disclosures.append(enriched)
            if action == "SELL":
                sell_disclosures.append(enriched)

        logger.info(
            f"[ANALYZE] {len(new_disclosures)} new actionable disclosures "
            f"({len(sell_disclosures)} SELLs)"
        )

        # ── Rebalancer — fast-path for sells ──────────────────────────────
        if sell_disclosures:
            logger.info(f"[REBALANCE] Processing {len(sell_disclosures)} sell disclosures...")
            rebalance_actions = self.rebalancer.run(sell_disclosures)
            self.stats.rebalance_exits = sum(1 for a in rebalance_actions if a.executed)
            # Mark rebalanced disclosures as processed
            for disc in sell_disclosures:
                if any(a.ticker == disc["ticker"] and a.executed for a in rebalance_actions):
                    db.mark_processed(disc["disclosure_id"], "rebalanced")

        # ── RISK_CHECK + EXECUTE — buys and unhandled sells ───────────────
        self.stats.state = State.RISK_CHECK
        remaining = [d for d in new_disclosures
                     if not db.is_processed(d["disclosure_id"])]

        for disc in remaining:
            self.stats.state = State.EXECUTE
            pilot_name  = disc["pilot_name"]
            ticker      = disc["ticker"]
            action      = disc["action"]
            did         = disc["disclosure_id"]
            mode        = disc["mode"]
            pilot_cfg   = self.pilots.get(pilot_name, {})

            logger.info(
                f"[EXECUTE] {pilot_name} {action} {ticker} [{mode.upper()}]"
            )

            result = self.order_mgr.execute(
                disclosure_id   = did,
                pilot_name      = pilot_name,
                ticker          = ticker,
                action          = action,
                tx_subtype      = disc.get("transaction_type", ""),
                disclosure_date = disc.get("transaction_date", ""),
                pilot_cfg       = pilot_cfg,
                mode            = mode,
            )

            if result.success:
                self.stats.orders_placed += 1
                db.mark_processed(did, "traded")
            else:
                self.stats.orders_blocked += 1
                reason = result.error or "unknown"
                if "cap" in reason.lower():
                    action_str = "skipped_cap"
                elif "slippage" in reason.lower():
                    action_str = "blocked_slippage"
                elif "power" in reason.lower():
                    action_str = "blocked_power"
                else:
                    action_str = "blocked_other"
                db.mark_processed(did, action_str)

        # ── Finalize ──────────────────────────────────────────────────────
        self.stats.finished_at = datetime.now(EASTERN)
        self.stats.state       = State.IDLE
        dur_ms = int((time.monotonic() - t0) * 1000)

        logger.info(
            f"Cycle done in {dur_ms}ms | "
            f"fetched={self.stats.disclosures_fetched} "
            f"new={self.stats.disclosures_new} "
            f"placed={self.stats.orders_placed} "
            f"blocked={self.stats.orders_blocked} "
            f"exits={self.stats.rebalance_exits}"
        )

        try:
            db.log_engine_run(
                state_reached       = "IDLE",
                disclosures_fetched = self.stats.disclosures_fetched,
                disclosures_new     = self.stats.disclosures_new,
                orders_placed       = self.stats.orders_placed,
                orders_blocked      = self.stats.orders_blocked,
                error_message       = "; ".join(self.stats.errors) or None,
                duration_ms         = dur_ms,
            )
        except Exception as e:
            logger.warning(f"Failed to log engine run: {e}")

        return self.stats


# ── CLI commands ──────────────────────────────────────────────────────────────

async def cmd_test():
    from rich.console import Console
    from rich.table import Table
    from rich import box
    console = Console()
    console.rule("[bold yellow]TEST MODE — no orders placed[/bold yellow]")
    disclosures = await ingest.fetch_all(days_back=7)
    cfg        = load_config()
    pilot_names = [n.lower() for n in cfg.get("pilots", {}).keys()]

    t = Table(title=f"Live Disclosures ({len(disclosures)} total)", box=box.ROUNDED)
    t.add_column("Pilot?", width=7)
    t.add_column("Politician", style="yellow", no_wrap=True)
    t.add_column("Ticker", style="cyan bold", width=8)
    t.add_column("Action", width=10)
    t.add_column("Amount", style="dim")
    t.add_column("Date", width=12)
    t.add_column("Source", width=16)

    for d in disclosures:
        pol    = d.get("politician", "")
        ticker = d.get("ticker", "")
        tx     = d.get("transaction_type", "")
        amt    = d.get("amount_range", "")
        date   = d.get("disclosure_date", "")[:10]
        src    = d.get("source", "")
        pnorm  = pol.lower()
        is_p   = any(pp in pnorm or pnorm in pp for pp in pilot_names)
        ac     = "green" if "purchase" in tx.lower() or "buy" in tx.lower() else "red"
        t.add_row(
            "[bold green]⭐[/]" if is_p else "[dim]—[/]",
            f"[bold]{pol}[/]" if is_p else pol,
            ticker, f"[{ac}]{tx[:10]}[/]", amt[:22], date, src[:16],
        )
    console.print(t)


def cmd_status():
    from rich.console import Console
    from rich.table import Table
    from rich import box
    console = Console()
    summary = db.get_portfolio_summary()
    t = Table(title="Open Positions", box=box.ROUNDED)
    t.add_column("Pilot",  style="yellow"); t.add_column("Ticker", style="cyan bold")
    t.add_column("Shares", justify="right"); t.add_column("Entry", justify="right")
    t.add_column("Value",  justify="right"); t.add_column("Mode",  justify="center")
    for p in summary["open_positions"]:
        val  = (p.get("shares") or 0) * (p.get("entry_price") or 0)
        mode = "[yellow]PAPER[/]" if p.get("mode") == "paper" else "[green]LIVE[/]"
        t.add_row(
            p.get("pilot_name",""), p.get("ticker","—"),
            f"{p.get('shares',0):.1f}", f"${p.get('entry_price',0):.2f}",
            f"${val:,.0f}", mode,
        )
    console.print(t)
    console.print(
        f"\n[bold]Summary:[/] {summary['open_count']} open | "
        f"${summary['total_invested_usd']:,.0f} deployed | "
        f"{summary['orders_today']} orders today"
    )


def main():
    parser = argparse.ArgumentParser(description="PoliTrade 3.0 Engine")
    parser.add_argument("--loop",      action="store_true", help="Continuous poll loop")
    parser.add_argument("--test",      action="store_true", help="Fetch disclosures, no orders")
    parser.add_argument("--auth",      action="store_true", help="Schwab OAuth flow")
    parser.add_argument("--status",    action="store_true", help="Portfolio status")
    parser.add_argument("--rebalance", action="store_true", help="Run drift report")
    args = parser.parse_args()

    # --auth doesn't need the database (Schwab OAuth is independent)
    if args.auth:
        broker = SchwabBroker(mode="live")
        ok     = broker.authenticate()
        print("✓ Auth OK" if ok else "✗ Auth failed")
        return

    db.init_db()

    if args.test:
        asyncio.run(cmd_test())
        return

    if args.status:
        cmd_status()
        return

    if args.rebalance:
        cfg     = load_config()
        mode    = cfg.get("engine", {}).get("default_mode", "paper")
        broker  = SchwabBroker(mode=mode)
        broker.authenticate()
        om   = OrderManager(broker)
        rb   = Rebalancer(om, mode=mode)
        report = rb.drift_report()
        if not report:
            print("No drift detected — all positions synced with pilot filings.")
        for item in report:
            print(
                f"DRIFT: {item['pilot_name']} {item['ticker']} "
                f"({item['open_shares']:.0f} shares open, unmirrored sell detected)"
            )
        return

    if args.loop:
        async def _loop():
            engine    = Engine()
            poll_mins = engine.engine_cfg.get("poll_interval_minutes", 30)
            engine.broker.authenticate()
            while True:
                await engine.run_cycle()
                logger.info(f"Sleeping {poll_mins}m until next cycle...")
                await asyncio.sleep(poll_mins * 60)
        asyncio.run(_loop())
        return

    # Default: one cycle (GitHub Actions mode)
    async def _once():
        engine = Engine()
        engine.broker.authenticate()
        stats  = await engine.run_cycle()
        print(
            f"PoliTrade 3.0 | {stats.started_at.strftime('%Y-%m-%d %H:%M ET')} | "
            f"fetched={stats.disclosures_fetched} new={stats.disclosures_new} "
            f"placed={stats.orders_placed} blocked={stats.orders_blocked} "
            f"exits={stats.rebalance_exits}"
        )
    asyncio.run(_once())


if __name__ == "__main__":
    main()
