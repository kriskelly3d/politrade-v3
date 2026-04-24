"""
rebalancer.py — Smart 60-Second Rebalancer
===========================================

Goal: if a tracked politician files a SELL disclosure, we must exit
our mirrored position within 60 seconds of detection.

How it works:
  - Called on every engine cycle (every 30 min baseline, but the executor
    workflow runs every 15 min so effective detection lag = 15 min).
  - Looks at all new SELL disclosures since last_checked_at.
  - For each, if we hold an open position for that pilot+ticker, fires an
    immediate exit order and logs the detection→execution latency.
  - "Within 60 seconds" refers to the time from disclosure detection to
    order submission, not broker fill time.

Note: 30-min polling intervals mean detection lag is poll-based, not real-time.
The 60-second target applies to the rebalancer's own execution time after
a sell disclosure lands in the unprocessed queue.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pytz

from utils import setup_logger, send_discord_alert
import db

logger = setup_logger()
EASTERN = pytz.timezone("America/New_York")


@dataclass
class RebalanceAction:
    pilot_name:  str
    ticker:      str
    shares:      float
    exit_price:  float
    pnl_usd:     float
    pnl_pct:     float
    detection_ms: int  # ms from start of rebalancer call to order submission
    executed:    bool
    error:       Optional[str] = None


class Rebalancer:

    def __init__(self, order_manager, mode: str = "paper"):
        self.order_manager = order_manager
        self.mode          = mode
        self._last_run_at: Optional[datetime] = None

    def run(self, sell_disclosures: list[dict]) -> list[RebalanceAction]:
        """
        Process a list of new SELL disclosures.
        For each one where we hold a position, exit immediately.

        sell_disclosures: list of disclosure dicts with action='SELL'
        Returns list of RebalanceActions (executed or failed).
        """
        if not sell_disclosures:
            return []

        actions: list[RebalanceAction] = []

        for disc in sell_disclosures:
            t0          = time.monotonic()
            pilot_name  = disc.get("pilot_name", "")
            ticker      = disc.get("ticker", "")
            disc_id     = disc.get("disclosure_id", "")
            tx_subtype  = disc.get("transaction_type", "")
            disc_date   = disc.get("transaction_date", "")

            if not pilot_name or not ticker:
                continue

            pos = db.get_position(pilot_name, ticker)
            if not pos:
                continue  # We don't hold this — nothing to rebalance

            logger.info(
                f"Rebalancer: {pilot_name} filed SELL on {ticker} "
                f"— exiting {pos['shares']:.0f} shares immediately"
            )

            pilot_cfg = self.order_manager.cfg.get("pilots", {}).get(pilot_name, {})
            if not pilot_cfg:
                pilot_cfg = {"order_type": "MARKET", "allocation_pct": 0.03}

            result = self.order_manager.execute(
                disclosure_id   = disc_id,
                pilot_name      = pilot_name,
                ticker          = ticker,
                action          = "SELL",
                tx_subtype      = tx_subtype,
                disclosure_date = disc_date,
                pilot_cfg       = pilot_cfg,
                mode            = self.mode,
            )

            detection_ms = int((time.monotonic() - t0) * 1000)

            action = RebalanceAction(
                pilot_name   = pilot_name,
                ticker       = ticker,
                shares       = pos.get("shares", 0),
                exit_price   = result.fill_price,
                pnl_usd      = 0.0,  # filled after position close
                pnl_pct      = 0.0,
                detection_ms = detection_ms,
                executed     = result.success,
                error        = result.error,
            )
            actions.append(action)

            if result.success:
                logger.info(
                    f"Rebalancer exit OK: {ticker} for {pilot_name} | "
                    f"detection→submit={detection_ms}ms"
                )
                if detection_ms > 60_000:
                    logger.warning(
                        f"Rebalancer exceeded 60s target: {detection_ms}ms for {ticker}"
                    )
                send_discord_alert(
                    title=f"⚖️ Rebalancer EXIT: {ticker} [{self.mode.upper()}]",
                    description=f"Mirrored {pilot_name}'s sell of **${ticker}**",
                    color=0xFF6600,
                    fields=[
                        {"name": "Pilot",         "value": pilot_name,              "inline": True},
                        {"name": "Shares Exited", "value": f"{pos['shares']:.0f}",  "inline": True},
                        {"name": "Exit Price",    "value": f"${result.fill_price:.2f}", "inline": True},
                        {"name": "Latency",       "value": f"{detection_ms}ms",     "inline": True},
                    ],
                )
            else:
                logger.warning(
                    f"Rebalancer exit FAILED: {ticker} for {pilot_name}: {result.error}"
                )

        self._last_run_at = datetime.now(EASTERN)
        return actions

    def drift_report(self) -> list[dict]:
        """
        Returns a report of open positions where the pilot has subsequently
        filed a SELL (i.e. we are out of sync with the pilot's current holdings).
        Used by the dashboard to show drift warnings.
        """
        open_pos = db.get_open_positions(mode=self.mode)
        report   = []

        for pos in open_pos:
            ticker     = pos["ticker"]
            pilot_name = pos["pilot_name"]
            # Check if pilot has any recent SELL disclosure for this ticker
            # that we haven't mirrored yet (processed=FALSE, action SELL)
            with db.get_conn() as conn:
                cur = conn.cursor()
                cur.execute("""
                    SELECT * FROM disclosures
                    WHERE politician ILIKE %s
                      AND ticker = %s
                      AND transaction_type LIKE %s
                      AND processed = FALSE
                    ORDER BY ingested_at DESC LIMIT 1
                """, (f"%{pilot_name.split()[-1]}%", ticker, "%sale%"))
                row = cur.fetchone()
                cur.close()

            if row:
                report.append({
                    "pilot_name":  pilot_name,
                    "ticker":      ticker,
                    "open_shares": pos["shares"],
                    "entry_price": pos["entry_price"],
                    "drift_type":  "unmirrored_sell",
                    "disclosure":  row,
                })

        return report
