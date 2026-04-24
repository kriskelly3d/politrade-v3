"""
db.py — PoliTrade 3.0 Database Layer
=====================================

New v3 schema: clean relational design with mode='paper'|'live' on every
order and position. All tables are created idempotently on startup.

Connection: Supabase PostgreSQL via pooler URL (IPv4-safe for GitHub Actions).
"""

from __future__ import annotations

import os
from contextlib import contextmanager
from datetime import date, datetime
from typing import Optional

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import psycopg2
    import psycopg2.extras
    import psycopg2.pool
    PG_AVAILABLE = True
except ImportError:
    PG_AVAILABLE = False

import logging
logger = logging.getLogger("politrade")

_pool: Optional["psycopg2.pool.ThreadedConnectionPool"] = None


def _get_pool():
    global _pool
    if _pool is None:
        url = os.environ.get("SUPABASE_DB_URL", "")
        if not url or "CHANGE_ME" in url:
            raise RuntimeError(
                "SUPABASE_DB_URL not set. Add it to .env or GitHub Actions secrets.\n"
                "Format: postgresql://postgres.[REF]:[PASS]@aws-1-us-west-2.pooler.supabase.com:6543/postgres"
            )
        _pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1, maxconn=8, dsn=url, connect_timeout=10
        )
        logger.info("DB pool ready.")
    return _pool


@contextmanager
def get_conn():
    pool = _get_pool()
    conn = pool.getconn()
    conn.autocommit = False
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


def _row(cur, row) -> Optional[dict]:
    if row is None:
        return None
    return dict(zip([d[0] for d in cur.description], row))


def _rows(cur, rows) -> list[dict]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]


# ── Schema ────────────────────────────────────────────────────────────────────

SCHEMA = """
CREATE TABLE IF NOT EXISTS pilots (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL UNIQUE,
    enabled         BOOLEAN DEFAULT TRUE,
    allocation_pct  REAL DEFAULT 0.03,
    order_type      TEXT DEFAULT 'MARKET',
    limit_offset_pct REAL DEFAULT 0.005,
    priority        INTEGER DEFAULT 99,
    mode            TEXT DEFAULT 'paper',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS disclosures (
    id               SERIAL PRIMARY KEY,
    disclosure_id    TEXT NOT NULL UNIQUE,
    politician       TEXT NOT NULL,
    ticker           TEXT,
    chamber          TEXT,
    transaction_type TEXT,
    transaction_date DATE,
    disclosure_date  DATE,
    amount_range     TEXT,
    amount_mid       REAL,
    source           TEXT,
    processed        BOOLEAN DEFAULT FALSE,
    action_taken     TEXT DEFAULT 'pending',
    ingested_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_disc_id ON disclosures(disclosure_id);
CREATE INDEX IF NOT EXISTS idx_disc_processed ON disclosures(processed, ingested_at DESC);

CREATE TABLE IF NOT EXISTS orders (
    id              SERIAL PRIMARY KEY,
    disclosure_id   TEXT,
    pilot_name      TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    action          TEXT NOT NULL,
    shares          REAL NOT NULL,
    order_type      TEXT NOT NULL DEFAULT 'MARKET',
    limit_price     REAL,
    fill_price      REAL,
    schwab_order_id TEXT,
    status          TEXT DEFAULT 'PENDING',
    mode            TEXT NOT NULL DEFAULT 'paper',
    error_message   TEXT,
    placed_at       TIMESTAMPTZ DEFAULT NOW(),
    filled_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_orders_pilot ON orders(pilot_name, placed_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_date ON orders(placed_at DESC);

CREATE TABLE IF NOT EXISTS positions (
    id              SERIAL PRIMARY KEY,
    pilot_name      TEXT NOT NULL,
    ticker          TEXT NOT NULL,
    shares          REAL NOT NULL,
    entry_price     REAL,
    entry_date      DATE DEFAULT CURRENT_DATE,
    disclosure_date DATE,
    status          TEXT DEFAULT 'OPEN',
    close_price     REAL,
    close_date      DATE,
    pnl_usd         REAL,
    pnl_pct         REAL,
    schwab_order_id TEXT,
    mode            TEXT NOT NULL DEFAULT 'paper',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_pos_open ON positions(status, pilot_name);
CREATE INDEX IF NOT EXISTS idx_pos_ticker ON positions(ticker, status);

CREATE TABLE IF NOT EXISTS slippage_events (
    id             SERIAL PRIMARY KEY,
    disclosure_id  TEXT,
    pilot_name     TEXT,
    ticker         TEXT,
    action         TEXT,
    trade_date     TEXT,
    price_then     REAL,
    price_now      REAL,
    move_pct       REAL,
    spread_pct     REAL,
    volume         BIGINT,
    threshold_pct  REAL,
    blocked_reason TEXT,
    logged_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS token_store (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS daily_orders (
    id         SERIAL PRIMARY KEY,
    trade_date DATE DEFAULT CURRENT_DATE,
    ticker     TEXT NOT NULL,
    pilot_name TEXT,
    mode       TEXT DEFAULT 'paper',
    placed_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_daily ON daily_orders(trade_date, mode);

CREATE TABLE IF NOT EXISTS engine_runs (
    id                  SERIAL PRIMARY KEY,
    run_at              TIMESTAMPTZ DEFAULT NOW(),
    state_reached       TEXT,
    disclosures_fetched INTEGER DEFAULT 0,
    disclosures_new     INTEGER DEFAULT 0,
    orders_placed       INTEGER DEFAULT 0,
    orders_blocked      INTEGER DEFAULT 0,
    error_message       TEXT,
    duration_ms         INTEGER
);
"""


def init_db() -> None:
    logger.info("Initializing database schema...")
    with get_conn() as conn:
        cur = conn.cursor()
        for stmt in SCHEMA.strip().split(";"):
            s = stmt.strip()
            if s:
                cur.execute(s)
        cur.close()
    logger.info("Schema ready.")


# ── Disclosures ───────────────────────────────────────────────────────────────

def is_processed(disclosure_id: str) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT id FROM disclosures WHERE disclosure_id = %s", (disclosure_id,))
        return cur.fetchone() is not None


def upsert_disclosure(d: dict, action: str = "pending") -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO disclosures
                (disclosure_id, politician, ticker, chamber, transaction_type,
                 transaction_date, disclosure_date, amount_range, amount_mid,
                 source, processed, action_taken)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (disclosure_id) DO NOTHING
        """, (
            d.get("disclosure_id"), d.get("politician"), d.get("ticker"),
            d.get("chamber"), d.get("transaction_type"),
            d.get("transaction_date"), d.get("disclosure_date"),
            d.get("amount_range"), d.get("amount_mid", 0),
            d.get("source"), action != "pending", action,
        ))
        cur.close()


def mark_processed(disclosure_id: str, action: str) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            UPDATE disclosures SET processed = TRUE, action_taken = %s
            WHERE disclosure_id = %s
        """, (action, disclosure_id))
        cur.close()


def get_unprocessed_disclosures(limit: int = 100) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM disclosures
            WHERE processed = FALSE
            ORDER BY ingested_at DESC LIMIT %s
        """, (limit,))
        rows = _rows(cur, cur.fetchall())
        cur.close()
        return rows


# ── Orders ────────────────────────────────────────────────────────────────────

def log_order(
    disclosure_id: str, pilot_name: str, ticker: str, action: str,
    shares: float, order_type: str, mode: str,
    fill_price: float = 0, schwab_order_id: str = None,
    status: str = "FILLED", error_message: str = None,
) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO orders
                (disclosure_id, pilot_name, ticker, action, shares,
                 order_type, fill_price, schwab_order_id, status, mode, error_message)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id
        """, (
            disclosure_id, pilot_name, ticker, action, shares,
            order_type, fill_price, schwab_order_id, status, mode, error_message,
        ))
        row_id = cur.fetchone()[0]
        cur.close()
        return row_id


def get_recent_orders(limit: int = 20, mode: str = None) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        if mode:
            cur.execute(
                "SELECT * FROM orders WHERE mode=%s ORDER BY placed_at DESC LIMIT %s",
                (mode, limit)
            )
        else:
            cur.execute("SELECT * FROM orders ORDER BY placed_at DESC LIMIT %s", (limit,))
        rows = _rows(cur, cur.fetchall())
        cur.close()
        return rows


# ── Positions ─────────────────────────────────────────────────────────────────

def open_position(
    pilot_name: str, ticker: str, shares: float, entry_price: float,
    disclosure_date: str, schwab_order_id: str, mode: str,
) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO positions
                (pilot_name, ticker, shares, entry_price, entry_date,
                 disclosure_date, status, schwab_order_id, mode)
            VALUES (%s,%s,%s,%s,CURRENT_DATE,%s,'OPEN',%s,%s)
            RETURNING id
        """, (pilot_name, ticker, shares, entry_price,
              disclosure_date, schwab_order_id, mode))
        row_id = cur.fetchone()[0]
        cur.close()
        return row_id


def close_position(
    pilot_name: str, ticker: str, close_price: float,
    partial: bool = False, shares_sold: float = None,
) -> Optional[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM positions
            WHERE pilot_name=%s AND ticker=%s AND status='OPEN'
            ORDER BY entry_date DESC LIMIT 1
        """, (pilot_name, ticker))
        row = cur.fetchone()
        if not row:
            return None
        pos = _row(cur, row)
        ep = pos["entry_price"] or close_price
        held = pos["shares"]
        closing = shares_sold if shares_sold else held
        pnl_usd = (close_price - ep) * closing
        pnl_pct = ((close_price - ep) / ep * 100) if ep else 0
        new_status = "PARTIAL" if (partial and closing < held) else "CLOSED"
        remaining = held - closing if new_status == "PARTIAL" else 0
        cur.execute("""
            UPDATE positions
            SET status=%s, shares=%s, close_price=%s,
                close_date=CURRENT_DATE, pnl_usd=%s, pnl_pct=%s,
                updated_at=NOW()
            WHERE id=%s
        """, (new_status, remaining, close_price, pnl_usd, pnl_pct, pos["id"]))
        cur.close()
        return {**pos, "pnl_usd": pnl_usd, "pnl_pct": pnl_pct}


def get_open_positions(pilot_name: str = None, mode: str = None) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        filters, vals = ["status='OPEN'"], []
        if pilot_name:
            filters.append("pilot_name=%s"); vals.append(pilot_name)
        if mode:
            filters.append("mode=%s"); vals.append(mode)
        cur.execute(
            f"SELECT * FROM positions WHERE {' AND '.join(filters)} ORDER BY pilot_name, entry_date DESC",
            vals
        )
        rows = _rows(cur, cur.fetchall())
        cur.close()
        return rows


def get_position(pilot_name: str, ticker: str) -> Optional[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM positions
            WHERE pilot_name=%s AND ticker=%s AND status='OPEN'
            ORDER BY entry_date DESC LIMIT 1
        """, (pilot_name, ticker))
        row = cur.fetchone()
        result = _row(cur, row) if row else None
        cur.close()
        return result


def get_closed_positions(pilot_name: str = None, limit: int = 50, mode: str = None) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        filters, vals = ["status IN ('CLOSED','PARTIAL')"], []
        if pilot_name:
            filters.append("pilot_name=%s"); vals.append(pilot_name)
        if mode:
            filters.append("mode=%s"); vals.append(mode)
        vals.append(limit)
        cur.execute(
            f"SELECT * FROM positions WHERE {' AND '.join(filters)} ORDER BY close_date DESC LIMIT %s",
            vals
        )
        rows = _rows(cur, cur.fetchall())
        cur.close()
        return rows


# ── Daily order cap ───────────────────────────────────────────────────────────

def count_orders_today(mode: str) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT COUNT(*) FROM daily_orders WHERE trade_date=CURRENT_DATE AND mode=%s",
            (mode,)
        )
        count = cur.fetchone()[0]
        cur.close()
        return count


def record_daily_order(ticker: str, pilot_name: str, mode: str) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO daily_orders (ticker, pilot_name, mode) VALUES (%s,%s,%s)",
            (ticker, pilot_name, mode)
        )
        cur.close()


# ── Slippage log ──────────────────────────────────────────────────────────────

def log_slippage_event(
    disclosure_id: str, pilot_name: str, ticker: str, action: str,
    trade_date: str, price_then: float, price_now: float,
    move_pct: float, spread_pct: float, volume: int,
    threshold_pct: float, reason: str,
) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO slippage_events
                (disclosure_id, pilot_name, ticker, action, trade_date,
                 price_then, price_now, move_pct, spread_pct, volume,
                 threshold_pct, blocked_reason)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (disclosure_id, pilot_name, ticker, action, trade_date,
              price_then, price_now, move_pct, spread_pct, volume,
              threshold_pct, reason))
        cur.close()


# ── Token store ───────────────────────────────────────────────────────────────

def store_token(key: str, value: str) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO token_store (key, value, updated_at)
            VALUES (%s,%s,NOW())
            ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=NOW()
        """, (key, value))
        cur.close()


def get_token(key: str) -> Optional[str]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM token_store WHERE key=%s", (key,))
        row = cur.fetchone()
        cur.close()
        return row[0] if row else None


# ── Engine runs ───────────────────────────────────────────────────────────────

def log_engine_run(
    state_reached: str, disclosures_fetched: int, disclosures_new: int,
    orders_placed: int, orders_blocked: int,
    error_message: Optional[str], duration_ms: int,
) -> None:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO engine_runs
                (state_reached, disclosures_fetched, disclosures_new,
                 orders_placed, orders_blocked, error_message, duration_ms)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
        """, (state_reached, disclosures_fetched, disclosures_new,
              orders_placed, orders_blocked, error_message, duration_ms))
        cur.close()


def get_last_run() -> Optional[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT * FROM engine_runs ORDER BY run_at DESC LIMIT 1")
        row = cur.fetchone()
        result = _row(cur, row) if row else None
        cur.close()
        return result


# ── Portfolio summary (for dashboard) ────────────────────────────────────────

def get_portfolio_summary(mode: str = None) -> dict:
    with get_conn() as conn:
        cur = conn.cursor()
        mode_filter = "AND mode=%s" if mode else ""
        vals_open = (mode,) if mode else ()

        cur.execute(
            f"SELECT pilot_name, ticker, shares, entry_price, entry_date, mode "
            f"FROM positions WHERE status='OPEN' {mode_filter} ORDER BY entry_date DESC",
            vals_open
        )
        open_rows = _rows(cur, cur.fetchall())

        cur.execute(
            f"""SELECT pilot_name,
                    SUM(pnl_usd) AS total_pnl,
                    COUNT(*) AS trades,
                    SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS winners,
                    AVG(pnl_pct) AS avg_pnl_pct
               FROM positions
               WHERE status IN ('CLOSED','PARTIAL') {mode_filter}
               GROUP BY pilot_name ORDER BY total_pnl DESC""",
            vals_open
        )
        pilot_perf = _rows(cur, cur.fetchall())

        cur.execute(
            f"SELECT COUNT(*) FROM daily_orders WHERE trade_date=CURRENT_DATE {mode_filter}",
            vals_open
        )
        orders_today = cur.fetchone()[0]
        cur.close()

    return {
        "open_positions":     open_rows,
        "open_count":         len(open_rows),
        "total_invested_usd": sum((r["shares"] or 0) * (r["entry_price"] or 0) for r in open_rows),
        "pilot_performance":  {r["pilot_name"]: r for r in pilot_perf},
        "orders_today":       orders_today,
    }


# ── Leaderboard (politicians ranked by our mirrored ROI) ─────────────────────

def get_leaderboard(mode: str = None) -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        mode_filter = "AND mode=%s" if mode else ""
        vals = (mode,) if mode else ()
        cur.execute(
            f"""SELECT pilot_name AS politician,
                    COUNT(*) AS total_trades,
                    SUM(CASE WHEN pnl_usd > 0 THEN 1 ELSE 0 END) AS winners,
                    SUM(pnl_usd) AS total_pnl_usd,
                    AVG(pnl_pct) AS avg_return_pct,
                    MAX(close_date) AS last_trade
               FROM positions
               WHERE status IN ('CLOSED','PARTIAL') {mode_filter}
               GROUP BY pilot_name
               ORDER BY total_pnl_usd DESC""",
            vals
        )
        rows = _rows(cur, cur.fetchall())
        cur.close()
        return rows
