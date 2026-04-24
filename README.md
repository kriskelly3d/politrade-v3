# PoliTrade 3.0 — Operating Manual

> Automated congressional trade mirroring. Built on Schwab API + Supabase + Streamlit.

---

## Quick Status Check

| Component | How to verify |
|-----------|---------------|
| Engine    | GitHub Actions → `executor` workflow → last run |
| Database  | Streamlit → Command Center → Engine Health |
| Token     | Streamlit → Command Center → Auth section (days remaining) |
| Discord   | Watch your phone for trade alerts |

---

## Architecture

```
Capitol Trades (HTML) ─┐
Senate GitHub (JSON)  ─┼─→ ingest.py (async httpx)
                        │       │
                    Supabase (disclosures table)
                        │
                   engine.py (State Machine)
                   IDLE → SCAN → ANALYZE → RISK_CHECK → EXECUTE → IDLE
                        │
                   guards.py (5 guards + Slippage 2.0)
                        │
                   broker.py → Schwab API → Real Orders
                        │
                   rebalancer.py → 60-second exit on pilot SELLs
                        │
                   Supabase (orders, positions, slippage_events)
                        │
                   Discord webhooks + Streamlit dashboard
```

### Separate GitHub Actions jobs:
- **Scraper** (`scraper.yml`) — runs every 15 min, fetches disclosures → Supabase
- **Executor** (`executor.yml`) — runs every 30 min, processes disclosures → orders

---

## File Reference

| File | Purpose |
|------|---------|
| `engine.py`     | Main async state machine. Entry point for all CLI commands. |
| `ingest.py`     | Async httpx fetchers for Capitol Trades + Senate GitHub |
| `broker.py`     | Schwab API wrapper + remote auth flow |
| `guards.py`     | All 5 pre-trade guards including Slippage 2.0 |
| `rebalancer.py` | 60-second exit when pilot files a sell |
| `db.py`         | Supabase PostgreSQL layer — all tables + queries |
| `app.py`        | Streamlit dashboard (Leaderboard / Portfolio / Command Center) |
| `utils.py`      | Config loader, logging, Discord alerts |
| `config.json`   | Pilot profiles, guard thresholds, engine settings |
| `.env`          | Local secrets (never commit) |
| `legacy_v2/`    | Original v2 source files (reference only) |

---

## CLI Commands

```bash
# One cycle — used by GitHub Actions
python engine.py

# Continuous loop — local dev
python engine.py --loop

# Inspect live disclosures without placing orders
python engine.py --test

# Schwab OAuth (terminal flow — first-time or recovery)
python engine.py --auth

# Print open positions and P&L
python engine.py --status

# Print drift report (positions out of sync with pilots)
python engine.py --rebalance
```

---

## Configuration (`config.json`)

### Switching to Live Mode

1. Change `engine.default_mode` from `"paper"` to `"live"`
2. For each pilot you want live, change their `"mode"` to `"live"`
3. Push to GitHub — the executor will pick it up on next run

```json
"engine": { "default_mode": "live" },
"pilots": {
  "Nancy Pelosi": { "mode": "live", ... }
}
```

### Adding a Pilot

1. Run `python engine.py --test` to find the exact politician name format
2. Add to `config.json` under `pilots`:

```json
"Jane Smith": {
  "enabled": true,
  "allocation_pct": 0.03,
  "order_type": "MARKET",
  "priority": 5,
  "mode": "paper"
}
```

### Guard Thresholds

```json
"guards": {
  "slippage_pct": 10.0,       // Block if price moved >10% since pol's trade date
  "slippage_v2": {
    "enabled": true,
    "max_spread_pct": 0.5,    // Warn if spread > 0.5%
    "soft_block_spread_pct": 1.0,  // Block if spread > 1%
    "min_volume": 100000      // Warn if daily volume < 100k shares
  }
}
```

---

## GitHub Secrets (Required)

Set these in GitHub → repo → Settings → Secrets → Actions:

| Secret | Value |
|--------|-------|
| `SUPABASE_DB_URL` | `postgresql://postgres.usarxtgzexryzeynfblz:[PASSWORD]@aws-1-us-west-2.pooler.supabase.com:6543/postgres` |
| `SCHWAB_APP_KEY` | `0ndIBCsspspKpJ3yoHx2ULUPCK4EkNPil33hH3gT1szk1cKi` |
| `SCHWAB_APP_SECRET` | (from config) |
| `SCHWAB_TOKEN_JSON` | Full contents of `data/schwab_token.json` — **update every 7 days** |
| `DISCORD_WEBHOOK` | Your Discord webhook URL |

### Setting secrets via CLI

```bash
gh secret set SUPABASE_DB_URL   --body "postgresql://postgres.usarxtgzexryzeynfblz:..."
gh secret set SCHWAB_APP_KEY    --body "0ndIBCsspspKpJ3yoHx2ULUPCK4EkNPil33hH3gT1szk1cKi"
gh secret set SCHWAB_APP_SECRET --body "GumN3pCUKF1ZluXnmIxNxarOgerUwAJh8vwFJMqoYji0BqDcZ0O3TaUUxSfBcGEY"
gh secret set SCHWAB_TOKEN_JSON < data/schwab_token.json
gh secret set DISCORD_WEBHOOK   --body "https://discordapp.com/api/webhooks/..."
```

---

## Streamlit Dashboard Deployment

1. Push this repo to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. New app → select repo → branch `main` → file `v3/app.py`
4. Advanced settings → add these secrets:
   - `SUPABASE_DB_URL`
   - `SCHWAB_APP_KEY`
   - `SCHWAB_APP_SECRET`
   - `DISCORD_WEBHOOK`
   - `AUTH_PAGE_PASSWORD` (choose a strong password for the auth portal)

---

## Schwab Token — 7-Day Renewal Process

The Schwab OAuth refresh token expires every **7 days**. You'll get a Discord
warning 2 days before expiry.

### Option A — Streamlit dashboard (recommended)
1. Open Streamlit → Command Center
2. Enter Auth Portal Password
3. Click "Generate Schwab Auth URL"
4. Open the URL in browser, log in, click Allow
5. Copy the full redirect URL (starts with `https://127.0.0.1?code=...`)
6. Paste it in the dashboard → click "Complete Auth"
7. Token is saved to Supabase automatically. Then update GitHub secret:

```bash
gh secret set SCHWAB_TOKEN_JSON < data/schwab_token.json
```

### Option B — Terminal
```bash
python engine.py --auth
# Follow the prompts, paste the redirect URL
gh secret set SCHWAB_TOKEN_JSON < data/schwab_token.json
```

---

## Database Schema

| Table | Purpose |
|-------|---------|
| `disclosures` | All fetched disclosures with processing status |
| `orders` | Every order placed (paper and live) with `mode` column |
| `positions` | Open and closed positions with P&L and `mode` column |
| `slippage_events` | Guard 3 rejections with spread + volume data |
| `pilots` | Configurable pilot profiles (future: runtime editing) |
| `token_store` | Schwab OAuth token (shared between Actions + dashboard) |
| `daily_orders` | Rate limiting — orders per day per mode |
| `engine_runs` | Health tracking for each execution |

**Supabase connection**: Always use the **pooler URL** (port 6543), not the direct URL.
Direct URL is IPv6 and GitHub Actions runners can't reach it.

---

## The 5 Guards (Do Not Remove or Reorder)

1. **Daily cap** — max 10 orders per day per mode
2. **Position exists** — SELLs require an open position in DB
3. **Slippage 2.0** — price move + bid-ask spread + volume check
4. **Buying power** — live mode only: ensures cash headroom + $2k reserve
5. **Market hours** — advisory only, non-blocking (Schwab queues after-hours orders)

---

## Known Non-Issues

| Symptom | Reason | Action |
|---------|--------|--------|
| Senate GitHub 404 | 2026 data not published yet | Expected, Capitol Trades covers it |
| `placed=0 blocked=0` on first run | All disclosures already in DB from local testing | Correct deduplication behavior |
| `authlib.jose` deprecation warning | schwab-py dependency, harmless | Ignore |
| Node.js 20 deprecation in Actions | GitHub runner, not our code | Ignore |
| `efts.senate.gov` connection errors | Government site blocks bots | Expected |

---

*PoliTrade 3.0 — Built April 2026*
