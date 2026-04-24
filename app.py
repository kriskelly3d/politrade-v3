"""
app.py — PoliTrade 3.0 Streamlit Dashboard (Mobile-First)
==========================================================

3 Views:
  View 1: Leaderboard    — Performance-ranked politicians (ROI, win rate)
  View 2: Portfolio      — Your trades, ROI charts, Win/Loss breakdown
  View 3: Command Center — Toggle autopilot, paper/live switch, auth refresh

Deploy: share.streamlit.io → politrade repo → app.py
Secrets required: SUPABASE_DB_URL, SCHWAB_APP_KEY, SCHWAB_APP_SECRET,
                  DISCORD_WEBHOOK, AUTH_PAGE_PASSWORD
"""

from __future__ import annotations

import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config (mobile-first: wide mode unnecessary, padding reduced) ────────
st.set_page_config(
    page_title="PoliTrade 3.0",
    page_icon="🏛️",
    layout="centered",
    initial_sidebar_state="collapsed",
)

# ── Inject secrets as env vars so db.py/broker.py can find them ──────────────
try:
    for key in ("SUPABASE_DB_URL", "SCHWAB_APP_KEY", "SCHWAB_APP_SECRET",
                "DISCORD_WEBHOOK", "AUTH_PAGE_PASSWORD"):
        if key not in os.environ:
            val = st.secrets.get(key)
            if val:
                os.environ[key] = val
except Exception:
    pass

import db
from utils import load_config

# ── Custom CSS (mobile-first, dark-friendly) ──────────────────────────────────
st.markdown("""
<style>
  .metric-card {
    background: #1e1e2e; border-radius: 12px; padding: 1rem;
    margin: 0.4rem 0; text-align: center;
  }
  .metric-val  { font-size: 1.8rem; font-weight: 700; }
  .metric-lbl  { font-size: 0.75rem; color: #888; margin-top: 2px; }
  .pill-green  { background: #2d6a4f; color: #fff; border-radius: 12px;
                 padding: 2px 10px; font-size: 0.8rem; }
  .pill-red    { background: #6b2737; color: #fff; border-radius: 12px;
                 padding: 2px 10px; font-size: 0.8rem; }
  .pill-paper  { background: #3a3a5c; color: #aaa; border-radius: 12px;
                 padding: 2px 8px; font-size: 0.75rem; }
  div[data-testid="stTabs"] button { font-size: 1rem; padding: 0.5rem 1rem; }
</style>
""", unsafe_allow_html=True)

# ── DB init + cache ───────────────────────────────────────────────────────────

@st.cache_resource(ttl=300)
def _init_db():
    db.init_db()
    return True

@st.cache_data(ttl=60)
def _leaderboard():
    return db.get_leaderboard()

@st.cache_data(ttl=30)
def _summary():
    return db.get_portfolio_summary()

@st.cache_data(ttl=30)
def _open_positions():
    return db.get_open_positions()

@st.cache_data(ttl=30)
def _closed_positions(limit=50):
    return db.get_closed_positions(limit=limit)

@st.cache_data(ttl=30)
def _recent_orders(limit=30):
    return db.get_recent_orders(limit=limit)

@st.cache_data(ttl=60)
def _last_run():
    return db.get_last_run()

try:
    _init_db()
    DB_OK = True
except Exception as e:
    DB_OK = False
    DB_ERR = str(e)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("## 🏛️ PoliTrade 3.0")

if not DB_OK:
    st.error(f"Database connection failed: {DB_ERR}")
    st.stop()

run = _last_run()
if run:
    run_at = run.get("run_at", "")
    st.caption(f"Last engine run: {run_at} | {run.get('orders_placed',0)} placed | {run.get('duration_ms',0)}ms")

# ── Navigation tabs ───────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📊 Leaderboard", "💼 Portfolio", "⚙️ Command Center"])


# ══════════════════════════════════════════════════════════════════════════════
# VIEW 1: LEADERBOARD
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### 🏆 Politicians — Ranked by Total P&L")
    st.caption("Your mirrored returns, sorted best to worst.")

    lb = _leaderboard()

    if not lb:
        st.info("No closed trades yet. The leaderboard populates once positions are closed.")
    else:
        df = pd.DataFrame(lb)
        df["win_rate_pct"] = (df["winners"] / df["total_trades"] * 100).round(1)
        df["total_pnl_usd"] = df["total_pnl_usd"].fillna(0)
        df["avg_return_pct"] = df["avg_return_pct"].fillna(0).round(2)

        # Bar chart — ROI per politician
        fig = px.bar(
            df.head(10),
            x="politician", y="total_pnl_usd",
            color="total_pnl_usd",
            color_continuous_scale=["#6b2737", "#2d6a4f"],
            labels={"politician": "Politician", "total_pnl_usd": "Total P&L ($)"},
            title="Mirrored P&L by Politician",
        )
        fig.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc", showlegend=False, margin=dict(l=0, r=0, t=40, b=0),
            xaxis_tickangle=-30,
        )
        st.plotly_chart(fig, use_container_width=True)

        # Table
        display_df = df[[
            "politician", "total_trades", "win_rate_pct",
            "total_pnl_usd", "avg_return_pct", "last_trade"
        ]].copy()
        display_df.columns = ["Politician", "Trades", "Win%", "P&L ($)", "Avg Return%", "Last Trade"]
        display_df["P&L ($)"] = display_df["P&L ($)"].map("${:,.0f}".format)
        display_df["Win%"]    = display_df["Win%"].map("{:.0f}%".format)
        display_df["Avg Return%"] = display_df["Avg Return%"].map("{:+.1f}%".format)

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Politician": st.column_config.TextColumn(width="medium"),
                "P&L ($)":    st.column_config.TextColumn(width="small"),
            }
        )


# ══════════════════════════════════════════════════════════════════════════════
# VIEW 2: PORTFOLIO
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    summary = _summary()

    # Top metrics
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric("Open Positions", summary.get("open_count", 0))
    with c2:
        st.metric("Capital Deployed", f"${summary.get('total_invested_usd', 0):,.0f}")
    with c3:
        total_pnl = sum(
            r.get("total_pnl_usd", 0) or 0
            for r in summary.get("pilot_performance", {}).values()
        )
        st.metric("Total Realized P&L", f"${total_pnl:+,.0f}",
                  delta_color="normal")
    with c4:
        st.metric("Orders Today", summary.get("orders_today", 0))

    st.divider()

    # Open positions
    st.markdown("#### Open Positions")
    open_pos = _open_positions()
    if not open_pos:
        st.info("No open positions.")
    else:
        df_open = pd.DataFrame(open_pos)
        df_open["Value ($)"] = (df_open["shares"] * df_open["entry_price"]).map("${:,.0f}".format)
        df_open["Entry ($)"] = df_open["entry_price"].map("${:.2f}".format)
        df_open["Mode"] = df_open["mode"].str.upper()
        st.dataframe(
            df_open[["pilot_name", "ticker", "shares", "Entry ($)", "Value ($)",
                      "entry_date", "Mode"]].rename(columns={
                "pilot_name": "Pilot", "ticker": "Ticker", "shares": "Shares",
                "entry_date": "Entered", "Mode": "Mode",
            }),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # Closed P&L
    st.markdown("#### Closed Trades — P&L")
    closed = _closed_positions(limit=50)
    if not closed:
        st.info("No closed trades yet.")
    else:
        df_c = pd.DataFrame(closed)
        df_c["pnl_usd"] = df_c["pnl_usd"].fillna(0)
        df_c["pnl_pct"] = df_c["pnl_pct"].fillna(0)

        # Win/Loss donut
        wins   = (df_c["pnl_usd"] > 0).sum()
        losses = (df_c["pnl_usd"] <= 0).sum()
        fig_wl = go.Figure(go.Pie(
            labels=["Wins", "Losses"],
            values=[wins, losses],
            hole=0.55,
            marker_colors=["#2d6a4f", "#6b2737"],
        ))
        fig_wl.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", margin=dict(l=0, r=0, t=20, b=0),
            font_color="#ccc", showlegend=True, height=220,
            annotations=[{"text": f"{wins}W/{losses}L", "x": 0.5, "y": 0.5,
                           "font_size": 16, "showarrow": False, "font_color": "#fff"}]
        )

        # Cumulative P&L line
        df_sorted = df_c.sort_values("close_date")
        df_sorted["cum_pnl"] = df_sorted["pnl_usd"].cumsum()
        fig_cum = px.area(
            df_sorted, x="close_date", y="cum_pnl",
            labels={"close_date": "Date", "cum_pnl": "Cumulative P&L ($)"},
            color_discrete_sequence=["#3dbc7e"],
        )
        fig_cum.update_layout(
            paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
            font_color="#ccc", margin=dict(l=0, r=0, t=10, b=0), height=220,
        )

        col_left, col_right = st.columns([1, 2])
        with col_left:
            st.plotly_chart(fig_wl, use_container_width=True)
        with col_right:
            st.plotly_chart(fig_cum, use_container_width=True)

        # Closed trades table
        df_c["P&L ($)"] = df_c["pnl_usd"].map("{:+,.0f}".format)
        df_c["P&L %"]   = df_c["pnl_pct"].map("{:+.1f}%".format)
        df_c["Entry"]   = df_c["entry_price"].map("${:.2f}".format)
        df_c["Close"]   = df_c["close_price"].map("${:.2f}".format)
        st.dataframe(
            df_c[["pilot_name", "ticker", "Entry", "Close", "P&L ($)", "P&L %",
                  "close_date", "mode"]].rename(columns={
                "pilot_name": "Pilot", "ticker": "Ticker",
                "close_date": "Closed", "mode": "Mode",
            }),
            use_container_width=True, hide_index=True,
        )

    st.divider()

    # Recent orders
    with st.expander("Recent Orders", expanded=False):
        orders = _recent_orders(30)
        if orders:
            df_o = pd.DataFrame(orders)
            st.dataframe(
                df_o[["placed_at", "pilot_name", "ticker", "action",
                       "shares", "fill_price", "status", "mode"]].rename(columns={
                    "placed_at": "Time", "pilot_name": "Pilot",
                    "ticker": "Ticker", "action": "Action",
                    "shares": "Shares", "fill_price": "Fill ($)",
                    "status": "Status", "mode": "Mode",
                }),
                use_container_width=True, hide_index=True,
            )
        else:
            st.info("No orders placed yet.")


# ══════════════════════════════════════════════════════════════════════════════
# VIEW 3: COMMAND CENTER
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    st.markdown("### ⚙️ Command Center")

    # ── Pilot toggles ──────────────────────────────────────────────────────
    st.markdown("#### Pilot Autopilot Control")
    cfg = load_config()
    pilots = cfg.get("pilots", {})

    if not pilots:
        st.warning("No pilots configured in config.json")
    else:
        for pname, pcfg in pilots.items():
            col_a, col_b, col_c, col_d = st.columns([3, 1, 1, 1])
            with col_a:
                st.write(f"**{pname}**")
            with col_b:
                enabled = pcfg.get("enabled", True)
                badge = "🟢 ON" if enabled else "⚫ OFF"
                st.write(badge)
            with col_c:
                mode = pcfg.get("mode", "paper")
                mode_badge = "📄 PAPER" if mode == "paper" else "🔴 LIVE"
                st.write(mode_badge)
            with col_d:
                alloc = pcfg.get("allocation_pct", 0.03)
                st.write(f"{alloc*100:.0f}%")

        st.caption("To change pilot settings, edit `config.json` in the repo and push.")

    st.divider()

    # ── Paper / Live mode indicator ────────────────────────────────────────
    st.markdown("#### Engine Mode")
    default_mode = cfg.get("engine", {}).get("default_mode", "paper")
    if default_mode == "paper":
        st.info("📄 **PAPER MODE** — Engine is simulating trades. No real money at risk.")
        st.caption(
            "To go live: set `engine.default_mode = \"live\"` in config.json "
            "AND update each pilot's `mode` field."
        )
    else:
        st.error("🔴 **LIVE MODE** — Real orders are being placed via Schwab!")

    st.divider()

    # ── Engine health ──────────────────────────────────────────────────────
    st.markdown("#### Engine Health")
    run = _last_run()
    if run:
        c1, c2, c3 = st.columns(3)
        with c1:
            st.metric("Last Run",    str(run.get("run_at", "—"))[:19])
        with c2:
            st.metric("Orders Placed", run.get("orders_placed", 0))
        with c3:
            st.metric("Duration", f"{run.get('duration_ms', 0)}ms")
        if run.get("error_message"):
            st.error(f"Last error: {run['error_message']}")
    else:
        st.warning("No engine runs recorded yet.")

    st.divider()

    # ── Schwab Token Refresh ────────────────────────────────────────────────
    st.markdown("#### 🔐 Schwab Token Refresh")
    st.caption(
        "The Schwab OAuth token expires every 7 days. "
        "Use this flow to renew it without touching a terminal."
    )

    # Check token age
    try:
        token_json = db.get_token("schwab_token")
        import json, time as _time
        if token_json:
            tdata = json.loads(token_json)
            created = tdata.get("creation_timestamp", 0)
            if created:
                age_days = (_time.time() - created) / 86400
                expire_days = max(0, 7 - age_days)
                if expire_days < 2:
                    st.error(f"⚠️ Token expires in **{expire_days:.1f} days** — refresh NOW!")
                elif expire_days < 4:
                    st.warning(f"Token expires in {expire_days:.1f} days.")
                else:
                    st.success(f"Token OK — expires in {expire_days:.1f} days.")
        else:
            st.warning("No token found in Supabase — first auth required.")
    except Exception:
        st.info("Cannot check token status — database may be unavailable.")

    # Password gate
    auth_pw = st.text_input("Auth Portal Password", type="password", key="auth_pw")
    correct_pw = os.environ.get("AUTH_PAGE_PASSWORD", "PoliTrade3!")
    if auth_pw and auth_pw != correct_pw:
        st.error("Wrong password.")
    elif auth_pw == correct_pw:
        st.success("Access granted.")

        if "auth_step" not in st.session_state:
            st.session_state.auth_step = "idle"

        if st.session_state.auth_step == "idle":
            if st.button("🔗 Generate Schwab Auth URL"):
                from broker import SchwabBroker
                broker = SchwabBroker(mode="live")
                url = broker.start_auth_url()
                if url:
                    st.session_state.auth_url = url
                    st.session_state.auth_step = "pending_url"
                    st.rerun()
                else:
                    st.error("Could not generate auth URL. Check SCHWAB_APP_KEY secret.")

        if st.session_state.auth_step == "pending_url":
            url = st.session_state.get("auth_url", "")
            st.markdown(f"**Step 1:** Open this URL in your browser and log in to Schwab:")
            st.code(url, language=None)
            st.markdown(
                "**Step 2:** After clicking Allow, your browser will redirect to "
                "`https://127.0.0.1?code=...` (shows an error page — that's normal). "
                "Copy the **full URL** from the address bar and paste it below."
            )
            redirected = st.text_input("Paste the full redirect URL here:", key="redirect_url")
            if st.button("✅ Complete Auth") and redirected:
                from broker import SchwabBroker
                broker = SchwabBroker(mode="live")
                ok = broker.complete_auth(redirected)
                if ok:
                    st.success("✅ Token refreshed successfully! Saved to Supabase.")
                    st.session_state.auth_step = "idle"
                    st.cache_data.clear()
                else:
                    st.error("Auth failed — check the URL and try again.")
            if st.button("Cancel"):
                st.session_state.auth_step = "idle"
                st.rerun()

    st.divider()

    # ── Manual trigger ─────────────────────────────────────────────────────
    st.markdown("#### 🚀 Manual Engine Trigger")
    st.caption("Trigger one engine cycle immediately (useful for testing).")
    if st.button("▶️ Run One Cycle Now"):
        with st.spinner("Running engine cycle..."):
            import asyncio
            from engine import Engine
            engine = Engine()
            engine.broker.authenticate()
            stats = asyncio.run(engine.run_cycle())
        st.success(
            f"Cycle complete: {stats.orders_placed} placed, "
            f"{stats.orders_blocked} blocked, "
            f"{stats.rebalance_exits} rebalanced."
        )
        st.cache_data.clear()
        st.rerun()
