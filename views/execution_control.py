"""
Portfolio & Performance — Live positions from Polymarket API,
closed markets with per-market W/L, and portfolio snapshots.
Bot controls (pause/resume, circuit breaker, risk settings) at the bottom.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, timedelta

from services.bot_api_client import get_bot_client


def render():
    st.header("Dashboard")

    client = get_bot_client()
    perf = client.get_performance()

    total_deposited = perf.get("total_deposited", 0)
    positions_value = perf.get("positions_value", 0)
    positions_cost = perf.get("positions_cost", 0)
    unrealized_pnl = perf.get("unrealized_pnl", 0)
    realized_pnl = perf.get("realized_pnl", 0)
    wins = perf.get("wins", 0)
    losses = perf.get("losses", 0)
    open_markets = perf.get("open_market_count", 0)
    equity_curve = perf.get("equity_curve", [])
    total_closed = len(perf.get("closed_markets", []))

    # Real cash balance from Polygon blockchain (USDC.e)
    real_cash = perf.get("cash_balance")
    if real_cash is not None:
        cash_available = real_cash
        portfolio_total = positions_value + real_cash
    else:
        cash_available = max(total_deposited - positions_cost + realized_pnl, 0)
        portfolio_total = positions_value + cash_available
    total_pnl = portfolio_total - total_deposited
    total_pnl_pct = (total_pnl / total_deposited * 100) if total_deposited > 0 else 0
    wr = (wins / total_closed * 100) if total_closed > 0 else 0

    # Today's PNL from equity curve
    today_pnl = _calc_today_pnl(equity_curve, unrealized_pnl, realized_pnl)

    # ══════════════════════════════════════════════════════════════════
    # 1. PORTFOLIO OVERVIEW — 4 Cards in one row
    # ══════════════════════════════════════════════════════════════════
    c1, c2, c3, c4 = st.columns(4)

    # --- Card 1: Portfolio ---
    with c1:
        with st.container(border=True):
            st.caption(f"Portfolio (${total_deposited:,.0f} eingezahlt)")
            st.markdown(f"### ${portfolio_total:,.2f}")
            st.markdown(f"Bargeld: **${cash_available:,.2f}**")
            _today_color = "green" if today_pnl >= 0 else "red"
            _sign = "+" if today_pnl >= 0 else ""
            _today_pct = (today_pnl / portfolio_total * 100) if portfolio_total > 0 else 0
            st.markdown(f":{_today_color}[{_sign}${today_pnl:.2f} ({_sign}{_today_pct:.2f}%) letzter Tag]")

    # --- Card 2: Gewinn / Verlust + Equity Curve ---
    with c2:
        with st.container(border=True):
            _pnl_color = "green" if total_pnl >= 0 else "red"
            st.caption("Gewinn/Verlust")
            period = st.segmented_control(
                "eq", ["1D", "1W", "1M", "All"],
                default="All", key="eq_period",
                label_visibility="collapsed",
            ) or "All"
            st.markdown(f"### :{_pnl_color}[${total_pnl:+,.2f}]")
            st.markdown(f":{_pnl_color}[{total_pnl_pct:+.1f}%] Gesamt")
            if equity_curve:
                df_eq = _filter_equity_curve(equity_curve, period, total_deposited)
                if not df_eq.empty:
                    _chart_color = "#00c853" if total_pnl >= 0 else "#ff1744"
                    _build_equity_chart(df_eq, _chart_color, total_deposited)

    # --- Card 3: Offene Positionen ---
    live_count = len(perf.get("live_positions", []))
    with c3:
        with st.container(border=True):
            st.caption("Offene Positionen")
            st.markdown(f"### ${positions_value:,.2f}")
            st.markdown(f"Einsatz: **${positions_cost:,.2f}**")
            _u_color = "green" if unrealized_pnl >= 0 else "red"
            st.markdown(f"Unrealisiert: **:{_u_color}[${unrealized_pnl:+,.2f}]**")
            st.caption(f"{live_count} Positionen offen")

    # --- Card 4: Realisierter PnL ---
    with c4:
        with st.container(border=True):
            st.caption("Realisiert")
            _r_color = "green" if realized_pnl >= 0 else "red"
            st.markdown(f"### :{_r_color}[${realized_pnl:+,.2f}]")
            st.markdown(f"W/L: **{wins}/{losses}** | WR: **{wr:.0f}%**")
            st.caption(f"{live_count} Positionen offen")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 2. OFFENE POSITIONEN (Live from Polymarket API)
    # ══════════════════════════════════════════════════════════════════
    _pos_pnl_color = "#00c853" if unrealized_pnl >= 0 else "#ff1744"
    _pos_sign = "+" if unrealized_pnl >= 0 else ""
    st.subheader(f"Open Positions ({len(perf.get('live_positions', []))} | ${positions_cost:.2f} Einsatz | ${positions_value:.2f} Wert | {_pos_sign}${unrealized_pnl:.2f} PnL)")

    live_positions = perf.get("live_positions", [])
    if live_positions:
        # Auto-import untracked on-chain positions
        imported_count = 0
        for pos in live_positions:
            if not pos.get("trade_id") and pos.get("market_id"):
                result = client.import_position({
                    "market_id": pos["market_id"],
                    "title": pos.get("title", ""),
                    "outcome": pos.get("outcome", "YES"),
                    "avg_price": pos.get("avg_price", 0),
                    "cost": pos.get("cost", 0),
                    "shares": pos.get("shares", 0),
                })
                if result and result.get("ok"):
                    pos["trade_id"] = result["trade_id"]
                    imported_count += 1
        if imported_count:
            st.info(f"{imported_count} Position(en) automatisch importiert.")

        # --- Filter & Sort controls ---
        fc1, fc2, fc3 = st.columns([3, 1.5, 1.5])
        with fc1:
            search = st.text_input("Suche", placeholder="Markt filtern...", key="pos_search", label_visibility="collapsed")
        with fc2:
            sort_by = st.selectbox("Sortieren", ["Gewinn %", "Gewinn $", "Wert", "Name"], key="pos_sort", label_visibility="collapsed")
        with fc3:
            filter_pnl = st.selectbox("Filter", ["Alle", "Im Gewinn", "Im Verlust", "Einlösbar"], key="pos_filter", label_visibility="collapsed")

        # Apply filters
        filtered = live_positions
        if search:
            filtered = [p for p in filtered if search.lower() in p.get("title", "").lower()]
        if filter_pnl == "Im Gewinn":
            filtered = [p for p in filtered if p.get("pnl", 0) > 0]
        elif filter_pnl == "Im Verlust":
            filtered = [p for p in filtered if p.get("pnl", 0) < 0]
        elif filter_pnl == "Einlösbar":
            filtered = [p for p in filtered if p.get("redeemable")]

        # Apply sort
        if sort_by == "Gewinn %":
            filtered.sort(key=lambda p: p.get("pnl_pct", 0), reverse=True)
        elif sort_by == "Gewinn $":
            filtered.sort(key=lambda p: p.get("pnl", 0), reverse=True)
        elif sort_by == "Wert":
            filtered.sort(key=lambda p: p.get("value", 0), reverse=True)
        elif sort_by == "Name":
            filtered.sort(key=lambda p: p.get("title", ""))

        # --- Header ---
        _W = [3.0, 1.2, 0.8, 0.8, 1.0, 0.7]
        hdr = st.columns(_W)
        for col, lbl in zip(hdr, ["MARKT", "DURCHSCHN. / JETZT", "WETTE", "WERT", "GEWINN & VERLUST", ""]):
            col.markdown(f"**<span style='color:#8892A0;font-size:0.85rem'>{lbl}</span>**", unsafe_allow_html=True)

        # --- Position rows ---
        for i, pos in enumerate(filtered):
            pnl = pos.get("pnl", 0)
            pnl_pct = pos.get("pnl_pct", 0)
            avg_price = pos.get("avg_price", 0)
            cur_price = pos.get("cur_price", 0)
            redeemable = pos.get("redeemable", False)

            # Price in cents
            avg_cents = avg_price * 100
            cur_cents = cur_price * 100
            price_color = "#00c853" if cur_price > avg_price else "#ff1744" if cur_price < avg_price else "#ccc"
            pnl_color = "#00c853" if pnl > 0 else "#ff1744" if pnl < 0 else "#888"
            sign = "+" if pnl >= 0 else ""

            row = st.columns(_W)

            # Col 1: Market name + outcome badge
            outcome = pos.get("outcome", "YES")
            badge_color = "#00c853" if outcome == "YES" else "#ff1744"
            row[0].markdown(
                f"<div style='font-size:1rem;font-weight:500;line-height:1.3'>{pos['title'][:55]}</div>"
                f"<span style='background:{badge_color};color:#fff;padding:1px 8px;border-radius:10px;font-size:0.75rem'>"
                f"{outcome} {cur_cents:.1f}c</span>"
                f"<span style='color:#8892A0;font-size:0.8rem;margin-left:6px'>{pos['shares']:,.1f} Anteile</span>",
                unsafe_allow_html=True,
            )

            # Col 2: Price movement
            row[1].markdown(
                f"<div style='font-size:0.95rem'>"
                f"<span style='color:#8892A0'>{avg_cents:.1f}c</span>"
                f" <span style='color:{price_color}'>&#8594; {cur_cents:.1f}c</span></div>",
                unsafe_allow_html=True,
            )

            # Col 3: Bet amount (cost)
            row[2].markdown(f"<div style='font-size:0.95rem'>${pos.get('cost', 0):.2f}</div>", unsafe_allow_html=True)

            # Col 4: Current value
            row[3].markdown(f"<div style='font-size:0.95rem'>${pos.get('value', 0):.2f}</div>", unsafe_allow_html=True)

            # Col 5: PnL
            if redeemable:
                row[4].markdown(
                    f"<div style='font-size:0.95rem;color:#00c853;font-weight:600'>GEWONNEN</div>"
                    f"<div style='color:#00c853;font-size:0.85rem'>${pos.get('value', 0):.2f}</div>",
                    unsafe_allow_html=True,
                )
            else:
                row[4].markdown(
                    f"<div style='font-size:0.95rem;color:{pnl_color};font-weight:500'>"
                    f"{sign}${pnl:.2f} ({sign}{pnl_pct:.1f}%)</div>",
                    unsafe_allow_html=True,
                )

            # Col 6: Sell button (compact)
            trade_id = pos.get("trade_id")
            if trade_id:
                btn_label = "✓" if redeemable else "Sell"
                if row[5].button(btn_label, key=f"sell_{trade_id}_{i}"):
                    with st.spinner("Verkaufe..."):
                        res = client.manual_cashout(trade_id)
                    if res and res.get("ok"):
                        st.success(f"Verkauft! Profit: ${res.get('profit_usd', 0):+.2f}")
                        st.rerun()
                    else:
                        st.error(f"Fehler: {res.get('error', '?') if res else '?'}")

            # Separator between rows
            if i < len(filtered) - 1:
                st.markdown("<hr style='margin:4px 0;border-color:#1e2530'>", unsafe_allow_html=True)

        # Summary bar
        _sum_color = "#00c853" if unrealized_pnl >= 0 else "#ff1744"
        st.markdown(
            f"<div style='background:#111827;padding:10px 16px;border-radius:8px;margin-top:12px;"
            f"font-size:0.95rem;color:#8892A0'>"
            f"<b>{len(filtered)}</b> Positionen | "
            f"Einsatz <b>${positions_cost:.2f}</b> | "
            f"Wert <b>${positions_value:.2f}</b> | "
            f"PnL <span style='color:{_sum_color}'>"
            f"<b>${unrealized_pnl:+.2f}</b></span></div>",
            unsafe_allow_html=True,
        )
    else:
        st.caption("Keine offenen Positionen.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 2b. OFFENE LIMIT ORDERS
    # ══════════════════════════════════════════════════════════════════
    open_orders = client.get_open_orders()
    if open_orders:
        # Group orders by market (like Polymarket UI)
        from collections import defaultdict
        grouped = defaultdict(lambda: {"orders": [], "total_usd": 0, "total_matched": 0, "total_size": 0})
        for o in open_orders:
            key = o.get("market", o.get("id", ""))
            grouped[key]["orders"].append(o)
            grouped[key]["total_usd"] += o.get("total_usd", 0)
            grouped[key]["total_matched"] += o.get("size_matched", 0)
            grouped[key]["total_size"] += o.get("original_size", 0)
            grouped[key]["name"] = o.get("market_name") or o.get("market", "?")[:16]
            grouped[key]["side"] = o.get("side", "BUY")
            grouped[key]["outcome"] = o.get("outcome", "")
            # Use best (lowest buy / highest sell) price for display
            if "price" not in grouped[key]:
                grouped[key]["price"] = o.get("price", 0)

        markets = list(grouped.values())
        total_order_usd = sum(o.get("total_usd", 0) for o in open_orders)
        st.subheader(f"Open Limit Orders ({len(markets)} | ${total_order_usd:,.2f})")

        _OW = [3.5, 0.8, 0.8, 0.8, 0.8]
        oh = st.columns(_OW)
        for col, lbl in zip(oh, ["MARKT", "SEITE", "PREIS", "GESAMT", "AUSGEFÜHRT"]):
            col.markdown(f"**<span style='color:#8892A0;font-size:0.85rem'>{lbl}</span>**", unsafe_allow_html=True)

        for k, g in enumerate(markets):
            orow = st.columns(_OW)
            side = g["side"]
            side_color = "#00c853" if side == "BUY" else "#ff1744"
            price_cents = g["price"] * 100
            fill_str = f"{g['total_matched']:.0f} / {g['total_size']:.0f}"
            order_count = len(g["orders"])
            count_badge = f"<span style='color:#8892A0;font-size:0.75rem;margin-left:6px'>({order_count}x)</span>" if order_count > 1 else ""

            outcome = g["outcome"]
            badge = f"<span style='background:{side_color};color:#fff;padding:1px 8px;border-radius:10px;font-size:0.75rem'>{side} {outcome}</span>" if outcome else ""

            orow[0].markdown(
                f"<div style='font-size:0.95rem;font-weight:500'>{g['name']}{count_badge}</div>{badge}",
                unsafe_allow_html=True,
            )
            orow[1].markdown(f"<span style='color:{side_color};font-weight:600'>{side}</span>", unsafe_allow_html=True)
            orow[2].markdown(f"<div style='font-size:0.95rem'>{price_cents:.1f}c</div>", unsafe_allow_html=True)
            orow[3].markdown(f"<div style='font-size:0.95rem'>${g['total_usd']:.2f}</div>", unsafe_allow_html=True)
            orow[4].markdown(f"<div style='font-size:0.95rem;color:#8892A0'>{fill_str}</div>", unsafe_allow_html=True)

            if k < len(markets) - 1:
                st.markdown("<hr style='margin:4px 0;border-color:#1e2530'>", unsafe_allow_html=True)

        st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 3. HISTORY (Polymarket-style activity feed with PnL)
    # ══════════════════════════════════════════════════════════════════
    history = client.get_closed_trades()

    if history:
        history.sort(key=lambda t: t.get("executed_at", "") or "", reverse=True)

        # --- Filter ---
        fc1, fc2 = st.columns([3, 1.5])
        with fc1:
            hist_search = st.text_input("Suche", placeholder="Markt filtern...", key="hist_search", label_visibility="collapsed")
        with fc2:
            hist_type = st.selectbox("Typ", ["Alle", "Gekauft", "Verkauft", "Verloren", "Beansprucht", "Belohnung"], key="hist_type", label_visibility="collapsed")

        if hist_search:
            history = [t for t in history if hist_search.lower() in (t.get("market_question") or "").lower()]
        if hist_type != "Alle":
            _type_map = {
                "Gekauft": [None, "", "open"],
                "Verkauft": ["cashout", "sell", "take_profit", "stop_loss", "sold_external", "STOP-LOSS (MANUAL)", "pm_sync_closed", "sync_not_onchain", "expired"],
                "Verloren": ["loss"],
                "Beansprucht": ["win", "settlement_win", "settled", "settled_recovered"],
                "Belohnung": ["yield"],
            }
            allowed = _type_map.get(hist_type, [])
            history = [t for t in history if (t.get("result") or "").lower() in [str(a).lower() if a else "" for a in allowed] or (t.get("result") is None and None in allowed)]

        st.subheader(f"History ({len(history)})")

        # --- Header ---
        _HW = [0.9, 3.0, 0.7, 0.7, 1.0, 0.6]
        hh = st.columns(_HW)
        for col, lbl in zip(hh, ["AKTIVITÄT", "MARKT", "EINSATZ", "PNL", "ZEIT", ""]):
            col.markdown(f"**<span style='color:#8892A0;font-size:0.8rem'>{lbl}</span>**", unsafe_allow_html=True)

        now = datetime.utcnow()
        for j, t in enumerate(history):
            hr = st.columns(_HW)
            result = (t.get("result") or "").lower()
            pnl = float(t.get("realized_pnl") or 0)
            amount = float(t.get("amount_usd") or 0)
            entry_price = float(t.get("entry_price") or 0)
            side = t.get("side", "YES")

            # Relative time
            try:
                exec_dt = datetime.fromisoformat(str(t.get("executed_at", "")))
                delta = now - exec_dt
                if delta.days == 0:
                    time_str = "Heute"
                elif delta.days == 1:
                    time_str = "Gestern"
                elif delta.days < 30:
                    time_str = f"{delta.days}T"
                else:
                    time_str = f"{delta.days // 30}Mo"
            except (ValueError, TypeError):
                time_str = "-"

            # Activity type (Polymarket-style)
            if not result or result == "open":
                activity = "Gekauft"
                activity_color = "#E8ECF1"
            elif result in ("win", "settlement_win", "settled", "settled_recovered"):
                activity = "Beansprucht"
                activity_color = "#00c853"
            elif result == "loss":
                activity = "Verloren"
                activity_color = "#ff1744"
            elif result in ("cashout", "take_profit", "stop_loss", "sold_external",
                            "STOP-LOSS (MANUAL)", "sell", "pm_sync_closed",
                            "sync_not_onchain", "expired"):
                activity = "Verkauft"
                activity_color = "#448AFF"
            elif result == "yield":
                activity = "Belohnung"
                activity_color = "#FFB74D"
            elif result in ("penny_cleanup", "phantom"):
                activity = "Bereinigt"
                activity_color = "#888"
            else:
                activity = result.capitalize() if result else "?"
                activity_color = "#888"

            # Col 1: Activity label
            hr[0].markdown(
                f"<div style='font-size:0.85rem;font-weight:600;color:{activity_color}'>{activity}</div>",
                unsafe_allow_html=True,
            )

            # Col 2: Market name + badge
            name = t.get("market_question", "?")[:50]
            badge_color = "#00c853" if side == "YES" else "#ff1744"
            price_cents = entry_price * 100
            badge = (f"<span style='background:{badge_color};color:#fff;padding:1px 6px;"
                     f"border-radius:10px;font-size:0.7rem'>{side} {price_cents:.0f}c</span>")
            hr[1].markdown(
                f"<div style='font-size:0.88rem;font-weight:500;line-height:1.3'>{name}</div>{badge}",
                unsafe_allow_html=True,
            )

            # Col 3: Einsatz
            hr[2].markdown(f"<div style='font-size:0.9rem'>${amount:.2f}</div>", unsafe_allow_html=True)

            # Col 4: PnL ($ and %)
            if not result or result == "open":
                hr[3].markdown("<div style='color:#555;font-size:0.9rem'>—</div>", unsafe_allow_html=True)
            else:
                pnl_color = "#00c853" if pnl > 0 else "#ff1744" if pnl < 0 else "#8892A0"
                pnl_sign = "+" if pnl >= 0 else ""
                pnl_pct = (pnl / amount * 100) if amount > 0 else 0
                pct_sign = "+" if pnl_pct >= 0 else ""
                hr[3].markdown(
                    f"<div style='font-size:0.9rem;font-weight:600;color:{pnl_color}'>"
                    f"{pnl_sign}${pnl:.2f}</div>"
                    f"<div style='font-size:0.75rem;color:{pnl_color};opacity:0.8'>"
                    f"({pct_sign}{pnl_pct:.1f}%)</div>",
                    unsafe_allow_html=True,
                )

            # Col 5: Relative time
            hr[4].markdown(
                f"<div style='font-size:0.85rem;color:#8892A0'>{time_str}</div>",
                unsafe_allow_html=True,
            )

            # Col 6: empty (alignment)
            hr[5].markdown("", unsafe_allow_html=True)

            if j < len(history) - 1:
                st.markdown("<hr style='margin:3px 0;border-color:#1e2530'>", unsafe_allow_html=True)
    else:
        st.caption("Noch keine History.")

    st.divider()

    # ══════════════════════════════════════════════════════════════════
    # 5. EINZAHLUNG BEARBEITEN
    # ══════════════════════════════════════════════════════════════════
    with st.expander("Einzahlung anpassen", expanded=False):
        new_deposited = st.number_input(
            "Gesamt eingezahlt ($)",
            min_value=0.0, max_value=100000.0,
            value=float(total_deposited), step=50.0,
            key="total_deposited",
        )
        if new_deposited != total_deposited and st.button("Einzahlung speichern"):
            result = client.save_setting("total_deposited", new_deposited)
            if result and result.get("ok"):
                st.success(f"Einzahlung auf ${new_deposited:,.2f} aktualisiert!")
                st.rerun()

    # ══════════════════════════════════════════════════════════════════
    # 5. BOT CONTROLS (collapsed)
    # ══════════════════════════════════════════════════════════════════
    with st.expander("Bot-Steuerung & Risk-Einstellungen", expanded=False):
        _render_bot_controls(client)

        config = client.get_config()
        _render_risk_controls(client, config)


# ======================================================================
# Bot Controls
# ======================================================================

def _render_bot_controls(client):
    """Circuit breaker + pause/resume."""
    cb = client.get_circuit_breaker()
    config = client.get_config()
    cb_config = config.get("circuit_breaker", {})
    max_losses = cb_config.get("max_consecutive_losses", 3)

    col1, col2 = st.columns(2)
    with col1:
        consecutive_losses = cb.get("consecutive_losses", 0)
        paused_until = cb.get("paused_until")
        is_cb_paused = False
        if paused_until:
            try:
                if datetime.fromisoformat(paused_until) > datetime.utcnow():
                    is_cb_paused = True
            except Exception:
                pass

        if is_cb_paused:
            st.error(f"CIRCUIT BREAKER AKTIV — Pausiert bis {paused_until[:16]}")
        else:
            st.success("Trading erlaubt")
        st.caption(f"Verluste in Folge: {consecutive_losses}/{max_losses}")

    with col2:
        if is_cb_paused and st.button("Circuit Breaker zurücksetzen"):
            result = client.reset_circuit_breaker()
            if result and result.get("ok"):
                st.success("Circuit Breaker zurückgesetzt!")
                st.rerun()

    status = client.get_status()
    bot_paused = status.get("bot_paused", False) if status else False

    col_p, col_r = st.columns(2)
    with col_p:
        if st.button("Bot PAUSIEREN", type="primary", disabled=bot_paused):
            result = client.pause_bot()
            if result and result.get("ok"):
                st.warning("Bot wurde pausiert!")
                st.rerun()
    with col_r:
        if st.button("Bot FORTSETZEN", disabled=not bot_paused):
            result = client.resume_bot()
            if result and result.get("ok"):
                st.success("Bot läuft wieder!")
                st.rerun()


# ======================================================================
# Risk Controls
# ======================================================================

def _render_risk_controls(client, config: dict):
    """Risk per trade controls."""
    st.subheader("Risk pro Trade")

    trading_cfg = config.get("trading", {})
    limits = trading_cfg.get("limits", {})
    capital = trading_cfg.get("capital_usd", 100.0)
    current_pct = limits.get("max_position_pct", 5)
    current_max_usd = capital * current_pct / 100
    min_edge = limits.get("min_edge", 0.03)
    max_daily_loss = limits.get("max_daily_loss_usd", 50.0)

    col_cap, col_pct, col_usd, col_edge = st.columns(4)
    with col_cap:
        st.metric("Kapital", f"${capital:.0f}")
    with col_pct:
        st.metric("Max Position", f"{current_pct}%")
    with col_usd:
        st.metric("Max pro Trade", f"${current_max_usd:.2f}")
    with col_edge:
        st.metric("Min Edge", f"{min_edge:.0%}")

    with st.expander("Anpassen", expanded=False):
        col_left, col_right = st.columns(2)

        with col_left:
            new_capital = st.number_input(
                "Trading-Kapital ($)",
                min_value=10.0, max_value=10000.0,
                value=float(capital), step=10.0,
                key="risk_capital",
            )
            new_pct = st.slider(
                "Max Position (%)",
                min_value=1, max_value=25, value=int(current_pct),
                key="risk_pct",
            )
            new_max_usd = new_capital * new_pct / 100
            st.caption(f"= max **${new_max_usd:.2f}** pro Trade")

        with col_right:
            new_min_edge = st.slider(
                "Min Edge (%)",
                min_value=1, max_value=20, value=int(min_edge * 100),
                key="risk_edge",
            )
            new_max_daily_loss = st.number_input(
                "Max Tagesverlust ($)",
                min_value=5.0, max_value=500.0,
                value=float(max_daily_loss), step=5.0,
                key="risk_daily_loss",
            )
            new_mode = st.selectbox(
                "Trading-Modus",
                ["paper", "semi-auto", "full-auto"],
                index=["paper", "semi-auto", "full-auto"].index(
                    trading_cfg.get("mode", "paper")
                ),
                key="risk_mode",
            )

        changed = (
            new_capital != capital
            or new_pct != current_pct
            or new_min_edge != int(min_edge * 100)
            or new_max_daily_loss != max_daily_loss
            or new_mode != trading_cfg.get("mode", "paper")
        )

        if changed and st.button("Speichern", type="primary", use_container_width=True):
            config["trading"]["capital_usd"] = new_capital
            config["trading"]["limits"]["max_position_pct"] = new_pct
            config["trading"]["limits"]["min_edge"] = new_min_edge / 100
            config["trading"]["limits"]["max_daily_loss_usd"] = new_max_daily_loss
            config["trading"]["mode"] = new_mode

            result = client.save_config(config)
            if result and result.get("ok"):
                st.success(f"Gespeichert! Modus: {new_mode}")
                st.rerun()
            else:
                st.error("Fehler beim Speichern.")


# ======================================================================
# Helper functions
# ======================================================================

def _calc_today_pnl(equity_curve: list, current_unrealized: float, current_realized: float) -> float:
    """Calculate today's PNL change from equity curve snapshots."""
    if not equity_curve:
        return 0.0
    today_str = datetime.utcnow().strftime("%Y-%m-%d")
    current_total = current_unrealized + current_realized
    # Find the earliest snapshot from today (or most recent before today)
    prev_total = 0.0
    for snap in equity_curve:
        snap_date = str(snap.get("snapshot_at", ""))[:10]
        snap_pnl = (snap.get("unrealized_pnl", 0) or 0) + (snap.get("realized_pnl", 0) or 0)
        if snap_date < today_str:
            prev_total = snap_pnl
        else:
            break
    return current_total - prev_total


def _filter_equity_curve(equity_curve: list, period: str, total_deposited: float = 0) -> pd.DataFrame:
    """Filter equity curve to daily portfolio value (like Polymarket).

    Uses equity_pnl (= positions_value + cash - deposited) from snapshots,
    which is the ground-truth calculated from on-chain USDC balance.
    Skips snapshots where equity_pnl is NULL (old data before fix).
    """
    if not equity_curve:
        return pd.DataFrame()

    now = datetime.utcnow()
    if period == "1D":
        cutoff = now - timedelta(days=1)
    elif period == "1W":
        cutoff = now - timedelta(weeks=1)
    elif period == "1M":
        cutoff = now - timedelta(days=30)
    else:
        cutoff = None

    all_rows = []
    for snap in equity_curve:
        snap_at = snap.get("snapshot_at", "")
        try:
            dt = datetime.fromisoformat(str(snap_at))
        except (ValueError, TypeError):
            continue
        if cutoff and dt < cutoff:
            continue

        equity_pnl = snap.get("equity_pnl")
        if equity_pnl is not None:
            # Best source: real on-chain equity PnL
            portfolio_value = total_deposited + float(equity_pnl)
        else:
            # Skip snapshots without equity_pnl (unreliable)
            continue
        all_rows.append({"date": dt, "day": dt.strftime("%Y-%m-%d"), "value": portfolio_value})

    if not all_rows:
        return pd.DataFrame()

    # One point per day (last snapshot of each day)
    df = pd.DataFrame(all_rows)
    df = df.sort_values("date")
    daily = df.groupby("day").last().reset_index()
    daily["date"] = pd.to_datetime(daily["day"])
    return daily[["date", "value"]]


def _build_equity_chart(df: pd.DataFrame, color: str, total_deposited: float = 0):
    """Render equity curve as portfolio value line (like Polymarket)."""
    import plotly.graph_objects as go

    fig = go.Figure()
    # Deposit baseline
    fig.add_hline(
        y=total_deposited, line_dash="dot",
        line_color="rgba(136,146,164,0.3)", line_width=1,
    )
    # Portfolio value line
    fig.add_trace(go.Scatter(
        x=df["date"], y=df["value"],
        mode="lines",
        line=dict(color=color, width=2),
        hovertemplate="$%{y:,.2f}<extra></extra>",
    ))
    fig.update_layout(
        height=100,
        margin=dict(l=0, r=0, t=0, b=0),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        showlegend=False,
        hovermode="x unified",
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
