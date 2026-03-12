"""
Strategies Page — Manage and monitor trading strategies.
Shows strategy list, backtest results, approve/retire actions.
"""

import json
import streamlit as st
from services.bot_api_client import get_bot_client

# Status → emoji/label mapping for expander titles (no HTML)
STATUS_EMOJI = {
    "draft": ("📝", "Draft"),
    "pending_backtest": ("⏳", "Pending Backtest"),
    "backtested": ("🔬", "Backtested"),
    "validated": ("✅", "Validated"),
    "active": ("🟢", "Active"),
    "retired": ("⏸️", "Retired"),
    "rejected": ("❌", "Rejected"),
}

# Status → color for HTML badges (used in markdown sections)
STATUS_COLORS = {
    "draft": "#8892A4",
    "pending_backtest": "#FFB74D",
    "backtested": "#448AFF",
    "validated": "#AB47BC",
    "active": "#00D4AA",
    "retired": "#5A6478",
    "rejected": "#FF5252",
}


def _status_label(status: str) -> str:
    """Plain text status label for expander titles."""
    emoji, label = STATUS_EMOJI.get(status, ("❓", status))
    return f"{emoji} {label}"


def _metric_card(label: str, value: str, color: str = "#E8ECF1") -> str:
    return f"""
    <div style="background:linear-gradient(135deg,#1A1F2E,#1E2538); border:1px solid rgba(0,212,170,0.12);
                border-radius:10px; padding:14px 18px; text-align:center;">
        <div style="color:#5A6478; font-size:0.7rem; text-transform:uppercase; letter-spacing:0.05em;">{label}</div>
        <div style="color:{color}; font-size:1.4rem; font-weight:700; margin-top:4px;">{value}</div>
    </div>"""


_OP_LABELS = {"gt": ">", "lt": "<", "gte": ">=", "lte": "<=", "eq": "="}
_OP_OPTIONS = ["gt", "lt", "gte", "lte", "eq"]
_FIELD_LABELS = {
    "yes_price": "YES Preis",
    "no_price": "NO Preis",
    "volume": "Volumen ($)",
    "liquidity": "Liquiditaet ($)",
    "calculated_edge": "Edge",
    "sentiment_score": "Sentiment",
    "days_to_expiry": "Tage bis Ablauf",
    "spread": "Spread",
    "whale_net_flow": "Whale Net Flow",
    "top_holder_concentration": "Top Holder Konz.",
    "open_interest": "Open Interest",
    "smart_money_score": "Smart Money Score",
}
_SIZING_LABELS = {"kelly": "Kelly-Formel", "fixed_pct": "Fester Prozentsatz", "fixed": "Fester Betrag"}


def _render_strategy_details(client, sid: str, name: str, definition: dict, strat: dict):
    """Render readable, editable strategy parameters inline per strategy."""
    entry_rules = definition.get("entry_rules", [])
    exit_rules = definition.get("exit_rules", [])
    trade_params = definition.get("trade_params", {})
    source = definition.get("source", "")
    name_lower = name.lower()

    # Detect strategy type for showing relevant global config
    is_weather = "weather" in name_lower or "wetter" in name_lower

    # Load global config for global params
    config = client.get_config()
    trading_cfg = config.get("trading", {})
    sched_cfg = config.get("scheduler", {})

    with st.expander("Parameter bearbeiten", expanded=False):
        changed = False
        config_changed = False

        # --- Strategy info header ---
        if is_weather:
            st.caption("Datenquelle: Open-Meteo API (kostenlos, kein API-Key) | 16-Tage Vorhersage | Unsicherheit: Tag 1 +-1C, Tag 7 +-3C, Tag 14 +-4C")

        if source == "pattern_scanner":
            st.caption(f"Quelle: Pattern Scanner | Regel: `{definition.get('rule', '?')}`")

        # --- Entry Rules ---
        st.markdown("**Einstiegsregeln**")
        new_entry_rules = []
        for i, rule in enumerate(entry_rules):
            c1, c2, c3, c4 = st.columns([3, 1.5, 2, 0.5])
            field = rule.get("field", "")
            op = rule.get("op", "gt")
            value = rule.get("value", 0)

            with c1:
                new_field = st.selectbox(
                    "Feld", list(_FIELD_LABELS.keys()),
                    index=list(_FIELD_LABELS.keys()).index(field) if field in _FIELD_LABELS else 0,
                    format_func=lambda x: _FIELD_LABELS.get(x, x),
                    key=f"ef_{sid}_{i}", label_visibility="collapsed",
                )
            with c2:
                new_op = st.selectbox(
                    "Op", _OP_OPTIONS,
                    index=_OP_OPTIONS.index(op) if op in _OP_OPTIONS else 0,
                    format_func=lambda x: _OP_LABELS.get(x, x),
                    key=f"eo_{sid}_{i}", label_visibility="collapsed",
                )
            with c3:
                new_val = st.number_input(
                    "Wert", value=float(value), step=0.01, format="%.4f",
                    key=f"ev_{sid}_{i}", label_visibility="collapsed",
                )
            with c4:
                remove = st.button("X", key=f"er_{sid}_{i}")

            if not remove:
                new_rule = {"field": new_field, "op": new_op, "value": new_val}
                new_entry_rules.append(new_rule)
                if new_rule != rule:
                    changed = True
            else:
                changed = True

        if st.button("+ Regel hinzufuegen", key=f"add_entry_{sid}"):
            new_entry_rules.append({"field": "calculated_edge", "op": "gte", "value": 0.05})
            changed = True

        # --- Exit Rules ---
        if exit_rules:
            st.markdown("**Ausstiegsregeln**")
            new_exit_rules = []
            for i, rule in enumerate(exit_rules):
                c1, c2, c3 = st.columns([3, 1.5, 2])
                with c1:
                    new_field = st.selectbox(
                        "Feld", list(_FIELD_LABELS.keys()),
                        index=list(_FIELD_LABELS.keys()).index(rule.get("field", "")) if rule.get("field", "") in _FIELD_LABELS else 0,
                        format_func=lambda x: _FIELD_LABELS.get(x, x),
                        key=f"xf_{sid}_{i}", label_visibility="collapsed",
                    )
                with c2:
                    new_op = st.selectbox(
                        "Op", _OP_OPTIONS,
                        index=_OP_OPTIONS.index(rule.get("op", "lt")) if rule.get("op", "lt") in _OP_OPTIONS else 0,
                        format_func=lambda x: _OP_LABELS.get(x, x),
                        key=f"xo_{sid}_{i}", label_visibility="collapsed",
                    )
                with c3:
                    new_val = st.number_input(
                        "Wert", value=float(rule.get("value", 0)), step=0.01, format="%.4f",
                        key=f"xv_{sid}_{i}", label_visibility="collapsed",
                    )
                new_exit_rule = {"field": new_field, "op": new_op, "value": new_val}
                new_exit_rules.append(new_exit_rule)
                if new_exit_rule != rule:
                    changed = True
        else:
            new_exit_rules = exit_rules

        st.markdown("---")

        # --- Trade Parameters ---
        st.markdown("**Trade-Parameter**")
        tp1, tp2, tp3 = st.columns(3)
        with tp1:
            sizing = st.selectbox(
                "Positionsgroesse",
                list(_SIZING_LABELS.keys()),
                index=list(_SIZING_LABELS.keys()).index(trade_params.get("sizing_method", "kelly")) if trade_params.get("sizing_method", "kelly") in _SIZING_LABELS else 0,
                format_func=lambda x: _SIZING_LABELS.get(x, x),
                key=f"tp_sizing_{sid}",
            )
        with tp2:
            max_pos = st.number_input(
                "Max Position % (vom Kapital pro Trade)",
                value=float(trade_params.get("max_position_pct", 5)),
                min_value=0.5, max_value=50.0, step=0.5,
                key=f"tp_maxpos_{sid}",
                help=f"Bei ${trading_cfg.get('capital_usd', 100):.0f} Kapital = max ${trading_cfg.get('capital_usd', 100) * float(trade_params.get('max_position_pct', 5)) / 100:.2f} pro Trade",
            )
        with tp3:
            min_edge = st.number_input(
                "Min Edge % (Einstieg)",
                value=float(trade_params.get("min_edge", 0.03)) * 100,
                min_value=0.0, max_value=50.0, step=0.5,
                key=f"tp_minedge_{sid}",
                help="Minimaler Edge um einen Trade zu eroeffnen",
            )

        new_trade_params = {
            **trade_params,
            "sizing_method": sizing,
            "max_position_pct": max_pos,
            "min_edge": round(min_edge / 100, 4),
        }
        if new_trade_params != trade_params:
            changed = True

        st.markdown("---")

        # --- Global Execution Config (per strategy context) ---
        st.markdown("**Ausfuehrung & Cashout**")
        gc1, gc2, gc3 = st.columns(3)

        # Analyse-Intervall (from scheduler config)
        if is_weather:
            sched_key = "weather_edge"
            default_interval = 30
        else:
            sched_key = "strategy_evaluation"
            default_interval = 15

        with gc1:
            analyse_interval = st.number_input(
                "Analyse-Intervall (Min)",
                value=int(sched_cfg.get(sched_key, {}).get("interval_minutes", default_interval)),
                min_value=5, max_value=120, step=5,
                key=f"gc_interval_{sid}",
                help="Wie oft wird nach passenden Maerkten gesucht (API-Anfragen, kostenlos)",
            )

        with gc2:
            cashout_min_profit = st.number_input(
                "Cashout ab Profit % (Minimum)",
                value=float(trading_cfg.get("cashout", {}).get("min_profit_pct", 10)),
                min_value=1.0, max_value=200.0, step=1.0,
                key=f"gc_cashout_{sid}",
                help="Position wird verkauft wenn Profit >= diesem Wert. Hoehere Profite werden auch gecashoutet.",
            )

        with gc3:
            cashout_min_usd = st.number_input(
                "Cashout ab Profit $ (Minimum)",
                value=float(trading_cfg.get("cashout", {}).get("min_profit_usd", 0.50)),
                min_value=0.10, max_value=50.0, step=0.10,
                key=f"gc_cashout_usd_{sid}",
                help="Zusaetzliche Bedingung: Mindestprofit in Dollar",
            )

        # Weather-specific: min edge for weather strategy
        if is_weather:
            st.markdown("---")
            st.markdown("**Wetter-spezifisch**")
            wc1, wc2 = st.columns(2)
            with wc1:
                weather_min_edge = st.number_input(
                    "Weather Min Edge % (Einstieg)",
                    value=float(trading_cfg.get("weather_min_edge", 0.15)) * 100,
                    min_value=1.0, max_value=50.0, step=1.0,
                    key=f"wc_minedge_{sid}",
                    help="Minimaler Edge zwischen Wettervorhersage und Polymarket-Preis fuer einen Trade",
                )
            with wc2:
                st.metric("Forecast-Reichweite", "16 Tage")

        # Detect global config changes
        orig_interval = int(sched_cfg.get(sched_key, {}).get("interval_minutes", default_interval))
        orig_cashout_pct = float(trading_cfg.get("cashout", {}).get("min_profit_pct", 10))
        orig_cashout_usd = float(trading_cfg.get("cashout", {}).get("min_profit_usd", 0.50))
        if (analyse_interval != orig_interval or cashout_min_profit != orig_cashout_pct
                or cashout_min_usd != orig_cashout_usd):
            config_changed = True

        if is_weather:
            orig_weather_edge = float(trading_cfg.get("weather_min_edge", 0.15)) * 100
            if weather_min_edge != orig_weather_edge:
                config_changed = True

        # --- Save button ---
        if changed or config_changed:
            if st.button("Aenderungen speichern", key=f"save_{sid}", type="primary", use_container_width=True):
                saved_ok = True

                # Save strategy definition changes
                if changed:
                    update_data = {
                        "entry_rules": new_entry_rules,
                        "trade_params": new_trade_params,
                    }
                    if new_exit_rules != exit_rules:
                        update_data["exit_rules"] = new_exit_rules
                    result = client.update_strategy(sid, update_data)
                    if not result:
                        saved_ok = False

                # Save global config changes
                if config_changed:
                    config.setdefault("scheduler", {}).setdefault(sched_key, {})["interval_minutes"] = analyse_interval
                    config.setdefault("trading", {}).setdefault("cashout", {})["min_profit_pct"] = cashout_min_profit
                    config["trading"]["cashout"]["min_profit_usd"] = cashout_min_usd
                    if is_weather:
                        config["trading"]["weather_min_edge"] = round(weather_min_edge / 100, 4)
                    result = client.save_config(config)
                    if not result:
                        saved_ok = False

                if saved_ok:
                    st.success("Gespeichert! Wirkt ab naechstem Zyklus.")
                    st.rerun()
                else:
                    st.error("Fehler beim Speichern")


def render():
    st.title("Strategien")
    client = get_bot_client()

    # --- Filter bar ---
    col_filter, col_refresh = st.columns([4, 1])
    with col_filter:
        status_filter = st.selectbox(
            "Status-Filter",
            ["Alle", "draft", "pending_backtest", "backtested", "validated", "active", "retired", "rejected"],
            index=0,
            label_visibility="collapsed",
        )
    with col_refresh:
        if st.button("Aktualisieren", use_container_width=True):
            st.rerun()

    # --- Load strategies ---
    filter_val = None if status_filter == "Alle" else status_filter
    strategies = client.get_strategies(status=filter_val)

    if not strategies:
        st.info("Keine Strategien gefunden. Der Strategy Agent wird automatisch neue Strategien entdecken.")
        return

    # --- Summary metrics ---
    active = [s for s in strategies if s.get("status") == "active"]
    total_live_pnl = sum(s.get("live_pnl", 0) or 0 for s in strategies)
    avg_confidence = (
        sum(s.get("confidence_score", 0) or 0 for s in strategies) / len(strategies)
        if strategies else 0
    )
    best_wr = max((s.get("backtest_win_rate", 0) or 0 for s in strategies), default=0)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(_metric_card("Gesamt", str(len(strategies))), unsafe_allow_html=True)
    with c2:
        st.markdown(_metric_card("Aktiv", str(len(active)), "#00D4AA"), unsafe_allow_html=True)
    with c3:
        pnl_color = "#00D4AA" if total_live_pnl >= 0 else "#FF5252"
        st.markdown(_metric_card("Live PnL", f"${total_live_pnl:.2f}", pnl_color), unsafe_allow_html=True)
    with c4:
        st.markdown(_metric_card("Avg Confidence", f"{avg_confidence:.0%}"), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # --- Strategy list ---
    for strat in strategies:
        sid = strat.get("id", "?")
        name = strat.get("name", "Unnamed")
        status = strat.get("status", "draft")
        confidence = strat.get("confidence_score", 0) or 0
        bt_wr = strat.get("backtest_win_rate", 0) or 0
        bt_pnl = strat.get("backtest_pnl", 0) or 0
        bt_sharpe = strat.get("backtest_sharpe", 0) or 0
        bt_dd = strat.get("backtest_max_dd", 0) or 0
        live_pnl = strat.get("live_pnl", 0) or 0
        live_trades = strat.get("live_trades", 0) or 0
        live_wr = strat.get("live_win_rate", 0) or 0
        category = strat.get("category", "")
        discovered_by = strat.get("discovered_by", "")
        description = strat.get("description", "")

        with st.expander(f"**{name}**  —  {_status_label(status)}", expanded=False):
            # Description
            if description:
                st.markdown(f"*{description}*")

            # Meta row
            meta_parts = []
            if category:
                meta_parts.append(f"Kategorie: **{category}**")
            if discovered_by:
                meta_parts.append(f"Entdeckt von: **{discovered_by}**")
            meta_parts.append(f"ID: `{sid[:12]}...`")
            st.markdown(" | ".join(meta_parts))

            st.markdown("---")

            # Backtest vs Live metrics
            col_bt, col_live = st.columns(2)

            with col_bt:
                st.markdown("##### Backtest")
                m1, m2 = st.columns(2)
                with m1:
                    wr_color = "#00D4AA" if bt_wr >= 0.5 else "#FFB74D" if bt_wr >= 0.4 else "#FF5252"
                    st.markdown(_metric_card("Win Rate", f"{bt_wr:.0%}", wr_color), unsafe_allow_html=True)
                with m2:
                    pnl_c = "#00D4AA" if bt_pnl >= 0 else "#FF5252"
                    st.markdown(_metric_card("PnL", f"${bt_pnl:.2f}", pnl_c), unsafe_allow_html=True)
                m3, m4 = st.columns(2)
                with m3:
                    sh_c = "#00D4AA" if bt_sharpe >= 0.5 else "#FFB74D"
                    st.markdown(_metric_card("Sharpe", f"{bt_sharpe:.2f}", sh_c), unsafe_allow_html=True)
                with m4:
                    dd_c = "#FF5252" if bt_dd > 20 else "#FFB74D" if bt_dd > 10 else "#00D4AA"
                    st.markdown(_metric_card("Max DD", f"{bt_dd:.1f}%", dd_c), unsafe_allow_html=True)

            with col_live:
                st.markdown("##### Live Performance")
                m5, m6 = st.columns(2)
                with m5:
                    st.markdown(_metric_card("Trades", str(live_trades)), unsafe_allow_html=True)
                with m6:
                    lwr_c = "#00D4AA" if live_wr >= 0.5 else "#FFB74D" if live_wr >= 0.4 else "#FF5252"
                    st.markdown(_metric_card("Win Rate", f"{live_wr:.0%}", lwr_c), unsafe_allow_html=True)
                m7, m8 = st.columns(2)
                with m7:
                    lpnl_c = "#00D4AA" if live_pnl >= 0 else "#FF5252"
                    st.markdown(_metric_card("PnL", f"${live_pnl:.2f}", lpnl_c), unsafe_allow_html=True)
                with m8:
                    conf_c = "#00D4AA" if confidence >= 0.6 else "#FFB74D" if confidence >= 0.4 else "#FF5252"
                    st.markdown(_metric_card("Confidence", f"{confidence:.0%}", conf_c), unsafe_allow_html=True)

            # Strategy definition — readable + editable
            definition_raw = strat.get("definition", "{}")
            try:
                definition = json.loads(definition_raw) if isinstance(definition_raw, str) else definition_raw
            except (json.JSONDecodeError, TypeError):
                definition = {}

            if definition:
                _render_strategy_details(client, sid, name, definition, strat)

            st.markdown("---")

            # Action buttons
            btn_cols = st.columns(5)

            with btn_cols[0]:
                if status in ("backtested", "validated") and st.button(
                    "Aktivieren", key=f"activate_{sid}", use_container_width=True
                ):
                    result = client.update_strategy_status(sid, "active")
                    if result:
                        st.success("Strategie aktiviert!")
                        st.rerun()
                    else:
                        st.error("Fehler beim Aktivieren")

            with btn_cols[1]:
                if status == "active" and st.button(
                    "Pausieren", key=f"retire_{sid}", use_container_width=True
                ):
                    result = client.update_strategy_status(sid, "retired")
                    if result:
                        st.success("Strategie pausiert!")
                        st.rerun()
                    else:
                        st.error("Fehler beim Pausieren")

            with btn_cols[2]:
                if status in ("draft", "backtested", "rejected") and st.button(
                    "Backtest", key=f"backtest_{sid}", use_container_width=True
                ):
                    with st.spinner("Backtest wird ausgefuehrt..."):
                        result = client.run_backtest(sid)
                    if result:
                        st.success(f"Backtest abgeschlossen! Confidence: {result.get('confidence_score', 0):.0%}")
                        st.rerun()
                    else:
                        st.error("Backtest fehlgeschlagen")

            with btn_cols[3]:
                if status in ("backtested", "validated") and st.button(
                    "Ablehnen", key=f"reject_{sid}", use_container_width=True
                ):
                    result = client.update_strategy_status(sid, "rejected")
                    if result:
                        st.warning("Strategie abgelehnt.")
                        st.rerun()
                    else:
                        st.error("Fehler beim Ablehnen")

            with btn_cols[4]:
                if status in ("draft", "retired", "rejected") and st.button(
                    "Loeschen", key=f"delete_{sid}", use_container_width=True
                ):
                    result = client.delete_strategy(sid)
                    if result:
                        st.warning("Strategie geloescht.")
                        st.rerun()
                    else:
                        st.error("Fehler beim Loeschen")

    # --- Pattern Analysis section ---
    st.markdown("---")
    st.subheader("Pattern-Analyse")

    patterns = client.get_patterns()
    if patterns:
        tab_cat, tab_price, tab_vol, tab_side = st.tabs(
            ["Nach Kategorie", "Nach Preis", "Nach Volumen", "Nach Seite"]
        )

        with tab_cat:
            cat_data = patterns.get("by_category", [])
            if cat_data:
                for item in cat_data:
                    cat = item.get("category", "?")
                    wr = item.get("win_rate", 0) or 0
                    count = item.get("count", 0)
                    wr_color = "#00D4AA" if wr >= 0.5 else "#FFB74D"
                    st.markdown(
                        f'**{cat}** — Win Rate: <span style="color:{wr_color}">{wr:.0%}</span> ({count} trades)',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Noch keine Kategorie-Daten")

        with tab_price:
            price_data = patterns.get("by_price_bucket", [])
            if price_data:
                for item in price_data:
                    bucket = item.get("price_bucket", "?")
                    wr = item.get("win_rate", 0) or 0
                    count = item.get("count", 0)
                    wr_color = "#00D4AA" if wr >= 0.5 else "#FFB74D"
                    st.markdown(
                        f'**{bucket}** — Win Rate: <span style="color:{wr_color}">{wr:.0%}</span> ({count} trades)',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Noch keine Preis-Daten")

        with tab_vol:
            vol_data = patterns.get("by_volume_bucket", [])
            if vol_data:
                for item in vol_data:
                    bucket = item.get("volume_bucket", "?")
                    wr = item.get("win_rate", 0) or 0
                    count = item.get("count", 0)
                    wr_color = "#00D4AA" if wr >= 0.5 else "#FFB74D"
                    st.markdown(
                        f'**{bucket}** — Win Rate: <span style="color:{wr_color}">{wr:.0%}</span> ({count} trades)',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Noch keine Volumen-Daten")

        with tab_side:
            side_data = patterns.get("by_side", [])
            if side_data:
                for item in side_data:
                    side = item.get("side", "?")
                    wr = item.get("win_rate", 0) or 0
                    count = item.get("count", 0)
                    wr_color = "#00D4AA" if wr >= 0.5 else "#FFB74D"
                    st.markdown(
                        f'**{side}** — Win Rate: <span style="color:{wr_color}">{wr:.0%}</span> ({count} trades)',
                        unsafe_allow_html=True,
                    )
            else:
                st.info("Noch keine Side-Daten")
    else:
        st.info("Pattern-Analyse nicht verfuegbar. Daten werden mit der Zeit gesammelt.")

