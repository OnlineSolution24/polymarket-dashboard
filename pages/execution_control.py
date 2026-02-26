"""
Tab 8: Execution Control
Full trade execution via Polymarket API, circuit breaker,
trade result tracking, and statistics.
"""

import re
import streamlit as st
from datetime import datetime, timedelta

from db import engine
from config import load_platform_config, AppConfig
from components.tables import trades_table
from components.charts import pnl_chart


def render():
    st.header("Execution Control")

    # --- Circuit Breaker Status ---
    cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
    platform_cfg = load_platform_config()
    cb_config = platform_cfg.get("circuit_breaker", {})
    max_losses = cb_config.get("max_consecutive_losses", 3)

    col1, col2 = st.columns(2)
    with col1:
        consecutive_losses = cb["consecutive_losses"] if cb else 0
        is_paused = False
        if cb and cb.get("paused_until"):
            try:
                paused_until = datetime.fromisoformat(cb["paused_until"])
                if paused_until > datetime.utcnow():
                    is_paused = True
            except Exception:
                pass

        if is_paused:
            st.error(f"CIRCUIT BREAKER AKTIV — Pausiert bis {cb['paused_until'][:16]}")
        else:
            st.success("Trading erlaubt")
        st.caption(f"Verluste in Folge: {consecutive_losses}/{max_losses}")

    with col2:
        if is_paused and st.button("Circuit Breaker zurücksetzen"):
            engine.execute(
                "UPDATE circuit_breaker SET consecutive_losses = 0, paused_until = NULL, last_updated = ? WHERE id = 1",
                (datetime.utcnow().isoformat(),),
            )
            st.rerun()

    st.divider()

    # --- EXECUTE Command ---
    st.subheader("Trade ausführen")

    # Quick-select from markets
    markets = engine.query("SELECT id, question, yes_price, no_price FROM markets ORDER BY volume DESC LIMIT 20")
    if markets:
        market_options = {f"{m['question'][:60]} (YES:{m['yes_price']:.0%} NO:{m['no_price']:.0%})": m["id"] for m in markets}
        selected_label = st.selectbox("Markt auswählen", ["-- Manuell eingeben --"] + list(market_options.keys()))

        if selected_label != "-- Manuell eingeben --":
            selected_id = market_options[selected_label]
            col_s, col_a = st.columns(2)
            with col_s:
                side = st.radio("Seite", ["YES", "NO"], horizontal=True)
            with col_a:
                amount = st.number_input("Betrag (USD)", min_value=1.0, max_value=1000.0, value=10.0, step=5.0)
            if st.button("EXECUTE", type="primary", disabled=is_paused):
                result = _parse_and_execute(f"EXECUTE {selected_id} {side} {amount}")
                _show_result(result)
        else:
            _manual_execute(is_paused)
    else:
        _manual_execute(is_paused)

    st.divider()

    # --- Resolve Open Trades ---
    st.subheader("Offene Trades abschliessen")
    open_trades = engine.query(
        "SELECT * FROM trades WHERE status = 'executed' AND (result IS NULL OR result = 'open') ORDER BY created_at DESC"
    )
    if open_trades:
        for trade in open_trades:
            with st.expander(f"{trade['side']} ${trade['amount_usd']:.2f} — {trade.get('market_question') or trade['market_id']}"):
                c1, c2, c3 = st.columns(3)
                with c1:
                    if st.button("WIN", key=f"win_{trade['id']}", type="primary"):
                        _resolve_trade(trade["id"], "win", trade["amount_usd"] * 0.9)
                        st.rerun()
                with c2:
                    if st.button("LOSS", key=f"loss_{trade['id']}"):
                        _resolve_trade(trade["id"], "loss", -trade["amount_usd"])
                        st.rerun()
                with c3:
                    custom_pnl = st.number_input("PnL", key=f"pnl_{trade['id']}", value=0.0, step=1.0)
                    if st.button("Custom", key=f"cust_{trade['id']}"):
                        _resolve_trade(trade["id"], "win" if custom_pnl >= 0 else "loss", custom_pnl)
                        st.rerun()
    else:
        st.caption("Keine offenen Trades.")

    st.divider()

    # --- Trade Statistics ---
    st.subheader("Trade Verlauf")
    stats = engine.query_one("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN result='win' THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN result='loss' THEN 1 ELSE 0 END) as losses,
               COALESCE(SUM(pnl),0) as total_pnl
        FROM trades WHERE result IS NOT NULL
    """)
    if stats and stats["total"] > 0:
        sc = st.columns(4)
        with sc[0]:
            st.metric("Trades", stats["total"])
        with sc[1]:
            wr = stats["wins"] / stats["total"] * 100
            st.metric("Win Rate", f"{wr:.0f}%")
        with sc[2]:
            st.metric("W / L", f"{stats['wins']} / {stats['losses']}")
        with sc[3]:
            st.metric("PnL", f"${stats['total_pnl']:+.2f}")

    trades = engine.query("SELECT * FROM trades ORDER BY created_at DESC LIMIT 50")
    if trades:
        done = [t for t in trades if t.get("pnl") is not None]
        if done:
            st.plotly_chart(pnl_chart(done), use_container_width=True)
        trades_table(trades)
    else:
        st.info("Noch keine Trades.")


def _manual_execute(is_paused: bool):
    st.markdown("**Format:** `EXECUTE [Market-ID] [YES/NO] [Betrag]`")
    cmd = st.text_input("EXECUTE Kommando", placeholder="EXECUTE market_id YES 50")
    if st.button("Ausführen", type="primary", disabled=is_paused):
        if cmd:
            _show_result(_parse_and_execute(cmd))
        else:
            st.warning("Bitte Kommando eingeben.")


def _show_result(result: dict):
    if result["ok"]:
        st.success(result["message"])
    else:
        st.error(result["message"])


def _parse_and_execute(cmd: str) -> dict:
    """Parse EXECUTE command, validate, and execute."""
    pattern = r"^EXECUTE\s+(\S+)\s+(YES|NO)\s+(\d+(?:\.\d+)?)\s*$"
    match = re.match(pattern, cmd.strip(), re.IGNORECASE)
    if not match:
        return {"ok": False, "message": "Ungültiges Format. Nutze: EXECUTE [Market-ID] [YES/NO] [Betrag]"}

    market_id = match.group(1)
    side = match.group(2).upper()
    amount = float(match.group(3))

    if amount <= 0:
        return {"ok": False, "message": "Betrag muss > 0 sein."}
    if amount > 1000:
        return {"ok": False, "message": "Betrag über $1000 — Sicherheitsgrenze."}

    # Circuit breaker check
    cb = engine.query_one("SELECT * FROM circuit_breaker WHERE id = 1")
    if cb and cb.get("paused_until"):
        try:
            if datetime.fromisoformat(cb["paused_until"]) > datetime.utcnow():
                return {"ok": False, "message": "Circuit Breaker aktiv!"}
        except Exception:
            pass

    market = engine.query_one("SELECT * FROM markets WHERE id = ?", (market_id,))
    market_question = market["question"] if market else market_id

    # Attempt execution
    exec_result = _try_polymarket_execution(market_id, side, amount)

    now = datetime.utcnow().isoformat()
    status = "executed" if exec_result["executed"] else "pending"
    price = exec_result.get("price")

    engine.execute(
        """INSERT INTO trades (market_id, market_question, side, amount_usd, price, status, user_cmd, result, created_at, executed_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (market_id, market_question, side, amount, price, status, cmd,
         "open" if status == "executed" else None, now, now if status == "executed" else None),
    )

    # Alert
    try:
        from services.telegram_alerts import get_alerts
        alerts = get_alerts(AppConfig.from_env())
        alerts.alert_trade_executed(market_question[:60], side, amount)
    except Exception:
        pass

    if exec_result["executed"]:
        return {"ok": True, "message": f"Trade LIVE ausgeführt: {side} ${amount:.2f} auf {market_question}"}
    else:
        return {"ok": True, "message": f"Trade gespeichert (Pending): {side} ${amount:.2f} auf {market_question}. {exec_result.get('note', '')}"}


def _try_polymarket_execution(market_id: str, side: str, amount: float) -> dict:
    """Try real Polymarket trade. Returns {"executed": bool, "price": float|None}."""
    try:
        config = AppConfig.from_env()
        if not config.polymarket_private_key:
            return {"executed": False, "note": "Kein POLYMARKET_PRIVATE_KEY konfiguriert."}

        from services.polymarket_client import PolymarketService
        service = PolymarketService(config)
        result = service.place_market_order(market_id, amount, side)

        if "error" in result:
            return {"executed": False, "note": result["error"]}

        return {"executed": True, "price": result.get("result", {}).get("price")}
    except Exception as e:
        return {"executed": False, "note": str(e)}


def _resolve_trade(trade_id: int, result: str, pnl: float):
    """Mark trade as won/lost and update circuit breaker."""
    now = datetime.utcnow().isoformat()
    engine.execute(
        "UPDATE trades SET result = ?, pnl = ?, executed_at = COALESCE(executed_at, ?) WHERE id = ?",
        (result, pnl, now, trade_id),
    )

    if result == "loss":
        cb = engine.query_one("SELECT consecutive_losses FROM circuit_breaker WHERE id = 1")
        new_losses = (cb["consecutive_losses"] if cb else 0) + 1
        platform_cfg = load_platform_config()
        cb_cfg = platform_cfg.get("circuit_breaker", {})
        max_l = cb_cfg.get("max_consecutive_losses", 3)
        pause_h = cb_cfg.get("pause_hours", 24)

        paused_until = None
        if new_losses >= max_l:
            paused_until = (datetime.utcnow() + timedelta(hours=pause_h)).isoformat()
            try:
                from services.telegram_alerts import get_alerts
                alerts = get_alerts(AppConfig.from_env())
                alerts.alert_circuit_breaker(new_losses, paused_until)
            except Exception:
                pass

        engine.execute(
            "UPDATE circuit_breaker SET consecutive_losses = ?, paused_until = ?, last_updated = ? WHERE id = 1",
            (new_losses, paused_until, now),
        )
    elif result == "win":
        engine.execute(
            "UPDATE circuit_breaker SET consecutive_losses = 0, last_updated = ? WHERE id = 1",
            (now,),
        )
