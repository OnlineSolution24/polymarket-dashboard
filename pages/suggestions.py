"""
Suggestion System — View and respond to bot suggestions via REST API.
Chief agent runs on the bot. Dashboard shows suggestions and allows Yes/No responses.
"""

import json
import streamlit as st

from services.bot_api_client import get_bot_client


def render():
    st.header("Vorschläge")

    client = get_bot_client()

    st.caption("Der Chief Agent analysiert automatisch und erstellt Vorschläge. Hier kannst du sie genehmigen oder ablehnen.")

    st.divider()

    # --- Pending Suggestions ---
    pending = client.get_suggestions(status="pending")

    if pending:
        st.subheader(f"Offene Vorschläge ({len(pending)})")

        for sugg in pending:
            type_icons = {
                "trade": "📈", "new_agent": "🤖", "config_change": "⚙️",
                "alert": "⚠️", "analysis": "🔍", "risk_adjustment": "🛡️",
                "source_change": "📰",
            }
            icon = type_icons.get(sugg.get("type", ""), "💡")

            with st.expander(f"{icon} {sugg.get('title', 'Vorschlag')}", expanded=True):
                st.markdown(sugg.get("description") or "")

                if sugg.get("payload"):
                    try:
                        payload = json.loads(sugg["payload"]) if isinstance(sugg["payload"], str) else sugg["payload"]
                        with st.popover("Details anzeigen"):
                            st.json(payload)
                    except Exception:
                        pass

                st.caption(f"Von: {sugg.get('agent_id', '?')} | Typ: {sugg.get('type', '?')} | {(sugg.get('created_at') or '')[:16]}")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Ja (Genehmigen)", key=f"yes_{sugg['id']}", type="primary"):
                        result = client.respond_suggestion(sugg["id"], "approve")
                        if result and result.get("ok"):
                            st.success("Genehmigt!")
                            st.rerun()
                with col2:
                    if st.button("Nein (Ablehnen)", key=f"no_{sugg['id']}"):
                        result = client.respond_suggestion(sugg["id"], "reject")
                        if result and result.get("ok"):
                            st.info("Abgelehnt.")
                            st.rerun()
    else:
        st.info("Keine offenen Vorschläge. Der Chief Agent erstellt automatisch neue Vorschläge.")

    st.divider()

    # --- Suggestion Statistics ---
    all_suggestions = client.get_suggestions(limit=200)
    resolved = [s for s in all_suggestions if s.get("status") != "pending"]

    if resolved:
        approved = sum(1 for s in resolved if s.get("user_response") in ("yes", "approve"))
        rejected = sum(1 for s in resolved if s.get("user_response") in ("no", "reject"))
        testing = sum(1 for s in resolved if s.get("user_response") == "test")
        auto = sum(1 for s in resolved if s.get("status") == "auto_approved")

        sc = st.columns(4)
        with sc[0]:
            st.metric("Total", len(resolved))
        with sc[1]:
            st.metric("Genehmigt", approved + auto)
        with sc[2]:
            st.metric("Abgelehnt", rejected)
        with sc[3]:
            st.metric("Auto-Approved", auto)

    # --- History ---
    st.subheader("Verlauf")

    filter_type = st.selectbox("Filtern nach Typ", ["Alle", "trade", "new_agent", "config_change", "analysis", "risk_adjustment"])

    history = resolved
    if filter_type != "Alle":
        history = [s for s in resolved if s.get("type") == filter_type]

    if history:
        for sugg in history[:50]:
            response_icons = {"yes": "✅", "approve": "✅", "no": "❌", "reject": "❌", "test": "🧪"}
            icon = response_icons.get(sugg.get("user_response", ""), "•")
            if sugg.get("status") == "auto_approved":
                icon = "🤖"
            ts = (sugg.get("resolved_at") or sugg.get("created_at") or "")[:16]
            st.caption(f"{icon} **{sugg.get('title', '?')}** — {sugg.get('user_response', sugg.get('status', '?'))} ({ts})")
    else:
        st.caption("Kein Verlauf vorhanden.")
