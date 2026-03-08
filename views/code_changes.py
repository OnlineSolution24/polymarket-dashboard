"""
Code Changes — View and approve/reject AI-proposed code modifications.
"""

import streamlit as st

from services.bot_api_client import get_bot_client


def render():
    st.header("Code-Anderungen")

    client = get_bot_client()

    st.caption("AI-Agents konnen Code- und Config-Anderungen vorschlagen. Hier kannst du sie prufen, genehmigen oder ablehnen.")

    st.divider()

    # --- Pending Changes ---
    pending = client.get_pending_code_changes()

    if pending:
        st.subheader(f"Wartende Anderungen ({len(pending)})")

        for change in pending:
            cid = change["id"]
            with st.expander(f"#{cid} — {change.get('reason', 'Keine Beschreibung')}", expanded=True):
                st.markdown(f"**Datei:** `{change.get('file_path', '?')}`")
                if change.get("description"):
                    st.markdown(change["description"])

                st.caption(f"Agent: {change.get('agent_id', '?')} | {(change.get('created_at') or '')[:16]}")

                # Load full details for diff
                details = client.get_code_change_details(cid)
                if details and details.get("diff"):
                    with st.expander("Diff anzeigen", expanded=False):
                        st.code(details["diff"], language="diff")

                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Genehmigen", key=f"approve_{cid}", type="primary", use_container_width=True):
                        result = client.approve_code_change(cid, approved=True, comment="Approved via Dashboard")
                        if result and result.get("ok"):
                            st.success(f"Angewendet! Backup: {result.get('backup', 'keins')}")
                            st.rerun()
                        else:
                            detail = (result or {}).get("detail", "Unbekannter Fehler")
                            st.error(f"Fehler: {detail}")
                with col2:
                    if st.button("Ablehnen", key=f"reject_{cid}", use_container_width=True):
                        result = client.approve_code_change(cid, approved=False, comment="Rejected via Dashboard")
                        if result and result.get("ok"):
                            st.info("Abgelehnt.")
                            st.rerun()
    else:
        st.info("Keine wartenden Code-Anderungen.")

    st.divider()

    # --- History ---
    st.subheader("Verlauf")

    filter_status = st.selectbox("Status", ["Alle", "applied", "rejected", "failed", "rolled_back"])

    status_filter = None if filter_status == "Alle" else filter_status
    history = client.get_code_change_history(status=status_filter, limit=50)

    if history:
        status_icons = {
            "applied": "✅",
            "rejected": "❌",
            "failed": "💥",
            "rolled_back": "↩️",
            "pending": "⏳",
        }

        for change in history:
            cid = change["id"]
            icon = status_icons.get(change.get("status", ""), "•")
            ts = (change.get("resolved_at") or change.get("created_at") or "")[:16]
            reason = change.get("reason", "?")[:80]

            with st.expander(f"{icon} #{cid} — {reason} ({ts})", expanded=False):
                st.markdown(f"**Datei:** `{change.get('file_path', '?')}`")
                st.markdown(f"**Status:** {change.get('status', '?')} | **Agent:** {change.get('agent_id', '?')}")

                # Load details on demand
                details = client.get_code_change_details(cid)
                if details:
                    if details.get("diff"):
                        st.code(details["diff"], language="diff")
                    if details.get("user_comment"):
                        st.caption(f"Kommentar: {details['user_comment']}")

                # Rollback button for applied changes
                if change.get("status") == "applied":
                    if st.button("Rollback", key=f"rollback_{cid}", use_container_width=True):
                        result = client.rollback_code_change(cid)
                        if result and result.get("ok"):
                            st.warning("Rollback durchgefuhrt!")
                            st.rerun()
                        else:
                            st.error("Rollback fehlgeschlagen.")
    else:
        st.caption("Kein Verlauf vorhanden.")
