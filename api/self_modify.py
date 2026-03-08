"""
Self-Modification API for AI agents.
Allows agents to propose code/config changes that require user approval.
Changes are tracked, backed up, syntax-checked, and rolled back on failure.
"""

import difflib
import logging
import os
import shutil
from datetime import datetime
from typing import Optional

from fastapi import Depends, HTTPException, Query
from pydantic import BaseModel

from api.auth import verify_api_key
from db import engine

logger = logging.getLogger(__name__)

# Directories the agent is allowed to modify (relative to /app/)
ALLOWED_PATHS = [
    "/app/agents/",
    "/app/agent_configs/",
    "/app/services/",
    "/app/strategies/",
    "/app/platform_config.yaml",
]

BACKUP_DIR = "/app/data/backups"


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class CodeChangeProposal(BaseModel):
    agent_id: str
    file_path: str
    new_code: str
    reason: str
    description: str = ""


class CodeChangeApproval(BaseModel):
    approved: bool
    user_comment: Optional[str] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_path_allowed(file_path: str) -> bool:
    """Check if the file path is within allowed directories."""
    normalized = os.path.normpath(file_path)
    return any(normalized.startswith(os.path.normpath(p)) for p in ALLOWED_PATHS)


def _make_diff(old: str, new: str, file_path: str) -> str:
    """Generate unified diff between old and new content."""
    return "".join(difflib.unified_diff(
        old.splitlines(keepends=True),
        new.splitlines(keepends=True),
        fromfile=f"a/{file_path}",
        tofile=f"b/{file_path}",
    ))


def _syntax_check(file_path: str) -> tuple[bool, str]:
    """Run Python syntax check on a file. Returns (ok, error_message)."""
    if not file_path.endswith(".py"):
        return True, ""
    import subprocess
    result = subprocess.run(
        ["python3", "-m", "py_compile", file_path],
        capture_output=True, text=True, timeout=10,
    )
    if result.returncode != 0:
        return False, result.stderr
    return True, ""


# ---------------------------------------------------------------------------
# Endpoint registration (called from rest_api.py create_app)
# ---------------------------------------------------------------------------

def register_self_modify_endpoints(app):
    """Register all self-modification endpoints on the FastAPI app."""

    @app.post("/api/code/propose", dependencies=[Depends(verify_api_key)])
    def propose_change(body: CodeChangeProposal):
        """
        Agent proposes a code/config change. Stored as 'pending' for user review.
        The change is NOT applied until explicitly approved.
        """
        if not _is_path_allowed(body.file_path):
            raise HTTPException(
                status_code=403,
                detail=f"Path not allowed. Permitted: {ALLOWED_PATHS}",
            )

        # Read current file content (empty if new file)
        old_code = ""
        if os.path.exists(body.file_path):
            with open(body.file_path, "r", encoding="utf-8") as f:
                old_code = f.read()

        diff = _make_diff(old_code, body.new_code, body.file_path)

        now = datetime.utcnow().isoformat()
        engine.execute(
            """INSERT INTO code_changes
               (agent_id, file_path, old_code, new_code, reason, description, diff_preview, status, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, 'pending', ?)""",
            (body.agent_id, body.file_path, old_code, body.new_code,
             body.reason, body.description, diff[:5000], now),
        )

        change_id = engine.query_one("SELECT MAX(id) as id FROM code_changes")["id"]

        logger.info(f"Code change #{change_id} proposed by {body.agent_id}: {body.file_path}")

        # Send Telegram alert
        try:
            from services.telegram_alerts import get_alerts
            alerts = get_alerts(app.state.config)
            alerts.send(
                f"🔧 <b>Code-Änderung vorgeschlagen</b>\n"
                f"Agent: {body.agent_id}\n"
                f"Datei: <code>{body.file_path}</code>\n"
                f"Grund: {body.reason}\n"
                f"ID: #{change_id}\n\n"
                f"Genehmigen via Dashboard oder API:\n"
                f"POST /api/code/{change_id}/approve"
            )
        except Exception:
            pass

        return {
            "change_id": change_id,
            "status": "pending",
            "diff_preview": diff[:2000],
        }

    @app.get("/api/code/pending", dependencies=[Depends(verify_api_key)])
    def get_pending_changes():
        """List all pending code change proposals."""
        rows = engine.query(
            """SELECT id, agent_id, file_path, reason, description, created_at
               FROM code_changes WHERE status = 'pending'
               ORDER BY created_at DESC""",
        )
        return {"pending": rows or []}

    @app.get("/api/code/history", dependencies=[Depends(verify_api_key)])
    def get_change_history(
        status: Optional[str] = None,
        limit: int = Query(50, ge=1, le=200),
    ):
        """List code change history with optional status filter."""
        if status:
            rows = engine.query(
                "SELECT id, agent_id, file_path, reason, status, created_at, resolved_at "
                "FROM code_changes WHERE status = ? ORDER BY created_at DESC LIMIT ?",
                (status, limit),
            )
        else:
            rows = engine.query(
                "SELECT id, agent_id, file_path, reason, status, created_at, resolved_at "
                "FROM code_changes ORDER BY created_at DESC LIMIT ?",
                (limit,),
            )
        return {"changes": rows or []}

    @app.get("/api/code/{change_id}", dependencies=[Depends(verify_api_key)])
    def get_change_details(change_id: int):
        """Get full details of a code change including diff."""
        row = engine.query_one(
            "SELECT * FROM code_changes WHERE id = ?", (change_id,)
        )
        if not row:
            raise HTTPException(status_code=404, detail="Change not found")

        # Regenerate diff for display
        diff = _make_diff(
            row.get("old_code", "") or "",
            row.get("new_code", "") or "",
            row.get("file_path", ""),
        )

        return {
            "id": row["id"],
            "agent_id": row["agent_id"],
            "file_path": row["file_path"],
            "reason": row["reason"],
            "description": row["description"],
            "status": row["status"],
            "diff": diff,
            "user_comment": row.get("user_comment"),
            "backup_path": row.get("backup_path"),
            "created_at": row["created_at"],
            "resolved_at": row.get("resolved_at"),
            "applied_at": row.get("applied_at"),
        }

    @app.post("/api/code/{change_id}/approve", dependencies=[Depends(verify_api_key)])
    def approve_or_reject_change(change_id: int, body: CodeChangeApproval):
        """
        Approve or reject a pending code change.
        On approval: backup original, apply change, syntax-check, rollback on failure.
        """
        row = engine.query_one(
            "SELECT * FROM code_changes WHERE id = ?", (change_id,)
        )
        if not row:
            raise HTTPException(status_code=404, detail="Change not found")
        if row["status"] != "pending":
            raise HTTPException(
                status_code=400,
                detail=f"Change already {row['status']}",
            )

        now = datetime.utcnow().isoformat()

        # --- REJECT ---
        if not body.approved:
            engine.execute(
                "UPDATE code_changes SET status = 'rejected', user_comment = ?, resolved_at = ? WHERE id = ?",
                (body.user_comment, now, change_id),
            )
            logger.info(f"Code change #{change_id} rejected")
            return {"ok": True, "status": "rejected", "change_id": change_id}

        # --- APPROVE: backup, apply, verify ---
        file_path = row["file_path"]
        new_code = row["new_code"]
        backup_path = None

        # Create backup
        if os.path.exists(file_path):
            os.makedirs(BACKUP_DIR, exist_ok=True)
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            basename = os.path.basename(file_path)
            backup_path = os.path.join(BACKUP_DIR, f"{basename}.{change_id}.{ts}.bak")
            shutil.copy2(file_path, backup_path)
            logger.info(f"Backup created: {backup_path}")

        try:
            # Write new code
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(new_code)

            # Syntax check for Python files
            ok, err = _syntax_check(file_path)
            if not ok:
                # Rollback
                if backup_path and os.path.exists(backup_path):
                    shutil.copy2(backup_path, file_path)
                engine.execute(
                    "UPDATE code_changes SET status = 'failed', user_comment = ?, resolved_at = ? WHERE id = ?",
                    (f"Syntax error: {err[:500]}", now, change_id),
                )
                logger.error(f"Code change #{change_id} failed syntax check: {err[:200]}")
                raise HTTPException(
                    status_code=400,
                    detail=f"Syntax error, change rolled back: {err[:300]}",
                )

            # Success
            engine.execute(
                """UPDATE code_changes
                   SET status = 'applied', user_comment = ?, backup_path = ?,
                       resolved_at = ?, applied_at = ?
                   WHERE id = ?""",
                (body.user_comment, backup_path, now, now, change_id),
            )

            logger.info(f"Code change #{change_id} applied to {file_path}")

            # Notify via Telegram
            try:
                from services.telegram_alerts import get_alerts
                alerts = get_alerts(app.state.config)
                alerts.send(
                    f"✅ <b>Code-Änderung angewendet</b>\n"
                    f"ID: #{change_id}\n"
                    f"Datei: <code>{file_path}</code>\n"
                    f"Grund: {row['reason']}"
                )
            except Exception:
                pass

            return {
                "ok": True,
                "status": "applied",
                "change_id": change_id,
                "backup": backup_path,
                "message": "Change applied. Restart scheduler with POST /api/scheduler/reload if needed.",
            }

        except HTTPException:
            raise
        except Exception as e:
            # Rollback on any error
            if backup_path and os.path.exists(backup_path):
                shutil.copy2(backup_path, file_path)
            engine.execute(
                "UPDATE code_changes SET status = 'failed', user_comment = ?, resolved_at = ? WHERE id = ?",
                (str(e)[:500], now, change_id),
            )
            logger.error(f"Code change #{change_id} failed: {e}")
            raise HTTPException(status_code=500, detail=f"Apply failed, rolled back: {e}")

    @app.post("/api/code/{change_id}/rollback", dependencies=[Depends(verify_api_key)])
    def rollback_change(change_id: int):
        """Rollback a previously applied change using its backup."""
        row = engine.query_one(
            "SELECT * FROM code_changes WHERE id = ?", (change_id,)
        )
        if not row:
            raise HTTPException(status_code=404, detail="Change not found")
        if row["status"] != "applied":
            raise HTTPException(status_code=400, detail="Can only rollback applied changes")

        backup_path = row.get("backup_path")
        if not backup_path or not os.path.exists(backup_path):
            raise HTTPException(status_code=400, detail="Backup file not found")

        file_path = row["file_path"]
        shutil.copy2(backup_path, file_path)

        now = datetime.utcnow().isoformat()
        engine.execute(
            "UPDATE code_changes SET status = 'rolled_back', resolved_at = ? WHERE id = ?",
            (now, change_id),
        )

        logger.info(f"Code change #{change_id} rolled back from {backup_path}")
        return {"ok": True, "status": "rolled_back", "change_id": change_id}
