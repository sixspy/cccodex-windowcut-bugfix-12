"""审计与互证只读旁路。"""
from __future__ import annotations

from fastapi import APIRouter

from auth import AuthHeader, require_user
from db import connect

router = APIRouter(tags=["audit"])


@router.get("/api/audit")
def list_audit(authorization: AuthHeader = None, limit: int = 100):
    require_user(authorization)
    limit = max(1, min(limit, 500))
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM audit_trail ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/audit/actions")
def audit_actions(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            """SELECT action, COUNT(*) c FROM audit_trail
               GROUP BY action ORDER BY c DESC"""
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/audit/reject-fate")
def reject_fate(authorization: AuthHeader = None):
    """拒收同命旁路：events 拒收行与 reject_slips 凭证同源核对。

    - shadow_events：有拒收事件、无凭证（双影/单边）
    - orphan_slips：有凭证、无拒收事件（孤儿凭证）
    """
    require_user(authorization)
    with connect() as conn:
        reject_events = conn.execute(
            "SELECT COUNT(*) c FROM events WHERE disposition='拒收'"
        ).fetchone()["c"]
        reject_slips = conn.execute(
            "SELECT COUNT(*) c FROM reject_slips"
        ).fetchone()["c"]
        shadows = conn.execute(
            """SELECT e.biz_key, e.event_ts, e.amount, e.created_at
               FROM events e
               WHERE e.disposition='拒收'
                 AND NOT EXISTS (
                     SELECT 1 FROM reject_slips s WHERE s.biz_key=e.biz_key)"""
        ).fetchall()
        orphans = conn.execute(
            """SELECT s.biz_key, s.window_name, s.event_ts, s.amount, s.created_at
               FROM reject_slips s
               WHERE NOT EXISTS (
                   SELECT 1 FROM events e
                   WHERE e.disposition='拒收' AND e.biz_key=s.biz_key)"""
        ).fetchall()
        contentions = conn.execute(
            "SELECT COUNT(*) c FROM contention_log WHERE lock_key LIKE 'ingest:%'"
        ).fetchone()["c"]
        return {
            "reject_events": reject_events,
            "reject_slips": reject_slips,
            "shadow_events": len(shadows),
            "orphan_slips": len(orphans),
            "shadow_detail": [dict(r) for r in shadows],
            "orphan_detail": [dict(r) for r in orphans],
            "ingest_contentions": contentions,
        }


@router.get("/api/dashboard")
def dashboard(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        open_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='开启'"
        ).fetchone()["c"]
        closed_n = conn.execute(
            "SELECT COUNT(*) c FROM windows WHERE status='已关闭'"
        ).fetchone()["c"]
        late_n = conn.execute("SELECT COUNT(*) c FROM late_bucket").fetchone()["c"]
        reject_n = conn.execute("SELECT COUNT(*) c FROM reject_slips").fetchone()["c"]
        hold_n = conn.execute(
            "SELECT COUNT(*) c FROM hold_staging WHERE status='暂存'"
        ).fetchone()["c"]
        mats = conn.execute("SELECT COUNT(*) c FROM materializations").fetchone()["c"]
        contention = conn.execute("SELECT COUNT(*) c FROM contention_log").fetchone()["c"]
        return {
            "open_windows": open_n,
            "closed_windows": closed_n,
            "late_bucket": late_n,
            "rejects": reject_n,
            "holds": hold_n,
            "materializations": mats,
            "contention": contention,
        }
