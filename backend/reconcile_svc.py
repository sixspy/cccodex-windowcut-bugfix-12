"""窗内事件与物化结果互证。"""
from __future__ import annotations

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import connect

router = APIRouter(tags=["reconcile"])


@router.get("/api/reconcile/{wid}")
def reconcile(wid: int, authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        w = conn.execute("SELECT * FROM windows WHERE id=?", (wid,)).fetchone()
        if not w:
            raise HTTPException(404, "窗口不存在")
        ev = conn.execute(
            """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
               FROM events WHERE window_id=? AND disposition='窗内'""",
            (wid,),
        ).fetchone()
        mat = conn.execute(
            "SELECT * FROM materializations WHERE window_id=? ORDER BY id DESC LIMIT 1",
            (wid,),
        ).fetchone()
        late = conn.execute(
            "SELECT COUNT(*) c FROM late_bucket WHERE window_name=?",
            (w["name"],),
        ).fetchone()["c"]
        # 拒收同命（甲路径）：计数取凭证实数；事件与凭证不一致即暴露双影/孤儿
        rejects = conn.execute(
            "SELECT COUNT(*) c FROM reject_slips WHERE window_name=?",
            (w["name"],),
        ).fetchone()["c"]
        reject_events = conn.execute(
            """SELECT COUNT(*) c FROM events
               WHERE disposition='拒收' AND biz_key IN (
                   SELECT biz_key FROM reject_slips WHERE window_name=?)""",
            (w["name"],),
        ).fetchone()["c"]
        # 有拒收事件、无对应凭证 = 双影；有凭证、无对应事件 = 孤儿凭证
        shadow_rows = conn.execute(
            """SELECT e.biz_key FROM events e
               WHERE e.disposition='拒收'
                 AND NOT EXISTS (
                     SELECT 1 FROM reject_slips s
                     WHERE s.biz_key=e.biz_key
                       AND (s.window_name=? OR s.window_name IS NULL))""",
            (w["name"],),
        ).fetchall()
        orphan_rows = conn.execute(
            """SELECT s.biz_key FROM reject_slips s
               WHERE s.window_name=?
                 AND NOT EXISTS (
                     SELECT 1 FROM events e
                     WHERE e.disposition='拒收' AND e.biz_key=s.biz_key)""",
            (w["name"],),
        ).fetchall()
        aligned = (
            bool(mat)
            and mat["row_count"] == ev["c"]
            and abs(mat["total_amount"] - ev["s"]) < 1e-6
        )
        return {
            "window": dict(w),
            "window_events": ev["c"],
            "window_amount": ev["s"],
            "materialized": dict(mat) if mat else None,
            "late_bucket": late,
            "rejects": rejects,
            "reject_events": reject_events,
            "reject_shadow_events": len(shadow_rows),
            "reject_orphan_slips": len(orphan_rows),
            "reject_shadow_keys": [r["biz_key"] for r in shadow_rows],
            "reject_orphan_keys": [r["biz_key"] for r in orphan_rows],
            "aligned": aligned,
        }


@router.get("/api/reconcile")
def reconcile_all(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        windows = conn.execute("SELECT id,name,status FROM windows ORDER BY id").fetchall()
        items = []
        for w in windows:
            ev = conn.execute(
                """SELECT COUNT(*) c, COALESCE(SUM(amount),0) s
                   FROM events WHERE window_id=? AND disposition='窗内'""",
                (w["id"],),
            ).fetchone()
            mat = conn.execute(
                "SELECT * FROM materializations WHERE window_id=? ORDER BY id DESC LIMIT 1",
                (w["id"],),
            ).fetchone()
            late = conn.execute(
                "SELECT COUNT(*) c FROM late_bucket WHERE window_name=?",
                (w["name"],),
            ).fetchone()["c"]
            rejects = conn.execute(
                "SELECT COUNT(*) c FROM reject_slips WHERE window_name=?",
                (w["name"],),
            ).fetchone()["c"]
            shadow = conn.execute(
                """SELECT COUNT(*) c FROM events e
                   WHERE e.disposition='拒收'
                     AND NOT EXISTS (
                         SELECT 1 FROM reject_slips s
                         WHERE s.biz_key=e.biz_key
                           AND (s.window_name=? OR s.window_name IS NULL))""",
                (w["name"],),
            ).fetchone()["c"]
            aligned = (
                bool(mat)
                and mat["row_count"] == ev["c"]
                and abs(mat["total_amount"] - ev["s"]) < 1e-6
            )
            items.append(
                {
                    "window_id": w["id"],
                    "name": w["name"],
                    "status": w["status"],
                    "aligned": aligned,
                    "window_events": ev["c"],
                    "late_bucket": late,
                    "rejects": rejects,
                    "reject_shadow_events": shadow,
                }
            )
        return {"items": items}
