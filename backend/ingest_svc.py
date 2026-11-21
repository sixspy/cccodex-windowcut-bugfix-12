"""事件入账：窗内 / 拒收 / 迟到桶分流。

拒收正确性口径见 ``docs/拒收同命册.md``：择定**甲路径**——
``events`` 拒收行与 ``reject_slips`` 凭证同库同事务，同生共死；
两人同时拒收同窗键时，入账锁串行化，后到只记争用、业务零写。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from fastapi import APIRouter, HTTPException

from auth import AuthHeader, require_user
from db import ROOT, connect, write_audit
from locks import release, try_acquire
from models import EventIn

router = APIRouter(tags=["ingest"])

# 拒收同命册（甲/乙口径唯一登记处，当日锁定禁止改口）
FATE_LEDGER_PATH = ROOT / "docs" / "拒收同命册.md"
_fate_caliber_cache: str | None = None


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _load_fate_caliber() -> str:
    """读同命册机器口径行（``口径: 甲``）。"""
    global _fate_caliber_cache
    if _fate_caliber_cache is None:
        try:
            text = FATE_LEDGER_PATH.read_text(encoding="utf-8")
        except OSError:
            text = ""
        caliber = ""
        for line in text.splitlines():
            if line.strip().startswith("口径"):
                caliber = line.split(":", 1)[1].strip()
                break
        _fate_caliber_cache = caliber
    return _fate_caliber_cache


def _guard_fate_ledger(conn, user: str) -> None:
    """偏离同命册（非甲/混用/无登记）：拒写并留痕『同命偏了』。"""
    caliber = _load_fate_caliber()
    if caliber != "甲":
        write_audit(
            conn,
            user,
            "reject_fate_drift",
            None,
            {"expected": "甲", "actual": caliber, "note": "同命偏了"},
        )
        conn.commit()
        raise HTTPException(500, "拒收同命册偏离甲口径：同命偏了，本次拒收拒写")


def _record_contention(conn, lock_key: str, user: str, detail: str) -> None:
    """后到者：争用登记 + 审计同事务落库，业务零写。"""
    now = _now()
    conn.execute(
        """INSERT INTO contention_log(lock_key,actor,action,detail,created_at)
           VALUES (?,?,?,?,?)""",
        (lock_key, user, "争用", detail, now),
    )
    write_audit(
        conn,
        user,
        "ingest_lock_contention",
        lock_key,
        {"ok": False, "detail": detail},
    )
    conn.commit()


def _acquire_ingest_lock(conn, lock_key: str, user: str) -> None:
    """占入账锁。

    锁行先落库并提前提交（发布），使并发者立即撞锁而非排队后双双写入；
    撞唯一约束或 ``database is locked`` 一律识别为争用：后到只记争用。
    """
    try:
        ok = try_acquire(conn, lock_key, user)
    except sqlite3.IntegrityError as exc:  # 并发 INSERT 撞锁行唯一约束
        # 先回滚撞锁语句（连同 try_acquire 未提交的争用登记），再重新
        # 登记：否则 rollback 会把 contention_log 一并撤销，争用无痕
        conn.rollback()
        _record_contention(conn, lock_key, user, f"唯一约束撞锁: {exc}")
        raise HTTPException(409, "争用：后到只记争用，不写业务") from exc
    except sqlite3.OperationalError as exc:  # database is locked
        if "locked" not in str(exc).lower():
            raise
        conn.rollback()
        _record_contention(conn, lock_key, user, f"写锁占用: {exc}")
        raise HTTPException(409, "争用：后到只记争用，不写业务") from exc
    if not ok:
        # try_acquire 已写 contention_log，这里补审计并提交
        write_audit(
            conn, user, "ingest_lock_contention", lock_key, {"ok": False}
        )
        conn.commit()
        raise HTTPException(409, "争用：后到只记争用，不写业务")
    # 提前发布锁行：并发者的锁 INSERT 立刻显形
    conn.commit()


@router.post("/api/events")
def ingest_event(body: EventIn, authorization: AuthHeader = None):
    user = require_user(authorization)
    lock_key = f"ingest:{body.window_name}"
    with connect() as conn:
        _guard_fate_ledger(conn, user)
        _acquire_ingest_lock(conn, lock_key, user)
        try:
            w = conn.execute(
                "SELECT * FROM windows WHERE name=?", (body.window_name,)
            ).fetchone()
            if not w:
                raise HTTPException(404, "窗口不存在")
            ts = body.event_ts
            created = _now()
            if w["status"] == "已关闭":
                cur = conn.execute(
                    """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (None, body.biz_key, ts, body.amount, "迟到桶", created),
                )
                eid = cur.lastrowid
                conn.execute(
                    """INSERT INTO late_bucket(event_id,window_name,biz_key,event_ts,amount,reason,created_at)
                       VALUES (?,?,?,?,?,?,?)""",
                    (
                        eid,
                        body.window_name,
                        body.biz_key,
                        ts,
                        body.amount,
                        "关窗后到达",
                        created,
                    ),
                )
                write_audit(
                    conn,
                    user,
                    "ingest_late",
                    body.biz_key,
                    {"window": body.window_name},
                )
                conn.commit()
                return {"disposition": "迟到桶", "event_id": eid}

            if not (w["open_at"] <= ts <= w["close_at"]):
                # 甲路径（拒收同命册）：events 拒收行与 reject_slips 凭证
                # 同库同事务，任一失败两侧回滚零改，绝不只写单边。
                try:
                    conn.execute(
                        """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                           VALUES (?,?,?,?,?,?)""",
                        (None, body.biz_key, ts, body.amount, "拒收", created),
                    )
                    conn.execute(
                        """INSERT INTO reject_slips(window_name,biz_key,event_ts,amount,reason,created_at)
                           VALUES (?,?,?,?,?,?)""",
                        (
                            body.window_name,
                            body.biz_key,
                            ts,
                            body.amount,
                            "事件时间落在窗外且窗仍开启：拒收",
                            created,
                        ),
                    )
                    write_audit(
                        conn,
                        user,
                        "ingest_reject",
                        body.biz_key,
                        {"window": body.window_name, "fate": "甲-同事务"},
                    )
                    conn.commit()
                except sqlite3.Error:
                    conn.rollback()
                    raise HTTPException(
                        500, "拒收同事务提交失败：事件与凭证两侧已回滚零改"
                    )
                raise HTTPException(409, "事件时间落在窗外且窗仍开启：拒收")

            cur = conn.execute(
                """INSERT INTO events(window_id,biz_key,event_ts,amount,disposition,created_at)
                   VALUES (?,?,?,?,?,?)""",
                (w["id"], body.biz_key, ts, body.amount, "窗内", created),
            )
            write_audit(
                conn,
                user,
                "ingest_in_window",
                body.biz_key,
                {"window_id": w["id"]},
            )
            conn.commit()
            return {"disposition": "窗内", "window_id": w["id"], "event_id": cur.lastrowid}
        finally:
            release(conn, lock_key, user)
            conn.commit()


@router.get("/api/events")
def list_events(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM events ORDER BY id DESC LIMIT 200"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}


@router.get("/api/rejects")
def list_rejects(authorization: AuthHeader = None):
    require_user(authorization)
    with connect() as conn:
        rows = conn.execute(
            "SELECT * FROM reject_slips ORDER BY id DESC LIMIT 100"
        ).fetchall()
        return {"items": [dict(r) for r in rows]}
