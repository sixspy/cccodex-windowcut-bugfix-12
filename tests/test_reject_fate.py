"""拒收同命（BUGFIX-12）：双影复现 + 双人同键争用。

纪律：先红后修。两个核心失败测——
  1. test_reject_double_shadow_fate：窗外拒收，events 与 reject_slips 必须同命运；
  2. test_concurrent_reject_same_key_contention：两人同时拒收同窗键，
     后到只记争用，业务零写。
另附：同事务失败两侧回滚、拒收同命册偏离守卫。
本地夹具 + SQLite，不换库文件。
"""
from __future__ import annotations

import contextlib
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient

from app import app, init_db


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("db.DB_PATH", tmp_path / "fate.db")
    import ingest_svc

    monkeypatch.setattr(ingest_svc, "_fate_caliber_cache", None)
    init_db()
    yield


def _auth(c: TestClient, username: str = "admin", password: str = "123456") -> dict:
    tok = c.post(
        "/api/login", json={"username": username, "password": password}
    ).json()["token"]
    return {"Authorization": "Bearer " + tok}


@contextlib.contextmanager
def _raw_conn():
    """只读/校验连接；退出时真正 close（sqlite3 自带的 with 不关连接）。"""
    from db import DB_PATH

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


def _count(conn: sqlite3.Connection, table: str, where: str = "", args=()) -> int:
    sql = f"SELECT COUNT(*) c FROM {table} {('WHERE ' + where) if where else ''}"
    return conn.execute(sql, args).fetchone()["c"]


REJECT_BODY = {
    "window_name": "W-1102",
    "biz_key": "OUT-FATE",
    "event_ts": "2026-10-01T00:00:00",  # 落在 W-1102 窗外、窗仍开启
    "amount": 9,
}
LOCK_KEY = "ingest:W-1102"


# ---------------------------------------------------------------- 失败测一：双影
def test_reject_double_shadow_fate():
    """窗外拒收：事件行与拒收凭证必须同源同命，审计/窗口/对账同态。"""
    c = TestClient(app)
    h = _auth(c)
    r = c.post("/api/events", headers=h, json=REJECT_BODY)
    assert r.status_code == 409
    assert "拒收" in r.json()["detail"]

    with _raw_conn() as conn:
        ev_n = _count(
            conn, "events", "disposition='拒收' AND biz_key=?", (REJECT_BODY["biz_key"],)
        )
        slip_n = _count(
            conn, "reject_slips", "biz_key=?", (REJECT_BODY["biz_key"],)
        )
        # 同命运：两侧各恰好一行，字段同源
        assert ev_n == 1, "拒收事件应恰好一行"
        assert slip_n == 1, "拒收凭证应恰好一行——双影/单边即失败"
        ev = conn.execute(
            "SELECT * FROM events WHERE disposition='拒收' AND biz_key=?",
            (REJECT_BODY["biz_key"],),
        ).fetchone()
        slip = conn.execute(
            "SELECT * FROM reject_slips WHERE biz_key=?", (REJECT_BODY["biz_key"],)
        ).fetchone()
        assert slip["window_name"] == REJECT_BODY["window_name"]
        assert slip["event_ts"] == ev["event_ts"]
        assert slip["amount"] == ev["amount"]
        assert slip["created_at"] == ev["created_at"]
        assert slip["reason"]
        # 窗口：拒收不改窗口
        w = conn.execute("SELECT * FROM windows WHERE name='W-1102'").fetchone()
        assert w["status"] == "开启"
        assert w["generation"] == 1
        # 审计：有且仅有一条 ingest_reject，且不再带埋雷标记
        audit = conn.execute(
            "SELECT * FROM audit_trail WHERE action='ingest_reject'"
        ).fetchall()
        assert len(audit) == 1
        assert "no_reject_slip" not in (audit[0]["payload"] or "")

    # 对账：拒收计数取凭证实数（不再恒 0）；本笔不得贡献双影
    recon = c.get("/api/reconcile/2", headers=h).json()
    assert recon["rejects"] >= 1
    assert REJECT_BODY["biz_key"] not in recon["reject_shadow_keys"]
    assert recon["reject_orphan_slips"] == 0

    # 审计旁路：同态总览。本笔事件/凭证同源配对；
    # 夹具历史双影（种子拒收事件无凭证）应被如实暴露，而不是被恒 0 掩盖
    fate = c.get("/api/audit/reject-fate", headers=h).json()
    shadow_keys = [x["biz_key"] for x in fate["shadow_detail"]]
    assert REJECT_BODY["biz_key"] not in shadow_keys
    assert "ORD-reject-sample" in shadow_keys  # 历史双影如实显形
    assert fate["orphan_slips"] == 0
    assert fate["reject_events"] - fate["shadow_events"] == fate["reject_slips"]


# ------------------------------------------------ 失败测二：双人同时拒收同键争用
def test_concurrent_reject_same_key_contention(monkeypatch):
    """两人同时对同窗键拒收：入账锁串行化，后到只记争用、业务零写。

    两个确定性场景：
      A. 锁已被他人持有（SELECT 可见）：后到直接 409 争用、业务零写；
      B. 同一瞬间 INSERT 锁行（唯一约束撞锁）：赢家占锁发布后挂起，
         输家正面撞锁被识别为争用，而非排队后双双写入或 500。
    """
    import ingest_svc
    import locks

    c = TestClient(app)
    h_admin = _auth(c, "admin", "123456")
    h_closer = _auth(c, "closer", "close123")

    # ---- 场景 A：锁行已发布，后到者 SELECT 即见持有者
    with _raw_conn() as holder:
        holder.execute(
            "INSERT INTO window_locks(lock_key,holder,acquired_at) VALUES (?,?,?)",
            (LOCK_KEY, "admin", "2026-11-21T00:00:00"),
        )
        holder.commit()
        r = c.post("/api/events", headers=h_closer, json=REJECT_BODY)
        assert r.status_code == 409
        assert "争用" in r.json()["detail"]
        # 业务零写：争用不产生拒收事件/凭证
        assert _count(
            holder, "events",
            "disposition='拒收' AND biz_key=?", (REJECT_BODY["biz_key"],),
        ) == 0
        assert _count(
            holder, "reject_slips", "biz_key=?", (REJECT_BODY["biz_key"],)
        ) == 0
        holder.execute("DELETE FROM window_locks WHERE lock_key=?", (LOCK_KEY,))
        holder.commit()

    # 记录场景 A 之后的争用/审计基线，以便精确核对场景 B 的增量
    with _raw_conn() as base:
        base_contention = _count(base, "contention_log", "lock_key=?", (LOCK_KEY,))
        base_audit = _count(base, "audit_trail", "action='ingest_lock_contention'")

    # ---- 场景 B：两线程同一瞬间 INSERT 同一锁行（唯一约束撞锁）
    start_barrier = threading.Barrier(2, timeout=10)
    winner_release = threading.Event()
    collision_seen = threading.Event()
    orig_try_acquire = locks.try_acquire
    winner_holder = {"name": None}

    def synced_try_acquire(conn, key, holder):
        start_barrier.wait()  # 两线程在此对齐，随后同时发起占锁
        try:
            ok = orig_try_acquire(conn, key, holder)
        except sqlite3.IntegrityError:
            # 输家已正面撞上锁行唯一约束，通知赢家可以放行
            collision_seen.set()
            raise
        if ok and key == LOCK_KEY:
            # 赢家：标记并提交发布锁行（释放写锁），让输家阻塞的 INSERT
            # 重试撞唯一约束；挂到输家确实撞锁后再继续，杜绝调度漏洞。
            winner_holder["name"] = holder
            conn.commit()
            if not winner_release.wait(timeout=10):
                # 回退：等待超时也放行，避免无赢家等极端调度下测试挂死
                pass
        return ok

    results: dict[str, dict] = {}

    def worker(name: str, headers: dict):
        try:
            cc = TestClient(app, raise_server_exceptions=False)
            rr = cc.post("/api/events", headers=headers, json=REJECT_BODY)
            results[name] = {
                "status": rr.status_code,
                "detail": rr.json().get("detail", ""),
            }
        except Exception as exc:  # noqa: BLE001
            results[name] = {"status": -1, "detail": repr(exc)}

    # ingest_svc 以 from-import 持有独立引用，两处都要 patch 才生效
    monkeypatch.setattr(locks, "try_acquire", synced_try_acquire)
    monkeypatch.setattr(ingest_svc, "try_acquire", synced_try_acquire)
    t1 = threading.Thread(target=worker, args=("a", h_admin), daemon=True)
    t2 = threading.Thread(target=worker, args=("b", h_closer), daemon=True)
    t1.start()
    t2.start()

    # 等输家确实正面撞锁（IntegrityError），再放行赢家完成拒收事务
    assert collision_seen.wait(timeout=10), "未观察到同键锁行唯一约束撞锁"
    assert winner_holder["name"] is not None, "赢家占锁未发布"
    winner_release.set()
    t1.join(15)
    t2.join(15)
    assert not t1.is_alive() and not t2.is_alive(), "争用线程超时未结束"
    assert set(results) == {"a", "b"}

    loser = next(v for v in results.values() if "争用" in v["detail"])
    assert loser["status"] == 409
    winner_res = next(v for v in results.values() if "拒收" in v["detail"])
    assert winner_res["status"] == 409

    # 断言必须在 monkeypatch.undo() 之前：此时 db.DB_PATH 仍指向临时库
    with _raw_conn() as conn:
        # 后到只记争用：场景 B 在场景 A 基线上恰好新增一条 contention_log
        assert (
            _count(conn, "contention_log", "lock_key=?", (LOCK_KEY,))
            == base_contention + 1
        )
        # 同键业务只成功一笔（赢家），输家零写
        assert _count(
            conn, "events",
            "disposition='拒收' AND biz_key=?", (REJECT_BODY["biz_key"],),
        ) == 1
        assert _count(
            conn, "reject_slips", "biz_key=?", (REJECT_BODY["biz_key"],)
        ) == 1
        # 争用同态：场景 B 新增一条锁争用审计
        assert (
            _count(conn, "audit_trail", "action='ingest_lock_contention'")
            == base_audit + 1
        )
        # 窗口未被任何一方改动
        w = conn.execute("SELECT * FROM windows WHERE name='W-1102'").fetchone()
        assert w["status"] == "开启"
        # 锁已释放，无残留
        assert _count(conn, "window_locks", "lock_key=?", (LOCK_KEY,)) == 0

    monkeypatch.undo()


# ------------------------------------------------ 同事务失败：两侧回滚零残留
def test_reject_atomic_rollback_when_slip_write_fails(monkeypatch):
    """甲路径：events 与 reject_slips 同库事务，凭证写失败则两侧回滚。"""
    import ingest_svc

    real_connect = ingest_svc.connect

    class ExplodingConn(sqlite3.Connection):
        def execute(self, sql, params=()):  # type: ignore[override]
            if "INSERT INTO reject_slips" in sql:
                raise sqlite3.OperationalError("夹具模拟：拒收凭证写入失败")
            return super().execute(sql, params)

    def exploding_connect():
        from db import DB_PATH

        conn = sqlite3.connect(DB_PATH, factory=ExplodingConn)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    c = TestClient(app)
    h = _auth(c)
    monkeypatch.setattr(ingest_svc, "connect", exploding_connect)
    r = c.post("/api/events", headers=h, json=REJECT_BODY)
    assert r.status_code == 500
    monkeypatch.setattr(ingest_svc, "connect", real_connect)

    with _raw_conn() as conn:
        assert _count(conn, "events", "disposition='拒收' AND biz_key=?",
                      (REJECT_BODY["biz_key"],)) == 0
        assert _count(conn, "reject_slips", "biz_key=?",
                      (REJECT_BODY["biz_key"],)) == 0
        assert _count(conn, "window_locks") == 0


# ------------------------------------------------ 同命册：偏离口径拒写并留痕
def test_fate_ledger_drift_blocks_reject(monkeypatch, tmp_path):
    """拒收同命册被改成混用/乙路径：偏离即拒写，审计记『同命偏了』。"""
    import ingest_svc

    drifted = tmp_path / "drifted.md"
    drifted.write_text(
        "# 拒收同命册\n\n口径: 乙\n日期: 2026-11-21\n", encoding="utf-8"
    )
    monkeypatch.setattr(ingest_svc, "FATE_LEDGER_PATH", drifted)
    monkeypatch.setattr(ingest_svc, "_fate_caliber_cache", None, raising=False)

    c = TestClient(app)
    h = _auth(c)
    r = c.post("/api/events", headers=h, json=REJECT_BODY)
    assert r.status_code == 500
    assert "同命偏了" in r.json()["detail"]

    with _raw_conn() as conn:
        assert _count(conn, "events", "disposition='拒收' AND biz_key=?",
                      (REJECT_BODY["biz_key"],)) == 0
        assert _count(conn, "reject_slips", "biz_key=?",
                      (REJECT_BODY["biz_key"],)) == 0
        assert _count(conn, "audit_trail", "action='reject_fate_drift'") == 1
