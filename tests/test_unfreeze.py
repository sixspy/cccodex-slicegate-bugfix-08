
import sqlite3
import threading
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app import app, init_db, compute_completeness, BOUNDARY_VERSION, CHOSEN_PATH
import app as appmod


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr(appmod, "DB_PATH", tmp_path / "t.db")
    init_db()
    yield


def _login(c, username="admin", password="123456"):
    r = c.post("/api/login", json={"username": username, "password": password})
    assert r.status_code == 200
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _raw():
    conn = sqlite3.connect(appmod.DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def test_boundary_doc_matches_api_and_path_a_chosen(tmp_path):
    # 口径唯一：文档、后端常量、策略旁注接口三处同源，且选定甲路径
    c = TestClient(app)
    h = _login(c)
    j = c.get("/api/policy/boundary", headers=h).json()
    assert j["chosen_path"] == "甲" == CHOSEN_PATH
    assert j["version"] == BOUNDARY_VERSION
    assert "同一事务" in j["chosen_rule"] and "待重裁" in j["chosen_rule"]
    # 乙路径仅留档不采用，禁止两套口径混用
    assert "不采用" in j["not_adopted"] and "快照" in j["not_adopted"]
    doc = (Path(__file__).resolve().parents[1] / "docs" / "解冻边界说明.md").read_text(encoding="utf-8")
    assert "**甲路径**" in doc
    assert BOUNDARY_VERSION in doc
    assert "失败闭" in doc and "解冻争用" in doc
    assert "乙" in doc and "不采用" in doc


def test_full_freeze_unfreeze_rejudge_release_path_a():
    c = TestClient(app)
    h = _login(c)
    sid = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-U-1", "biz_date": "2026-11-05", "partition_key": "east",
        "row_count": 1000, "expected_count": 1000}).json()["id"]

    # 冻结入候：关前“完整”被封存
    assert c.post(f"/api/slices/{sid}/freeze", headers=h).status_code == 200
    s = next(x for x in c.get("/api/slices", headers=h).json()["items"] if x["id"] == sid)
    assert s["frozen"] == 1 and s["completeness"] == "完整"
    assert s["preclose_completeness"] == "完整"

    # 候审中禁止重裁、禁止放行
    assert c.post(f"/api/slices/{sid}/evaluate", headers=h).status_code == 409
    assert c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "x"}).status_code == 409

    # 解冻：同事务作废关前缓存 → 待重裁；放行态仍为未放行
    r = c.post(f"/api/slices/{sid}/unfreeze", headers=h)
    assert r.status_code == 200
    assert r.json()["completeness"] == "待重裁"
    s = next(x for x in c.get("/api/slices", headers=h).json()["items"] if x["id"] == sid)
    assert s["frozen"] == 0 and s["completeness"] == "待重裁"
    assert s["preclose_completeness"] == "完整"  # 缓存只留档，不再当现行完整度
    assert s["release_status"] == "未放行"

    # 重裁前放行必须被拦并登记偏离
    blocked = c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "x"})
    assert blocked.status_code == 409 and "重裁" in blocked.json()["detail"]

    # 制造缺口后重裁：正式完整度由重裁写路径回写为部分完整，仍不得放行
    with _raw() as conn:
        conn.execute("UPDATE slices SET row_count=700 WHERE id=?", (sid,))
        conn.commit()
    ev = c.post(f"/api/slices/{sid}/evaluate", headers=h)
    assert ev.status_code == 200
    assert ev.json()["previous_completeness"] == "待重裁"
    assert ev.json()["completeness"] == "部分完整"
    assert c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "x"}).status_code == 409

    # 补齐后重裁为完整，才允许放行；放行成功写入水位
    with _raw() as conn:
        conn.execute("UPDATE slices SET row_count=1000 WHERE id=?", (sid,))
        conn.commit()
    assert c.post(f"/api/slices/{sid}/evaluate", headers=h).json()["completeness"] == "完整"
    assert c.post(f"/api/slices/{sid}/release", headers=h, json={"consumer": "ads_u"}).status_code == 200
    wms = c.get("/api/watermarks", headers=h).json()["items"]
    assert any(w["source_slice"] == "SL-U-1" for w in wms)

    # 已放行后再解冻/冻结：拒绝且不改放行态，登记口径偏离
    assert c.post(f"/api/slices/{sid}/unfreeze", headers=h).status_code == 409
    assert c.post(f"/api/slices/{sid}/freeze", headers=h).status_code == 409
    with _raw() as conn:
        row = conn.execute("SELECT release_status FROM slices WHERE id=?", (sid,)).fetchone()
        assert row["release_status"] == "已放行"
    events = [x for x in c.get("/api/ledger", headers=h).json()["items"] if x["slice_id"] == sid]
    names = [e["event"] for e in events]
    assert "冻结入候" in names and "解冻" in names and "重裁回写" in names
    assert names.count("解冻口径偏了") >= 3  # 候审放行、待重裁放行、已放行解冻/冻结


def test_concurrent_unfreeze_serialized_with_contention_log():
    c = TestClient(app)
    h = _login(c)
    sid = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-LOCK-1", "biz_date": "2026-11-06", "partition_key": "west",
        "row_count": 1000, "expected_count": 1000}).json()["id"]
    assert c.post(f"/api/slices/{sid}/freeze", headers=h).status_code == 200

    results = []
    barrier = threading.Barrier(2)

    def worker():
        cc = TestClient(app)
        hh = _login(cc)
        barrier.wait()
        rr = cc.post(f"/api/slices/{sid}/unfreeze", headers=hh)
        results.append(rr.status_code)

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)
    t1.start(); t2.start(); t1.join(); t2.join()

    assert sorted(results) == [200, 409]
    body = c.get("/api/slices", headers=h).json()
    s = next(x for x in body["items"] if x["id"] == sid)
    assert s["frozen"] == 0 and s["completeness"] == "待重裁"
    led = c.get("/api/ledger", headers=h).json()["items"]
    contends = [x for x in led if x["slice_id"] == sid and x["event"] == "解冻争用"]
    assert len(contends) == 1
    assert c.post(f"/api/slices/{sid}/unfreeze", headers=h).status_code == 409
    # 串行误操作（锁已空闲）不算争用，按口径偏离登记
    led2 = c.get("/api/ledger", headers=h).json()["items"]
    extra = [x for x in led2 if x["slice_id"] == sid and x["event"] == "解冻争用"]
    assert len(extra) == 1
    dev = [x for x in led2 if x["slice_id"] == sid and x["event"] == "解冻口径偏了"]
    assert len(dev) == 1


def test_unfreeze_fail_closed_on_write_error():
    import app as appmod
    c = TestClient(app)
    h = _login(c)
    sid = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-FC-1", "biz_date": "2026-11-07", "partition_key": "mid",
        "row_count": 1000, "expected_count": 1000}).json()["id"]
    c.post(f"/api/slices/{sid}/freeze", headers=h)

    # 用真实 SQL 触发器制造解冻 UPDATE 写入故障：事务必须整体回滚，状态钉在候审态（失败闭）
    setup = sqlite3.connect(appmod.DB_PATH)
    setup.execute("CREATE TRIGGER fail_closed_guard BEFORE UPDATE ON slices "
                  "BEGIN SELECT RAISE(ABORT,'injected disk I/O error'); END")
    setup.commit(); setup.close()

    r = c.post(f"/api/slices/{sid}/unfreeze", headers=h)
    assert r.status_code == 500 and "失败闭" in r.json()["detail"]

    conn = sqlite3.connect(appmod.DB_PATH)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT frozen,completeness,release_status FROM slices WHERE id=?", (sid,)).fetchone()
    assert row["frozen"] == 1 and row["completeness"] == "完整" and row["release_status"] == "未放行"
    n = conn.execute("SELECT COUNT(*) n FROM unfreeze_ledger WHERE slice_id=? AND event='解冻'", (sid,)).fetchone()["n"]
    assert n == 0  # 失败的解冻不得留下成功流水
    conn.close()


def test_preclose_cache_never_read_as_current_completeness():
    # 解冻台与详情同源载荷：缓存列与现行完整度分列，待重裁绝不显示成“完整”
    c = TestClient(app)
    h = _login(c)
    items = {x["slice_no"]: x for x in c.get("/api/slices", headers=h).json()["items"]}
    seed = items["SL-2026-11-03-south"]
    assert seed["completeness"] == "待重裁"
    assert seed["preclose_completeness"] == "完整"
    assert seed["frozen"] == 0
    # 放行闸不认缓存
    r = c.post(f"/api/slices/{seed['id']}/release", headers=h, json={"consumer": "x"})
    assert r.status_code == 409


def test_compute_completeness_single_source():
    # 与既有登记口径完全一致，全仓只此一个裁决函数
    assert compute_completeness(1000, 1000) == "完整"
    assert compute_completeness(999, 1000) == "部分完整"
    assert compute_completeness(0, 1000) == "待检"
    assert compute_completeness(10, 0) == "部分完整"
    assert compute_completeness(0, 0) == "待检"


def test_double_freeze_and_unfreeze_non_frozen_are_deviations():
    c = TestClient(app)
    h = _login(c)
    sid = c.post("/api/slices", headers=h, json={
        "slice_no": "SL-DEV-1", "biz_date": "2026-11-08", "partition_key": "east",
        "row_count": 10, "expected_count": 10}).json()["id"]
    assert c.post(f"/api/slices/{sid}/freeze", headers=h).status_code == 200
    assert c.post(f"/api/slices/{sid}/freeze", headers=h).status_code == 409
    assert c.post(f"/api/slices/{sid}/unfreeze", headers=h).status_code == 200
    # 未候审再解冻同样偏离
    assert c.post(f"/api/slices/{sid}/unfreeze", headers=h).status_code == 409
    led = c.get("/api/ledger", headers=h).json()["items"]
    devs = [x for x in led if x["slice_id"] == sid and x["event"] == "解冻口径偏了"]
    assert len(devs) == 2


def test_legacy_db_migration_keeps_data_and_adds_columns(tmp_path, monkeypatch):
    # 模拟旧库：只有最初 8 列、无 unfreeze_ledger，迁移后数据不丢、新列补齐
    db = tmp_path / "legacy.db"
    monkeypatch.setattr(appmod, "DB_PATH", db)
    conn = sqlite3.connect(db)
    conn.execute("""CREATE TABLE slices(
        id INTEGER PRIMARY KEY AUTOINCREMENT, slice_no TEXT UNIQUE, biz_date TEXT,
        partition_key TEXT, row_count INTEGER, expected_count INTEGER,
        completeness TEXT, release_status TEXT)""")
    conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status)"
                 " VALUES ('SL-OLD-1','2026-10-31','east',5,10,'部分完整','未放行')")
    conn.execute("""CREATE TABLE releases(id INTEGER PRIMARY KEY AUTOINCREMENT, slice_id INTEGER,
        consumer TEXT, status TEXT, note TEXT, created_at TEXT)""")
    conn.execute("""CREATE TABLE watermarks(id INTEGER PRIMARY KEY AUTOINCREMENT, consumer TEXT,
        biz_date TEXT, watermark TEXT, source_slice TEXT)""")
    conn.commit(); conn.close()

    init_db()
    conn = sqlite3.connect(db)
    conn.row_factory = sqlite3.Row
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(slices)").fetchall()}
    assert {"frozen", "preclose_completeness", "preclose_snapshot"} <= cols
    row = conn.execute("SELECT * FROM slices WHERE slice_no='SL-OLD-1'").fetchone()
    assert row["completeness"] == "部分完整" and row["frozen"] == 0 and row["preclose_completeness"] is None
    conn.execute("SELECT COUNT(*) n FROM unfreeze_ledger").fetchone()
    conn.close()


def test_login_unchanged_and_single_site():
    c = TestClient(app)
    # 口令登录照旧
    assert c.post("/api/login", json={"username": "admin", "password": "123456"}).status_code == 200
    assert c.post("/api/login", json={"username": "viewer", "password": "view123"}).status_code == 200
    bad = c.post("/api/login", json={"username": "admin", "password": "wrong"})
    assert bad.status_code == 401
    # 仍是既有单页站点，未新建站点
    page = c.get("/")
    assert page.status_code == 200 and "日切放行" in page.text
    assert "解冻台" in page.text and "处置页" in page.text
    # 未登录拿不到业务数据
    assert c.get("/api/slices").status_code == 401
