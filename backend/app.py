
import hashlib, json, secrets, sqlite3, threading
from datetime import datetime
from pathlib import Path
from fastapi import Header, HTTPException
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# ---------------------------------------------------------------------------
# 解冻边界（口径唯一，选定后当天禁止改口；详见 docs/解冻边界说明.md）
# 采用甲路径：解冻与“作废关前完整缓存”同一事务提交，并写入待重裁态；
# 正式完整度仅在重裁成功后由重裁写路径回写。
# 禁止解冻后继续拿关前缓存当完整；禁止解冻顺带改已放行态。
# ---------------------------------------------------------------------------
BOUNDARY_VERSION = "2026-11-17"
CHOSEN_PATH = "甲"
PATH_A_RULE = ("解冻与作废关前完整缓存在同一事务提交，切片进入待重裁态；"
               "正式完整度仅重裁成功后由重裁写路径回写")
PATH_B_RULE = ("关前完整先落入只读历史快照表再清空现行完整度字段，"
               "解冻后须按现行期望重裁通过才允许放行（本仓不采用）")

EVT_FREEZE = "冻结入候"
EVT_UNFREEZE = "解冻"
EVT_REJUDGE = "重裁回写"
EVT_CONTENTION = "解冻争用"
EVT_DEVIATION = "解冻口径偏了"

# 同一切片两名值班同时抢解冻时的进程内串行锁（跨进程由 BEGIN IMMEDIATE 兜底）
_locks_guard = threading.Lock()
_unfreeze_locks: dict[int, threading.Lock] = {}


def slice_lock(sid: int) -> threading.Lock:
    with _locks_guard:
        lk = _unfreeze_locks.get(sid)
        if lk is None:
            lk = threading.Lock()
            _unfreeze_locks[sid] = lk
        return lk


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()


def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))


# 完整度口径的唯一裁决函数：登记、重裁以及任何写路径都只能用它，禁止各处混算
def compute_completeness(row_count: int, expected_count: int) -> str:
    if expected_count > 0 and row_count >= expected_count:
        return "完整"
    if row_count > 0:
        return "部分完整"
    return "待检"


class LoginIn(BaseModel):
    username: str
    password: str


def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

PRODUCT = "日切放行"
SKELETON = "业务日分区切片登记后裁决完整度，完整才放行下游，并与消费水位对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])


def _has_column(conn, table: str, column: str) -> bool:
    return any(r["name"] == column for r in conn.execute(f"PRAGMA table_info({table})").fetchall())


def _record_event(conn, sid: int, event: str, detail: str, operator: str):
    conn.execute(
        "INSERT INTO unfreeze_ledger(slice_id,event,detail,operator,created_at) VALUES (?,?,?,?,?)",
        (sid, event, detail, operator, now_iso()),
    )


def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS slices(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_no TEXT UNIQUE NOT NULL,
          biz_date TEXT NOT NULL,
          partition_key TEXT NOT NULL,
          row_count INTEGER NOT NULL,
          expected_count INTEGER NOT NULL,
          completeness TEXT NOT NULL,
          release_status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS releases(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          consumer TEXT NOT NULL,
          status TEXT NOT NULL,
          note TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS watermarks(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          consumer TEXT NOT NULL,
          biz_date TEXT NOT NULL,
          watermark TEXT NOT NULL,
          source_slice TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS unfreeze_ledger(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          slice_id INTEGER NOT NULL,
          event TEXT NOT NULL,
          detail TEXT NOT NULL,
          operator TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        """)
        # 既有库文件沿用：缺列补列，不重建、不丢数据
        if not _has_column(conn, "slices", "frozen"):
            conn.execute("ALTER TABLE slices ADD COLUMN frozen INTEGER NOT NULL DEFAULT 0")
        if not _has_column(conn, "slices", "preclose_completeness"):
            conn.execute("ALTER TABLE slices ADD COLUMN preclose_completeness TEXT")
        if not _has_column(conn, "slices", "preclose_snapshot"):
            conn.execute("ALTER TABLE slices ADD COLUMN preclose_snapshot TEXT")

        if conn.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"] == 0:
            rows = [
                ("SL-2026-11-01-east", "2026-11-01", "east", 980, 1000, "部分完整", "未放行"),
                ("SL-2026-11-01-west", "2026-11-01", "west", 1000, 1000, "完整", "已放行"),
                ("SL-2026-11-02-east", "2026-11-02", "east", 0, 1000, "待检", "未放行"),
            ]
            for r in rows:
                conn.execute("INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)", r)
            conn.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (2,'ads_order','成功','完整切片放行','2026-11-01T18:00:00')")
            conn.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES ('ads_order','2026-11-01','2026-11-01T18:00:00','SL-2026-11-01-west')")

            # 候审中：关前缓存仍记录着“完整”，解冻台须标明该缓存不得再当完整
            conn.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                "completeness,release_status,frozen,preclose_completeness,preclose_snapshot) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("SL-2026-11-03-north", "2026-11-03", "north", 1000, 1000,
                 "完整", "未放行", 1, "完整",
                 json.dumps({"row_count": 1000, "expected_count": 1000, "completeness": "完整"},
                            ensure_ascii=False)),
            )
            north_id = conn.execute("SELECT id FROM slices WHERE slice_no='SL-2026-11-03-north'").fetchone()["id"]
            _record_event(conn, north_id, EVT_FREEZE, "关前冻结入候，关前完整缓存已封存", "系统")

            # 已解冻待重裁：关前缓存“完整”已作废，现行完整度为待重裁，重裁通过前不得放行
            conn.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,"
                "completeness,release_status,frozen,preclose_completeness,preclose_snapshot) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                ("SL-2026-11-03-south", "2026-11-03", "south", 720, 1000,
                 "待重裁", "未放行", 0, "完整",
                 json.dumps({"row_count": 1000, "expected_count": 1000, "completeness": "完整"},
                            ensure_ascii=False)),
            )
            south_id = conn.execute("SELECT id FROM slices WHERE slice_no='SL-2026-11-03-south'").fetchone()["id"]
            _record_event(conn, south_id, EVT_UNFREEZE,
                          "同一事务作废关前完整缓存并写入待重裁态，正式完整度待重裁回写", "系统")


@app.on_event("startup")
def _start():
    init_db()


@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM slices").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "slices": n, "db": str(DB_PATH)}


@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username, role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row:
        raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok] = row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}


@app.get("/api/slices")
def list_slices(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM slices ORDER BY id").fetchall()
    # 解冻台、切片详情、重裁目录共用这一份载荷，数字同源可核
    return {"items": [dict(r) for r in rows]}


class SliceIn(BaseModel):
    slice_no: str
    biz_date: str
    partition_key: str
    row_count: int
    expected_count: int


@app.post("/api/slices")
def add_slice(body: SliceIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    comp = compute_completeness(body.row_count, body.expected_count)
    with connect() as c:
        try:
            cur = c.execute(
                "INSERT INTO slices(slice_no,biz_date,partition_key,row_count,expected_count,completeness,release_status) VALUES (?,?,?,?,?,?,?)",
                (body.slice_no, body.biz_date, body.partition_key, body.row_count, body.expected_count, comp, "未放行"),
            )
            return {"id": cur.lastrowid, "completeness": comp}
        except sqlite3.IntegrityError:
            raise HTTPException(409, "切片号已存在")


@app.post("/api/slices/{sid}/freeze")
def freeze(sid: int, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        if row["release_status"] == "已放行":
            detail = "已放行切片禁止改候审态，冻结请求被拦截"
            _record_event(c, sid, EVT_DEVIATION, detail, operator)
            c.commit()
            raise HTTPException(409, detail)
        if row["frozen"] == 1:
            detail = "切片已处于冻结候审态，禁止重复冻结"
            _record_event(c, sid, EVT_DEVIATION, detail, operator)
            c.commit()
            raise HTTPException(409, detail)
        snapshot = json.dumps(
            {"row_count": row["row_count"], "expected_count": row["expected_count"],
             "completeness": row["completeness"]},
            ensure_ascii=False,
        )
        c.execute(
            "UPDATE slices SET frozen=1, preclose_completeness=?, preclose_snapshot=? WHERE id=?",
            (row["completeness"], snapshot, sid),
        )
        _record_event(c, sid, EVT_FREEZE,
                      f"关前冻结入候，关前完整度={row['completeness']} 仅作缓存封存", operator)
    return {"ok": True, "id": sid, "frozen": 1}


@app.post("/api/slices/{sid}/unfreeze")
def unfreeze(sid: int, authorization: str | None = Header(default=None)):
    """甲路径唯一解冻写路径：加锁串行 → 同事务作废关前缓存 + 待重裁态。失败闭。"""
    operator = require_user(authorization)
    lk = slice_lock(sid)
    # 抢锁前锁已被占，说明是同切片并发抢解冻，后到者按“解冻争用”处置
    contended = lk.locked()
    if not lk.acquire(timeout=10):
        try:
            with connect() as ec:
                _record_event(ec, sid, EVT_CONTENTION, "等待解冻锁超时，后到请求未获处置权", operator)
        except Exception:
            raise HTTPException(500, "解冻争用登记失败，按失败闭拒绝本次操作")
        raise HTTPException(409, "解冻争用：当前切片正由其他值班处理，请稍后重试")
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            conn.rollback()
            raise HTTPException(404, "切片不存在")
        if row["release_status"] == "已放行":
            detail = "禁止解冻顺带改已放行态，请求被拦截"
            _record_event(conn, sid, EVT_DEVIATION, detail, operator)
            conn.commit()
            raise HTTPException(409, detail)
        if row["frozen"] != 1:
            if contended:
                _record_event(conn, sid, EVT_CONTENTION,
                              "两名值班同时抢解冻，后到请求未获处置权", operator)
                conn.commit()
                raise HTTPException(409, "解冻争用：该切片已被其他值班解冻，请勿重复操作")
            detail = "切片未处于冻结候审态，解冻请求偏离解冻边界说明"
            _record_event(conn, sid, EVT_DEVIATION, detail, operator)
            conn.commit()
            raise HTTPException(409, detail)
        # 甲路径核心：条件更新保证只有持缓存者能解冻；同事务把现行完整度钉成待重裁，
        # 关前缓存列保留仅作历史核对，任何读路径都不得再把它当现行完整度。
        cur = conn.execute(
            "UPDATE slices SET frozen=0, completeness='待重裁' "
            "WHERE id=? AND frozen=1 AND release_status!='已放行'",
            (sid,),
        )
        if cur.rowcount == 0:
            conn.rollback()
            with connect() as ec:
                _record_event(ec, sid, EVT_CONTENTION,
                              "两名值班同时抢解冻，后到请求未获处置权", operator)
            raise HTTPException(409, "解冻争用：该切片已被其他值班解冻，请勿重复操作")
        _record_event(
            conn, sid, EVT_UNFREEZE,
            f"同一事务作废关前完整缓存（关前={row['preclose_completeness'] or row['completeness']}）"
            "并写入待重裁态；正式完整度仅重裁成功后回写，未重裁通过前禁止放行",
            operator,
        )
        conn.commit()
    except HTTPException:
        raise
    except Exception:
        conn.rollback()
        raise HTTPException(500, "解冻写入失败，已按失败闭回滚，切片状态未变更")
    finally:
        conn.close()
        lk.release()

    with connect() as c:
        latest = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
    return {"ok": True, "id": sid, "frozen": 0,
            "completeness": latest["completeness"],
            "preclose_completeness": latest["preclose_completeness"]}


@app.post("/api/slices/{sid}/evaluate")
def evaluate(sid: int, authorization: str | None = Header(default=None)):
    """重裁写路径：唯一允许把待重裁态改写回正式完整度的地方。"""
    operator = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        if row["frozen"] == 1:
            detail = "切片冻结候审中，须先解冻再重裁，请求被拦截"
            _record_event(c, sid, EVT_DEVIATION, detail, operator)
            c.commit()
            raise HTTPException(409, detail)
        old_comp = row["completeness"]
        comp = compute_completeness(row["row_count"], row["expected_count"])
        c.execute("UPDATE slices SET completeness=? WHERE id=?", (comp, sid))
        if old_comp == "待重裁":
            # 正式完整度只在这一刻、由这一条写路径回写
            _record_event(c, sid, EVT_REJUDGE,
                          f"按现行实收/期望重裁：待重裁 → {comp}（关前缓存不再参与裁决）",
                          operator)
    return {"id": sid, "completeness": comp, "previous_completeness": old_comp}


class ReleaseIn(BaseModel):
    consumer: str


@app.post("/api/slices/{sid}/release")
def release(sid: int, body: ReleaseIn, authorization: str | None = Header(default=None)):
    operator = require_user(authorization)
    with connect() as c:
        row = c.execute("SELECT * FROM slices WHERE id=?", (sid,)).fetchone()
        if not row:
            raise HTTPException(404, "切片不存在")
        # 失败闭：候审/待重裁一律拦，绝不允许拿关前缓存放行
        if row["frozen"] == 1:
            detail = "切片冻结候审中，禁止放行"
            _record_event(c, sid, EVT_DEVIATION, detail, operator)
            c.commit()
            raise HTTPException(409, detail)
        if row["completeness"] == "待重裁":
            detail = "解冻后尚未重裁回写正式完整度，重裁通过前禁止放行"
            _record_event(c, sid, EVT_DEVIATION, detail, operator)
            c.commit()
            raise HTTPException(409, detail)
        if row["completeness"] != "完整":
            c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                      (sid, body.consumer, "拒绝", "完整度未达标", now_iso()))
            c.commit()
            raise HTTPException(409, "未完整禁止放行")
        c.execute("UPDATE slices SET release_status='已放行' WHERE id=?", (sid,))
        c.execute("INSERT INTO releases(slice_id,consumer,status,note,created_at) VALUES (?,?,?,?,?)",
                  (sid, body.consumer, "成功", "完整切片放行", now_iso()))
        c.execute("INSERT INTO watermarks(consumer,biz_date,watermark,source_slice) VALUES (?,?,?,?)",
                  (body.consumer, row["biz_date"], now_iso(), row["slice_no"]))
    return {"ok": True}


@app.get("/api/releases")
def list_releases(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT r.*, s.slice_no FROM releases r JOIN slices s ON s.id=r.slice_id ORDER BY r.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/watermarks")
def list_wm(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM watermarks ORDER BY id DESC").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/ledger")
def ledger(authorization: str | None = Header(default=None)):
    """处置页数据源：解冻争用、口径偏离、冻结/解冻/重裁全程留痕。"""
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT l.*, s.slice_no FROM unfreeze_ledger l
                            JOIN slices s ON s.id=l.slice_id ORDER BY l.id DESC""").fetchall()
    return {"items": [dict(r) for r in rows]}


@app.get("/api/policy/boundary")
def policy_boundary(authorization: str | None = Header(default=None)):
    """策略旁注数据源：前端只展示，不自行解释口径。"""
    require_user(authorization)
    return {
        "version": BOUNDARY_VERSION,
        "chosen_path": CHOSEN_PATH,
        "chosen_rule": PATH_A_RULE,
        "not_adopted": PATH_B_RULE,
        "invariants": [
            "解冻写完整度失败闭：任何一步失败整体回滚，切片状态不变",
            "完整度口径只走解冻写路径（甲路径）一种，禁止两套口径混用",
            "解冻同事务作废关前完整缓存并写入待重裁态，关前缓存不得再当完整",
            "正式完整度仅重裁成功后由重裁写路径回写",
            "解冻不得修改已放行态，违者拦截并登记“解冻口径偏了”",
            "同切片并发解冻加锁串行，后到者明示“解冻争用”并登记",
        ],
    }


@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")


app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
