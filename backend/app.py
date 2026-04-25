"""
GW Construction Management - Backend API
- FastAPI + SQLite (단일 파일 DB, 외부 DB 불필요)
- 직원, 현장, 배치, 출퇴근(GPS), 법인 관리
- 핵심 정책:
    * 배치는 'plan' / 'actual' / 'reported' 3종으로 분리 저장
    * 출퇴근은 GPS 좌표를 받아 현장 지오펜스 안인지 검증
"""
import os
import math
import json
import sqlite3
import secrets
from datetime import date, datetime
from contextlib import contextmanager
from typing import Optional, List

from fastapi import FastAPI, HTTPException, Query, Body, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from starlette.middleware.sessions import SessionMiddleware

try:
    import bcrypt
except ImportError:
    bcrypt = None  # 패키지 미설치 시 임시 평문 모드 (배포 시엔 항상 설치됨)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# DB 경로 — 환경변수 DB_PATH 로 영구 디스크로 옮길 수 있음 (Render 유료 디스크 등)
DB_PATH = os.environ.get("DB_PATH") or os.path.join(os.path.dirname(os.path.abspath(__file__)), "construction.db")
FRONTEND_DIR = os.path.join(BASE_DIR, "frontend")
SECRET_KEY = os.environ.get("SECRET_KEY") or secrets.token_hex(32)

# ========================================================================
# DB
# ========================================================================
SCHEMA = """
CREATE TABLE IF NOT EXISTS companies (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL,
  business_no TEXT,
  ceo TEXT,
  license_info TEXT
);
CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER REFERENCES companies(id),
  name TEXT NOT NULL,
  address TEXT,
  latitude REAL,
  longitude REAL,
  geofence_meters INTEGER DEFAULT 200,
  contract_amount INTEGER DEFAULT 0,
  paid_amount INTEGER DEFAULT 0,
  start_date TEXT,
  end_date TEXT,
  status TEXT DEFAULT 'active',
  manager TEXT
);
CREATE TABLE IF NOT EXISTS workers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  company_id INTEGER REFERENCES companies(id),
  name TEXT NOT NULL,
  phone TEXT,
  worker_type TEXT NOT NULL DEFAULT 'daily',  -- 'daily' | 'office'
  daily_wage INTEGER DEFAULT 0,
  job_role TEXT,
  hired_date TEXT,
  rrn_last TEXT,
  bank_account TEXT,
  note TEXT
);
CREATE TABLE IF NOT EXISTS deployments (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
  date TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'plan',  -- 'plan' | 'actual' | 'reported'
  note TEXT,
  UNIQUE(worker_id, date, kind)
);
CREATE TABLE IF NOT EXISTS users (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  username TEXT UNIQUE NOT NULL,
  password_hash TEXT NOT NULL,
  name TEXT,
  role TEXT DEFAULT 'admin',          -- 'admin' | 'manager'
  company_id INTEGER REFERENCES companies(id),
  created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS clock_records (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  worker_id INTEGER NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
  site_id INTEGER NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
  date TEXT NOT NULL,
  clock_in TEXT,
  clock_out TEXT,
  in_lat REAL,
  in_lng REAL,
  in_distance_m REAL,
  in_verified INTEGER DEFAULT 0,
  out_lat REAL,
  out_lng REAL,
  out_distance_m REAL,
  out_verified INTEGER DEFAULT 0,
  UNIQUE(worker_id, date)
);
"""

@contextmanager
def conn():
    c = sqlite3.connect(DB_PATH)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA foreign_keys = ON")
    try:
        yield c
        c.commit()
    finally:
        c.close()

def init_db():
    with conn() as c:
        c.executescript(SCHEMA)

# ========================================================================
# Helpers
# ========================================================================
def haversine_m(lat1, lon1, lat2, lon2):
    """두 GPS 좌표 사이의 거리 (m)."""
    if None in (lat1, lon1, lat2, lon2):
        return None
    R = 6371000.0
    p1 = math.radians(lat1); p2 = math.radians(lat2)
    dp = math.radians(lat2 - lat1); dl = math.radians(lon2 - lon1)
    a = math.sin(dp/2)**2 + math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*R*math.asin(math.sqrt(a))

def row_to_dict(row):
    return dict(row) if row else None

def rows(rs):
    return [dict(r) for r in rs]

# ========================================================================
# Pydantic models (입력)
# ========================================================================
class CompanyIn(BaseModel):
    name: str
    business_no: Optional[str] = None
    ceo: Optional[str] = None
    license_info: Optional[str] = None

class SiteIn(BaseModel):
    company_id: Optional[int] = None
    name: str
    address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geofence_meters: Optional[int] = 200
    contract_amount: Optional[int] = 0
    paid_amount: Optional[int] = 0
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    status: Optional[str] = "active"
    manager: Optional[str] = None

class WorkerIn(BaseModel):
    company_id: Optional[int] = None
    name: str
    phone: Optional[str] = None
    worker_type: Optional[str] = "daily"
    daily_wage: Optional[int] = 0
    job_role: Optional[str] = None
    hired_date: Optional[str] = None
    rrn_last: Optional[str] = None
    bank_account: Optional[str] = None
    note: Optional[str] = None

class RegisterIn(BaseModel):
    name: str
    phone: str
    worker_type: Optional[str] = "daily"
    job_role: Optional[str] = None

class LoginIn(BaseModel):
    username: str
    password: str

class PasswordChangeIn(BaseModel):
    current_password: str
    new_password: str

class DeploymentIn(BaseModel):
    worker_id: int
    site_id: int
    date: str
    kind: str = "plan"   # plan | actual | reported
    note: Optional[str] = None

class ClockIn(BaseModel):
    worker_id: int
    site_id: int
    lat: Optional[float] = None
    lng: Optional[float] = None
    direction: str = "in"   # 'in' or 'out'

# ========================================================================
# App
# ========================================================================
app = FastAPI(title="GW Construction Management API")
app.add_middleware(SessionMiddleware, secret_key=SECRET_KEY, session_cookie="gwc_sess",
                   max_age=60*60*24*14, https_only=False, same_site="lax")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=False,
    allow_methods=["*"], allow_headers=["*"],
)

# ========================================================================
# Auth helpers
# ========================================================================
def hash_pw(p: str) -> str:
    if bcrypt:
        return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()
    return "plain:" + p   # fallback (개발용)

def verify_pw(p: str, h: str) -> bool:
    if not h: return False
    if h.startswith("plain:"):
        return h == "plain:" + p
    if bcrypt:
        try: return bcrypt.checkpw(p.encode(), h.encode())
        except Exception: return False
    return False

def require_login(request: Request):
    uid = request.session.get("user_id")
    if not uid:
        raise HTTPException(401, "로그인이 필요합니다")
    with conn() as c:
        u = c.execute("SELECT id, username, name, role, company_id FROM users WHERE id=?", (uid,)).fetchone()
    if not u:
        request.session.clear()
        raise HTTPException(401, "세션이 만료되었습니다")
    return dict(u)

def _bootstrap_admin():
    """관리자 계정이 하나도 없으면 기본 admin 계정 생성."""
    try:
        with conn() as c:
            n = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if n == 0:
                pw = os.environ.get("ADMIN_PASSWORD", "admin1234")
                c.execute("INSERT INTO users(username,password_hash,name,role) VALUES(?,?,?,?)",
                          ("admin", hash_pw(pw), "관리자", "admin"))
                used_env = bool(os.environ.get("ADMIN_PASSWORD"))
                print(f"[startup] 기본 관리자 계정 생성 — username=admin, "
                      + ("password=(환경변수 ADMIN_PASSWORD 사용)" if used_env
                         else "password=admin1234  ⚠️ 환경변수 ADMIN_PASSWORD 설정 강력 권장"))
    except Exception as e:
        print(f"[startup] bootstrap admin skipped: {e}")

def _auto_seed_if_empty():
    """빈 DB면 샘플 데이터 자동 입력 (Render 무료 티어 cold start 대비)."""
    try:
        with conn() as c:
            n = c.execute("SELECT COUNT(*) FROM companies").fetchone()[0]
        if n == 0:
            import sys as _sys, os as _os
            _sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))
            from seed import seed as _seed
            _seed()
            print("[startup] 샘플 데이터 자동 입력 완료")
    except Exception as e:
        print(f"[startup] auto-seed skipped: {e}")

@app.on_event("startup")
def _startup():
    init_db()
    _bootstrap_admin()
    _auto_seed_if_empty()

# ----- Auth -----
@app.post("/api/login")
def login(payload: LoginIn, request: Request):
    with conn() as c:
        u = c.execute("SELECT * FROM users WHERE username=?", (payload.username,)).fetchone()
    if not u or not verify_pw(payload.password, u["password_hash"]):
        raise HTTPException(401, "아이디 또는 비밀번호가 일치하지 않습니다")
    request.session["user_id"] = u["id"]
    request.session["username"] = u["username"]
    request.session["role"] = u["role"]
    return {"ok": True, "username": u["username"], "name": u["name"], "role": u["role"]}

@app.post("/api/logout")
def logout(request: Request):
    request.session.clear()
    return {"ok": True}

@app.get("/api/me")
def me(request: Request):
    uid = request.session.get("user_id")
    if not uid:
        return {"authenticated": False}
    with conn() as c:
        u = c.execute("SELECT id, username, name, role, company_id FROM users WHERE id=?", (uid,)).fetchone()
    if not u:
        request.session.clear()
        return {"authenticated": False}
    return {"authenticated": True, **dict(u)}

@app.post("/api/me/password")
def change_password(payload: PasswordChangeIn, user: dict = Depends(require_login)):
    with conn() as c:
        u = c.execute("SELECT password_hash FROM users WHERE id=?", (user["id"],)).fetchone()
        if not u or not verify_pw(payload.current_password, u["password_hash"]):
            raise HTTPException(401, "현재 비밀번호가 맞지 않습니다")
        if len(payload.new_password) < 6:
            raise HTTPException(400, "새 비밀번호는 6자 이상이어야 합니다")
        c.execute("UPDATE users SET password_hash=? WHERE id=?",
                  (hash_pw(payload.new_password), user["id"]))
    return {"ok": True}

# ----- Companies -----
@app.get("/api/companies")
def list_companies(_: dict = Depends(require_login)):
    with conn() as c:
        return rows(c.execute("SELECT * FROM companies ORDER BY id").fetchall())

@app.post("/api/companies")
def create_company(payload: CompanyIn, _: dict = Depends(require_login)):
    with conn() as c:
        cur = c.execute(
            "INSERT INTO companies(name,business_no,ceo,license_info) VALUES(?,?,?,?)",
            (payload.name, payload.business_no, payload.ceo, payload.license_info)
        )
        return {"id": cur.lastrowid}

# ----- 모바일 출퇴근 화면용 공개 API (최소 정보만) -----
@app.get("/api/public/clock-options")
def public_clock_options():
    with conn() as c:
        workers = rows(c.execute(
            "SELECT id, name, job_role, worker_type FROM workers ORDER BY name").fetchall())
        sites = rows(c.execute(
            "SELECT id, name FROM sites WHERE status='active' ORDER BY name").fetchall())
    return {"workers": workers, "sites": sites}

# ----- Sites -----
@app.get("/api/sites")
def list_sites(active_only: bool = False, _: dict = Depends(require_login)):
    sql = "SELECT s.*, c.name AS company_name FROM sites s LEFT JOIN companies c ON s.company_id=c.id"
    if active_only:
        sql += " WHERE s.status='active'"
    sql += " ORDER BY s.id DESC"
    with conn() as c:
        return rows(c.execute(sql).fetchall())

@app.post("/api/sites")
def create_site(payload: SiteIn, _: dict = Depends(require_login)):
    with conn() as c:
        cur = c.execute(
            """INSERT INTO sites(company_id,name,address,latitude,longitude,geofence_meters,
               contract_amount,paid_amount,start_date,end_date,status,manager)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            (payload.company_id, payload.name, payload.address, payload.latitude, payload.longitude,
             payload.geofence_meters or 200, payload.contract_amount or 0, payload.paid_amount or 0,
             payload.start_date, payload.end_date, payload.status or "active", payload.manager)
        )
        return {"id": cur.lastrowid}

@app.put("/api/sites/{sid}")
def update_site(sid: int, payload: SiteIn, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute(
            """UPDATE sites SET company_id=?, name=?, address=?, latitude=?, longitude=?,
               geofence_meters=?, contract_amount=?, paid_amount=?, start_date=?, end_date=?,
               status=?, manager=? WHERE id=?""",
            (payload.company_id, payload.name, payload.address, payload.latitude, payload.longitude,
             payload.geofence_meters or 200, payload.contract_amount or 0, payload.paid_amount or 0,
             payload.start_date, payload.end_date, payload.status or "active", payload.manager, sid)
        )
    return {"ok": True}

@app.delete("/api/sites/{sid}")
def delete_site(sid: int, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute("DELETE FROM sites WHERE id=?", (sid,))
    return {"ok": True}

# ----- Workers -----
@app.get("/api/workers")
def list_workers(worker_type: Optional[str] = None, q: Optional[str] = None,
                 _: dict = Depends(require_login)):
    sql = "SELECT w.*, c.name AS company_name FROM workers w LEFT JOIN companies c ON w.company_id=c.id WHERE 1=1"
    args = []
    if worker_type:
        sql += " AND w.worker_type=?"; args.append(worker_type)
    if q:
        sql += " AND (w.name LIKE ? OR w.phone LIKE ?)"
        args += [f"%{q}%", f"%{q}%"]
    sql += " ORDER BY w.name"
    with conn() as c:
        return rows(c.execute(sql, args).fetchall())

@app.post("/api/workers")
def create_worker(payload: WorkerIn, _: dict = Depends(require_login)):
    with conn() as c:
        cur = c.execute(
            """INSERT INTO workers(company_id,name,phone,worker_type,daily_wage,job_role,hired_date,rrn_last,bank_account,note)
               VALUES(?,?,?,?,?,?,?,?,?,?)""",
            (payload.company_id, payload.name, payload.phone, payload.worker_type or "daily",
             payload.daily_wage or 0, payload.job_role, payload.hired_date, payload.rrn_last,
             payload.bank_account, payload.note)
        )
        return {"id": cur.lastrowid}

@app.put("/api/workers/{wid}")
def update_worker(wid: int, payload: WorkerIn, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute(
            """UPDATE workers SET company_id=?, name=?, phone=?, worker_type=?, daily_wage=?,
               job_role=?, hired_date=?, rrn_last=?, bank_account=?, note=? WHERE id=?""",
            (payload.company_id, payload.name, payload.phone, payload.worker_type or "daily",
             payload.daily_wage or 0, payload.job_role, payload.hired_date, payload.rrn_last,
             payload.bank_account, payload.note, wid)
        )
    return {"ok": True}

@app.delete("/api/workers/{wid}")
def delete_worker(wid: int, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute("DELETE FROM workers WHERE id=?", (wid,))
    return {"ok": True}

# ----- 직원 자가 가입 (공개 엔드포인트) -----
@app.post("/api/register")
def register_worker(payload: RegisterIn):
    """직원이 폰에서 직접 등록. 이름·번호만으로 가입되며, 본사 관리자가 나중에
    소속 법인·일당 등을 보강한다. 같은 번호로 이미 가입된 경우 거부한다."""
    name = (payload.name or "").strip()
    phone = (payload.phone or "").strip()
    if not name or not phone:
        raise HTTPException(400, "이름과 연락처를 모두 입력해주세요.")
    if len(name) < 2:
        raise HTTPException(400, "이름은 2자 이상이어야 합니다.")
    digits = ''.join(ch for ch in phone if ch.isdigit())
    if len(digits) < 10 or len(digits) > 11:
        raise HTTPException(400, "연락처를 010-XXXX-XXXX 형식으로 입력해주세요.")
    with conn() as c:
        dup = c.execute("SELECT id, name FROM workers WHERE REPLACE(REPLACE(phone,'-',''),' ','')=?", (digits,)).fetchone()
        if dup:
            raise HTTPException(409, f"이미 {dup['name']} 님으로 가입된 번호입니다.")
        cur = c.execute(
            """INSERT INTO workers(name,phone,worker_type,job_role,note,hired_date)
               VALUES(?,?,?,?,?,?)""",
            (name, phone, payload.worker_type or "daily", payload.job_role,
             "자가 가입 — 본사 검토 대기", date.today().isoformat())
        )
        return {"ok": True, "id": cur.lastrowid, "name": name}

# ----- Deployments -----
@app.get("/api/deployments")
def list_deployments(date: str = Query(...), kind: Optional[str] = None,
                     _: dict = Depends(require_login)):
    sql = """SELECT d.*, w.name AS worker_name, w.worker_type, w.daily_wage,
             s.name AS site_name FROM deployments d
             JOIN workers w ON d.worker_id=w.id
             JOIN sites s ON d.site_id=s.id
             WHERE d.date=?"""
    args = [date]
    if kind:
        sql += " AND d.kind=?"; args.append(kind)
    sql += " ORDER BY s.name, w.name"
    with conn() as c:
        return rows(c.execute(sql, args).fetchall())

@app.post("/api/deployments")
def upsert_deployment(payload: DeploymentIn, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute("DELETE FROM deployments WHERE worker_id=? AND date=? AND kind=?",
                  (payload.worker_id, payload.date, payload.kind))
        cur = c.execute(
            "INSERT INTO deployments(worker_id,site_id,date,kind,note) VALUES(?,?,?,?,?)",
            (payload.worker_id, payload.site_id, payload.date, payload.kind, payload.note)
        )
        return {"id": cur.lastrowid}

@app.delete("/api/deployments/{did}")
def delete_deployment(did: int, _: dict = Depends(require_login)):
    with conn() as c:
        c.execute("DELETE FROM deployments WHERE id=?", (did,))
    return {"ok": True}

@app.post("/api/deployments/copy")
def copy_deployments(src_kind: str = Body(...), dst_kind: str = Body(...), date: str = Body(...),
                     _: dict = Depends(require_login)):
    """계획 → 실적 복사 같은 운영 편의 기능."""
    with conn() as c:
        c.execute("DELETE FROM deployments WHERE date=? AND kind=?", (date, dst_kind))
        c.execute(
            """INSERT INTO deployments(worker_id,site_id,date,kind,note)
               SELECT worker_id,site_id,date,?,note FROM deployments WHERE date=? AND kind=?""",
            (dst_kind, date, src_kind)
        )
    return {"ok": True}

# ----- Clock in/out (mobile, GPS) -----
@app.post("/api/clock")
def clock(payload: ClockIn):
    with conn() as c:
        site = c.execute("SELECT * FROM sites WHERE id=?", (payload.site_id,)).fetchone()
        if not site:
            raise HTTPException(404, "site not found")
        worker = c.execute("SELECT * FROM workers WHERE id=?", (payload.worker_id,)).fetchone()
        if not worker:
            raise HTTPException(404, "worker not found")

        today = date.today().isoformat()
        now = datetime.now().isoformat(timespec="seconds")
        dist = haversine_m(payload.lat, payload.lng, site["latitude"], site["longitude"])
        verified = 1 if (dist is not None and dist <= (site["geofence_meters"] or 200)) else 0

        existing = c.execute(
            "SELECT * FROM clock_records WHERE worker_id=? AND date=?",
            (payload.worker_id, today)
        ).fetchone()

        if payload.direction == "in":
            if existing:
                c.execute("""UPDATE clock_records SET clock_in=?, in_lat=?, in_lng=?,
                             in_distance_m=?, in_verified=?, site_id=? WHERE id=?""",
                          (now, payload.lat, payload.lng, dist, verified, payload.site_id, existing["id"]))
            else:
                c.execute("""INSERT INTO clock_records(worker_id,site_id,date,clock_in,in_lat,in_lng,in_distance_m,in_verified)
                             VALUES(?,?,?,?,?,?,?,?)""",
                          (payload.worker_id, payload.site_id, today, now, payload.lat, payload.lng, dist, verified))
            # 자동으로 실적 배치도 기록
            c.execute("DELETE FROM deployments WHERE worker_id=? AND date=? AND kind='actual'",
                      (payload.worker_id, today))
            c.execute("INSERT INTO deployments(worker_id,site_id,date,kind,note) VALUES(?,?,?,'actual','GPS 출근 자동')",
                      (payload.worker_id, payload.site_id, today))
        else:  # out
            if not existing:
                raise HTTPException(400, "출근 기록이 없습니다")
            c.execute("""UPDATE clock_records SET clock_out=?, out_lat=?, out_lng=?,
                         out_distance_m=?, out_verified=? WHERE id=?""",
                      (now, payload.lat, payload.lng, dist, verified, existing["id"]))
        return {
            "ok": True, "verified": bool(verified),
            "distance_m": round(dist, 1) if dist is not None else None,
            "geofence_m": site["geofence_meters"] or 200,
        }

@app.get("/api/clock/today")
def clock_today(_: dict = Depends(require_login)):
    today = date.today().isoformat()
    with conn() as c:
        return rows(c.execute("""
            SELECT cr.*, w.name AS worker_name, s.name AS site_name
            FROM clock_records cr
            JOIN workers w ON cr.worker_id=w.id
            JOIN sites s ON cr.site_id=s.id
            WHERE cr.date=? ORDER BY cr.clock_in DESC
        """, (today,)).fetchall())

# ----- Projects (현장별 일정·인력·비용·손익 종합) -----
@app.get("/api/projects")
def projects_overview(include_closed: bool = False, _: dict = Depends(require_login)):
    """현장을 큰 단위로 보기 위한 통합 데이터.
    누적 투입 인일, 누적 노무비, 일정 진행률, 예상 잔여 노무비, 예상 손익까지."""
    today_d = date.today()
    with conn() as c:
        sql = """SELECT s.*, c.name AS company_name FROM sites s
                 LEFT JOIN companies c ON s.company_id=c.id"""
        if not include_closed:
            sql += " WHERE s.status='active'"
        sql += " ORDER BY s.start_date IS NULL, s.start_date, s.id"
        sites = c.execute(sql).fetchall()
        out = []
        for s in sites:
            sd = dict(s)
            stats = c.execute(
                """SELECT COUNT(*) AS person_days,
                          IFNULL(SUM(w.daily_wage),0) AS labor_cost,
                          COUNT(DISTINCT d.worker_id) AS unique_workers,
                          MIN(d.date) AS first_day,
                          MAX(d.date) AS last_day
                   FROM deployments d JOIN workers w ON d.worker_id=w.id
                   WHERE d.site_id=? AND d.kind='actual'""", (sd['id'],)
            ).fetchone()
            sd['person_days']    = stats['person_days'] or 0
            sd['labor_cost']     = stats['labor_cost'] or 0
            sd['unique_workers'] = stats['unique_workers'] or 0
            sd['first_actual']   = stats['first_day']
            sd['last_actual']    = stats['last_day']

            # 오늘 인원 (실적)
            today_count = c.execute(
                "SELECT COUNT(DISTINCT worker_id) FROM deployments WHERE site_id=? AND date=? AND kind='actual'",
                (sd['id'], today_d.isoformat())
            ).fetchone()[0]
            sd['today_count'] = today_count

            # 일정 진행률
            try:
                start = datetime.fromisoformat(sd['start_date']).date() if sd['start_date'] else None
            except Exception: start = None
            try:
                end = datetime.fromisoformat(sd['end_date']).date() if sd['end_date'] else None
            except Exception: end = None
            total_days = (end - start).days if (start and end) else 0
            if start and today_d < start:
                elapsed = 0; sched = 'upcoming'
            elif end and today_d > end:
                elapsed = total_days; sched = 'overdue'
            elif start:
                elapsed = (today_d - start).days; sched = 'in_progress'
            else:
                elapsed = 0; sched = 'unknown'
            progress_pct = round(elapsed / total_days * 100) if total_days else 0
            remaining_days = max(0, total_days - elapsed)

            # 예상 총 노무비 (선형 외삽)
            if elapsed > 0 and total_days > 0:
                projected_labor = round(sd['labor_cost'] * total_days / elapsed)
            else:
                projected_labor = sd['labor_cost']
            projected_remaining_labor = max(0, projected_labor - sd['labor_cost'])
            estimated_profit = sd['contract_amount'] - projected_labor

            sd['days_total']       = total_days
            sd['days_elapsed']     = elapsed
            sd['days_remaining']   = remaining_days
            sd['progress_pct']     = progress_pct
            sd['schedule_status']  = sched
            sd['projected_labor']  = projected_labor
            sd['projected_remaining_labor'] = projected_remaining_labor
            sd['estimated_profit'] = estimated_profit
            sd['contract_remaining'] = sd['contract_amount'] - sd['paid_amount']
            out.append(sd)
        return out

# ----- Dashboard -----
@app.get("/api/dashboard")
def dashboard(_: dict = Depends(require_login)):
    today = date.today().isoformat()
    with conn() as c:
        sites_active = c.execute("SELECT COUNT(*) FROM sites WHERE status='active'").fetchone()[0]
        workers_total = c.execute("SELECT COUNT(*) FROM workers").fetchone()[0]
        deployed_today = c.execute(
            "SELECT COUNT(DISTINCT worker_id) FROM deployments WHERE date=? AND kind='actual'", (today,)
        ).fetchone()[0]
        planned_today = c.execute(
            "SELECT COUNT(DISTINCT worker_id) FROM deployments WHERE date=? AND kind='plan'", (today,)
        ).fetchone()[0]
        contract_total = c.execute("SELECT IFNULL(SUM(contract_amount),0) FROM sites WHERE status='active'").fetchone()[0]
        paid_total = c.execute("SELECT IFNULL(SUM(paid_amount),0) FROM sites WHERE status='active'").fetchone()[0]
        site_summary = rows(c.execute("""
            SELECT s.id, s.name, s.contract_amount, s.paid_amount,
                   (SELECT COUNT(DISTINCT worker_id) FROM deployments WHERE site_id=s.id AND date=? AND kind='actual') AS today_actual,
                   (SELECT COUNT(DISTINCT worker_id) FROM deployments WHERE site_id=s.id AND date=? AND kind='plan')   AS today_plan
            FROM sites s WHERE s.status='active' ORDER BY s.id DESC
        """, (today, today)).fetchall())
        company_breakdown = rows(c.execute("""
            SELECT c.id, c.name,
                   (SELECT COUNT(*) FROM sites WHERE company_id=c.id AND status='active') AS sites,
                   (SELECT COUNT(*) FROM workers WHERE company_id=c.id) AS workers,
                   (SELECT IFNULL(SUM(contract_amount),0) FROM sites WHERE company_id=c.id AND status='active') AS contract,
                   (SELECT IFNULL(SUM(paid_amount),0)     FROM sites WHERE company_id=c.id AND status='active') AS paid
            FROM companies c ORDER BY c.id
        """).fetchall())
    return {
        "today": today,
        "sites_active": sites_active,
        "workers_total": workers_total,
        "deployed_today": deployed_today,
        "planned_today": planned_today,
        "contract_total": contract_total,
        "paid_total": paid_total,
        "remaining": contract_total - paid_total,
        "site_summary": site_summary,
        "companies": company_breakdown,
    }

# ----- Static -----
@app.get("/")
def index():
    return FileResponse(os.path.join(FRONTEND_DIR, "index.html"))

@app.get("/m")
def mobile_page():
    return FileResponse(os.path.join(FRONTEND_DIR, "mobile.html"))

@app.get("/register")
def register_page():
    return FileResponse(os.path.join(FRONTEND_DIR, "register.html"))

@app.get("/login")
def login_page():
    return FileResponse(os.path.join(FRONTEND_DIR, "login.html"))

app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")

if __name__ == "__main__":
    import uvicorn
    init_db()
    port = int(os.environ.get("PORT", 8765))
    uvicorn.run(app, host="0.0.0.0", port=port)
