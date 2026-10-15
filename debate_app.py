# ─────────────────────────────────────────────
# gevent monkey patching — 반드시 다른 import보다 먼저 실행되어야 한다.
# 이게 없으면 AI 호출이나 DB 조회 한 건이 서버 전체를 멈추게 하고,
# 그동안 다른 참가자들의 연결이 끊긴다.
# ─────────────────────────────────────────────
from gevent import monkey
monkey.patch_all()

try:
    from psycogreen.gevent import patch_psycopg
    patch_psycopg()   # psycopg2를 gevent와 협조적으로 동작하게 만든다
    _PSYCOGREEN = True
except ImportError:
    _PSYCOGREEN = False
    print("[경고] psycogreen 미설치 — DB 조회 중 다른 사용자가 멈출 수 있습니다.")

import os
import json
import random
import time
import uuid
import hmac
import secrets
import hashlib
import psycopg2
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
from flask_socketio import SocketIO, emit, join_room

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('DEBATE_SECRET_KEY', 'debate_secret_key_1234')
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="gevent",
    ping_interval=25,   # 25초마다 연결 확인
    ping_timeout=60,    # 60초 무응답까지 버팀 (모바일 화면 꺼짐·네트워크 전환 대비)
)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다. Supabase 연결 문자열을 등록해주세요.")

waiting_pool = []
rooms = {}
sid_to_room = {}
sid_to_user = {}
user_to_room = {}      # username -> room_id (재접속 시 방을 찾기 위해)
resume_tokens = {}     # token -> username (재접속 인증용)

# 접속이 끊겨도 토론은 끝나지 않는다. 돌아올 때까지 시계를 멈추고 기다린다.
# 이 시간이 지나도 안 돌아오면 관리자 화면에 경고를 띄워 운영자가 판단하게 한다.
# (몰수패는 본인이 직접 누른 항복, 또는 관리자가 승인한 '토론 의지 없음' 판정뿐)
ABSENT_ALERT_SECONDS = 120

STAGES = ["입론", "반론", "반박", "최종변론"]
TOTAL_STAGES = len(STAGES)
TURN_SECONDS = 300              # 기본 발언 시간
OPENING_SECONDS = 420           # 입론 — 두 사람이 동시에 7분 동안 작성
MIN_MESSAGE_LENGTH = 30        # 이보다 짧은 발언은 반려한다
EMPTY_MESSAGE = "(작성 없음)"   # 시간이 끝났는데 아무것도 안 썼을 때
AUTO_NOTE = "⏰ 시간 종료로 자동 제출된 발언 — 작성 중이던 내용 그대로"   # 심사 AI에게 알리는 표시
SERVER_TIMEOUT_GRACE = 6        # 브라우저 자동 전송이 안 오면 서버가 대신 마감하기까지 여유

# 반론 이후 단계별 발언 순서 (진영 기준)
# 반론·반박은 찬성 먼저, 최종변론은 반대 먼저 → 찬성(입증 책임이 있는 쪽)이 마지막 말을 한다.
SPEAK_ORDER = {
    1: ["찬성", "반대"],   # 반론
    2: ["찬성", "반대"],   # 반박
    3: ["반대", "찬성"],   # 최종변론
}


def turn_limit(stage_index, speaker_idx=None):
    """단계별 제한 시간. 입론은 7분(동시 작성), 나머지는 5분."""
    return OPENING_SECONDS if stage_index == 0 else TURN_SECONDS


# 심사 기준 — 100점 만점 (단계당 25점, 세부 항목별 채점)
# (키, 항목 이름, 배점)
RUBRIC = [
    ("입론", [("op_claim", "논지의 명확성", 10),
              ("op_evidence", "근거의 타당성·구체성", 10),
              ("op_form", "구성과 전달력", 5)]),
    ("반론", [("rb_target", "상대 논리의 핵심 허점 포착", 10),
              ("rb_evidence", "반론 근거의 타당성", 10),
              ("rb_form", "구성과 전달력", 5)]),
    ("반박", [("rr_answer", "받은 반론에 대한 직접 대응", 10),
              ("rr_logic", "재반박의 논리성", 10),
              ("rr_reinforce", "자기 논지 보강", 5)]),
    ("최종변론", [("cl_summary", "전체 논지 정리", 10),
                ("cl_persuasion", "설득력", 10),
                ("cl_compare", "상대 대비 우위 제시", 5)]),
]
TOTAL_POINTS = sum(pts for _, items in RUBRIC for _, _, pts in items)
JUDGE_CRITERIA = [(stage, " · ".join(f"{name} {pts}" for _, name, pts in items))
                  for stage, items in RUBRIC]

ADMIN_CODE = os.environ.get("DEBATE_ADMIN_CODE", "1234")
TEST_MODE = "테스트 모드"

# AI는 ChatGPT(OpenAI) 하나만 쓴다.
PROVIDER = "openai"
DEFAULT_MODEL = "gpt-6.1-sol"   # 모델은 고정 — 관리자 화면에서는 키만 등록한다
# 지정한 모델이 없다고(404 model_not_found) 나오면 순서대로 시도한다.
# 모델명이 틀렸을 때 엉뚱한 오류로 헤매지 않도록.
FALLBACK_MODELS = ["gpt-6.1-sol", "gpt-5.6-terra", "gpt-5.4-mini"]
DEFAULT_MAX_RPM = 120           # 유료 키 기준. 관리자 화면에서 조정 가능

# AI가 주제를 못 만들 때 쓸 예비 주제. 예전처럼 한 주제만 반복되지 않도록 여러 개를 둔다.
# 주제는 '~해야 한다' 형태의 긍정 평서문으로 통일한다.
# 의문문이면 찬성/반대가 무엇을 뜻하는지 헷갈리기 때문.
BACKUP_TOPICS = [
    "학교는 학생의 스마트폰 사용을 제한해야 한다.",
    "대학 입시에서 정시 비중을 늘려야 한다.",
    "인공지능이 만든 창작물에도 저작권을 인정해야 한다.",
    "청소년의 SNS 이용에 연령 제한을 두어야 한다.",
    "학교 급식은 채식 선택권을 보장해야 한다.",
    "학교는 교내 CCTV를 확대 설치해야 한다.",
    "선거 연령을 더 낮춰야 한다.",
    "학생부 기재에서 봉사활동을 제외해야 한다.",
    "온라인 수업으로 대면 수업을 대체할 수 있다.",
    "기업의 주 4일제 도입을 확대해야 한다.",
    "동물 실험을 전면 금지해야 한다.",
    "자율주행차 사고의 책임은 제조사가 져야 한다.",
    "공공장소에서 얼굴 인식 기술 사용을 허용해야 한다.",
    "학교 시험을 절대평가로 전환해야 한다.",
    "유명인의 사생활 보도는 더 엄격히 제한해야 한다.",
]



# ─────────────────────────────────────────────
# DB 기본 유틸
# ─────────────────────────────────────────────
# ── DB 연결 풀 ─────────────────────────────────────────────
# 예전에는 조회할 때마다 DB에 새로 접속(보안 연결 포함)해서 버튼 하나에 수백 ms씩 걸렸다.
# 접속을 몇 개 열어두고 돌려쓰면 그 대기 시간이 사라진다.
from psycopg2 import pool as _pg_pool
_pool = None

def _get_pool():
    global _pool
    if _pool is None:
        _pool = _pg_pool.ThreadedConnectionPool(
            1, 8, DATABASE_URL, sslmode=os.environ.get("DB_SSLMODE", "require"),
            keepalives=1, keepalives_idle=30, keepalives_interval=10, keepalives_count=3)
    return _pool


def _run(work):
    """풀에서 연결을 빌려 work(cursor)를 실행. 끊어진 연결이면 버리고 한 번 더 시도한다."""
    for attempt in range(2):
        p = _get_pool()
        conn = p.getconn()
        broken = False
        try:
            cur = conn.cursor()
            result = work(cur)
            conn.commit()
            cur.close()
            return result
        except (psycopg2.OperationalError, psycopg2.InterfaceError):
            broken = True
            if attempt == 1:
                raise
        except Exception:
            try:
                conn.rollback()
            except Exception:
                broken = True
            raise
        finally:
            try:
                p.putconn(conn, close=broken or conn.closed != 0)
            except Exception:
                pass


def db_execute(query, params=()):
    _run(lambda c: c.execute(query, params))

def db_fetchall(query, params=()):
    def work(c):
        c.execute(query, params)
        return c.fetchall()
    return _run(work)

def db_fetchone(query, params=()):
    rows = db_fetchall(query, params)
    return rows[0] if rows else None

def db_execute_returning(query, params=()):
    """INSERT ... RETURNING 처럼 값을 돌려받으면서 커밋도 해야 하는 경우."""
    def work(c):
        c.execute(query, params)
        return c.fetchone()
    return _run(work)


def init_db():
    db_execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, pin_hash TEXT,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, points INTEGER DEFAULT 0)''')
    # 점수 = AI 심사 점수(100점 만점)의 누적. 새 계정은 0점에서 시작
    db_execute("ALTER TABLE users ALTER COLUMN points SET DEFAULT 0")
    db_execute('''CREATE TABLE IF NOT EXISTS debates (
        id SERIAL PRIMARY KEY, played_at TEXT, topic TEXT,
        player_a TEXT, player_b TEXT, side_a TEXT, side_b TEXT,
        log_json TEXT, winner TEXT, reason TEXT, engine TEXT)''')
    db_execute("ALTER TABLE debates ADD COLUMN IF NOT EXISTS side_a TEXT")
    db_execute("ALTER TABLE debates ADD COLUMN IF NOT EXISTS side_b TEXT")
    # '토론 의지 없음' 판정 대기 / 재심사에 필요한 정보 (AI 원래 판정 등)
    db_execute("ALTER TABLE debates ADD COLUMN IF NOT EXISTS pending_json TEXT")
    # AI 프로필과 설정을 DB에 저장 → Render 재배포에도 유지됨
    db_execute('''CREATE TABLE IF NOT EXISTS ai_profiles (
        name TEXT PRIMARY KEY, provider TEXT, model TEXT, api_key TEXT)''')
    db_execute('''CREATE TABLE IF NOT EXISTS app_settings (
        key TEXT PRIMARY KEY, value TEXT)''')
    # AI 호출 성공/실패 기록 — 무승부·요약 실패의 원인을 눈으로 확인하기 위한 것
    db_execute('''CREATE TABLE IF NOT EXISTS ai_calls (
        id SERIAL PRIMARY KEY, called_at TEXT, kind TEXT, ok BOOLEAN,
        engine TEXT, model TEXT, error TEXT, ms INTEGER)''')


# ─────────────────────────────────────────────
# 설정 / AI 프로필 (DB 기반)
# ─────────────────────────────────────────────
# 설정은 자주 읽으므로 메모리에 들고 있는다 (서버가 하나뿐이라 안전). 쓸 때는 DB와 함께 갱신.
_settings_cache = None

def _load_settings():
    global _settings_cache
    if _settings_cache is None:
        _settings_cache = {k: v for k, v in db_fetchall("SELECT key, value FROM app_settings")}
    return _settings_cache

def get_setting(key, default=None):
    v = _load_settings().get(key)
    return v if v is not None else default

def set_setting(key, value):
    db_execute("INSERT INTO app_settings (key, value) VALUES (%s, %s) "
               "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))
    _load_settings()[key] = value

def list_profiles():
    rows = db_fetchall("SELECT name, provider, model, api_key FROM ai_profiles "
                       "WHERE provider = %s ORDER BY name", (PROVIDER,))
    return [{"name": r[0], "provider": r[1], "model": r[2], "api_key": r[3] or ""} for r in rows]

def upsert_profile(name, provider, model, api_key):
    db_execute("INSERT INTO ai_profiles (name, provider, model, api_key) VALUES (%s,%s,%s,%s) "
               "ON CONFLICT (name) DO UPDATE SET provider = EXCLUDED.provider, "
               "model = EXCLUDED.model, api_key = EXCLUDED.api_key",
               (name, provider, model, api_key))

def delete_profile(name):
    db_execute("DELETE FROM ai_profiles WHERE name = %s", (name,))
    if get_active_name() == name:
        set_setting("active_profile", TEST_MODE)

def get_active_name():
    return get_setting("active_profile", TEST_MODE) or TEST_MODE

def seed_default_profile():
    """
    ChatGPT 전용으로 정리한다.
    - 예전 Gemini/Claude 프로필(과 그 키)은 지운다.
    - 환경변수 OPENAI_API_KEY가 있으면 'ChatGPT' 프로필을 만들어 둔다 (관리자가 넣은 키는 건드리지 않음).
    - 활성 프로필이 사라졌으면 남은 ChatGPT 프로필로, 없으면 테스트 모드로.
    """
    old = db_fetchall("SELECT name FROM ai_profiles WHERE provider <> %s", (PROVIDER,))
    if old:
        db_execute("DELETE FROM ai_profiles WHERE provider <> %s", (PROVIDER,))
        print(f"[정리] ChatGPT 외 프로필 삭제: {[r[0] for r in old]}")
    env_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if env_key:
        row = db_fetchone("SELECT api_key FROM ai_profiles WHERE name = %s", ("ChatGPT",))
        if not row:
            upsert_profile("ChatGPT", PROVIDER, DEFAULT_MODEL, env_key)
        elif not (row[0] or "").strip():
            db_execute("UPDATE ai_profiles SET api_key = %s WHERE name = %s", (env_key, "ChatGPT"))
    db_execute("UPDATE ai_profiles SET model = %s WHERE model IS DISTINCT FROM %s", (DEFAULT_MODEL, DEFAULT_MODEL))
    names = [p["name"] for p in list_profiles()]
    active = get_setting("active_profile")
    if active != TEST_MODE and active not in names:
        set_setting("active_profile", names[0] if names else TEST_MODE)

def get_active_profile():
    """활성 프로필 반환. 테스트 모드거나 키가 없으면 None."""
    active = get_active_name()
    if active == TEST_MODE:
        return None
    row = db_fetchone("SELECT name, provider, model, api_key FROM ai_profiles WHERE name = %s", (active,))
    if not row:
        return None
    name, provider, model, api_key = row
    if not api_key or not api_key.strip():
        return None
    return {"name": name, "provider": provider, "model": model, "api_key": api_key.strip()}

def mask_key(key):
    if not key:
        return ""
    key = key.strip()
    return ("*" * max(0, len(key) - 4)) + key[-4:] if len(key) > 4 else "****"


# ─────────────────────────────────────────────
# LLM 호출
# ─────────────────────────────────────────────
def log_ai_call(kind, ok, error="", ms=0):
    """AI 호출 결과를 남긴다. 실패가 조용히 묻히지 않도록."""
    profile = None
    try:
        profile = get_active_profile()
    except Exception:
        pass
    model = (profile or {}).get("model", "-")
    if profile and _model_redirect.get(model):
        model = f"{model}→{_model_redirect[model]}"
    try:
        db_execute("INSERT INTO ai_calls (called_at, kind, ok, engine, model, error, ms) "
                   "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                   (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), kind, ok,
                    get_active_name(), model, (error or "")[:400], int(ms)))
        # 오래된 기록 정리 (최근 300건만 유지)
        db_execute("DELETE FROM ai_calls WHERE id < "
                   "(SELECT COALESCE(MIN(id),0) FROM (SELECT id FROM ai_calls "
                   "ORDER BY id DESC LIMIT 300) t)")
    except Exception as e:
        print(f"[AI 로그 기록 실패] {e}")


def ai_call_summary():
    """관리자 화면에 보여줄 최근 AI 호출 통계."""
    rows = db_fetchall("SELECT kind, ok, COUNT(*) FROM ai_calls "
                       "WHERE called_at >= %s GROUP BY kind, ok",
                       (datetime.now().strftime("%Y-%m-%d 00:00:00"),))
    stats = {}
    for kind, ok, cnt in rows:
        s = stats.setdefault(kind, {"ok": 0, "fail": 0})
        s["ok" if ok else "fail"] += cnt
    recent = db_fetchall("SELECT called_at, kind, ok, model, error, ms FROM ai_calls "
                         "ORDER BY id DESC LIMIT 15")
    recent_view = [{"at": r[0], "kind": r[1], "ok": r[2], "model": r[3],
                    "error": r[4] or "", "ms": r[5]} for r in recent]
    billing_hit = any("insufficient_quota" in r["error"] for r in recent_view)
    rate_hit = any(("429" in r["error"] or "rate_limit" in r["error"]) and
                   "insufficient_quota" not in r["error"] for r in recent_view)
    return stats, recent_view, billing_hit, rate_hit


# ── 오류 분류 (OpenAI) ─────────────────────────────────────
def _is_auth_error(e):
    s = str(e)
    return "401" in s or "invalid_api_key" in s or "Incorrect API key" in s

def _is_quota_error(e):
    """크레딧/결제 한도 소진. 기다려도 안 풀린다 — 결제 등록이 필요."""
    return "insufficient_quota" in str(e)

def _is_rate_error(e):
    """분당 한도 초과(429). 잠깐 기다리면 풀린다."""
    s = str(e)
    return ("429" in s or "rate_limit" in s) and not _is_quota_error(e)

def _is_model_missing(e):
    s = str(e)
    return "model_not_found" in s or ("404" in s and "model" in s.lower()) or "does not exist" in s

def _is_busy_error(e):
    """서버 일시 오류. 잠시 후 재시도하면 풀린다."""
    s = str(e)
    return any(k in s for k in ("500", "502", "503", "overloaded", "server_error",
                                "Timeout", "timed out", "APIConnectionError"))

def _retry_after_seconds(e, default=5):
    """'Please try again in 1.5s' / '...in 300ms' 같은 안내에서 대기 시간을 뽑아낸다."""
    import re as _re
    s = str(e)
    m = _re.search(r"try again in ([0-9.]+)\s*ms", s)
    if m:
        return min(float(m.group(1)) / 1000 + 0.5, 60)
    m = _re.search(r"try again in ([0-9.]+)\s*s", s)
    if m:
        return min(float(m.group(1)) + 0.5, 60)
    return default


# ── 분당 호출량 제한 ───────────────────────────────────────
from collections import deque
_call_times = deque()

def current_rpm():
    now = time.time()
    while _call_times and now - _call_times[0] > 60:
        _call_times.popleft()
    return len(_call_times)

def max_rpm():
    try:
        return int(get_setting("ai_max_rpm_v2", str(DEFAULT_MAX_RPM)) or DEFAULT_MAX_RPM)
    except (TypeError, ValueError):
        return DEFAULT_MAX_RPM

def rate_gate(priority="high", max_wait=70):
    """
    호출 허가. 낮은 우선순위(요약·주제 창고)는 붐비면 즉시 포기한다.
    높은 우선순위도 무한정 기다리지 않는다 — 최대 max_wait초.
    """
    limit = max_rpm()
    deadline = time.time() + max_wait
    while True:
        if current_rpm() < limit:
            _call_times.append(time.time())
            return True
        if priority == "low" or time.time() >= deadline:
            return False
        wait = max(0.5, 60 - (time.time() - _call_times[0]) + 0.3)
        _pause(min(wait, 10, max(0.5, deadline - time.time())))

def _pause(seconds):
    """gevent 워커를 막지 않는 대기."""
    try:
        socketio.sleep(seconds)
    except Exception:
        time.sleep(seconds)


class QuotaExceeded(Exception):
    """한도 초과. retry_after>0이면 잠시 후 재시도로 풀릴 수 있다."""
    def __init__(self, message, retry_after=0):
        super().__init__(message)
        self.retry_after = retry_after


# 모델마다 지원하지 않는 옵션 (예: gpt-4o-mini는 reasoning_effort를 모른다) — 한 번 겪으면 기억
_unsupported_params = {}
# 지정 모델이 없을 때 실제로 쓰게 된 모델
_model_redirect = {}


def _openai_once(api_key, model, prompt, json_schema=None, effort="low"):
    from openai import OpenAI
    client = OpenAI(api_key=api_key, timeout=90, max_retries=0)
    drop = _unsupported_params.setdefault(model, set())
    for _ in range(3):
        kwargs = {"model": model, "messages": [{"role": "user", "content": prompt}]}
        if effort and "reasoning_effort" not in drop:
            kwargs["reasoning_effort"] = effort
        if json_schema and "response_format" not in drop:
            kwargs["response_format"] = {"type": "json_schema", "json_schema": json_schema}
        try:
            r = client.chat.completions.create(**kwargs)
            return (r.choices[0].message.content or "").strip()
        except Exception as e:
            s = str(e)
            if "400" in s or "BadRequest" in type(e).__name__:
                if "reasoning_effort" in kwargs and "reasoning" in s.lower():
                    drop.add("reasoning_effort")
                    continue
                if "response_format" in kwargs and ("response_format" in s or "json_schema" in s):
                    drop.add("response_format")
                    continue
            raise
    raise RuntimeError("요청 옵션을 맞추지 못했습니다.")


def _openai_call(api_key, model, prompt, json_schema=None):
    """
    - 모델이 없으면(404) 예비 모델로 넘어가고, 다음부터는 바로 그 모델을 쓴다.
    - 서버 일시 오류는 1회 재시도.
    - 한도 초과는 QuotaExceeded로 올려서 호출한 쪽이 기다릴지 결정하게 한다.
    """
    start = _model_redirect.get(model, model)
    candidates = [start] + [m for m in FALLBACK_MODELS if m != start]
    last_err = None
    first = True
    for cand in candidates:
        for attempt in range(2):
            if not first:
                _call_times.append(time.time())   # 재시도도 한도에 포함
            first = False
            try:
                out = _openai_once(api_key, cand, prompt, json_schema)
                if cand != model:
                    if _model_redirect.get(model) != cand:
                        print(f"[모델 대체] {model} 없음 → {cand}")
                    _model_redirect[model] = cand
                return out
            except Exception as e:
                last_err = e
                if _is_quota_error(e):
                    raise QuotaExceeded(str(e), 0)
                if _is_rate_error(e):
                    raise QuotaExceeded(str(e), _retry_after_seconds(e, 5))
                if _is_busy_error(e) and attempt == 0:
                    _pause(1.5)
                    continue
                break
        if not _is_model_missing(last_err):
            break
    raise last_err


def call_one_profile(profile, prompt, json_schema=None):
    return _openai_call(profile["api_key"], profile["model"], prompt, json_schema)


def backup_profiles():
    """활성 키가 크레딧 소진·한도에 걸렸을 때 대신 쓸 키들 (등록된 것만)."""
    active = get_active_name()
    return [p for p in list_profiles()
            if p["name"] != active and (p.get("api_key") or "").strip()]


def llm_call(prompt, max_tokens=None, raise_errors=False, priority="high", json_schema=None):
    """
    priority='high'  — 심사·주제. 자리가 날 때까지 기다린다.
    priority='low'   — 발언 요약·주제 창고. 한도가 빠듯하면 건너뛴다.
    """
    profile = get_active_profile()
    if not profile:
        if raise_errors:
            raise RuntimeError("활성 AI가 없거나 API 키가 비어 있습니다. (테스트 모드)")
        return None

    if not rate_gate(priority):
        msg = "분당 호출 상한에 걸려 이번 요청은 건너뜁니다."
        if raise_errors:
            raise QuotaExceeded(msg, 5)
        return None

    try:
        return call_one_profile(profile, prompt, json_schema)
    except QuotaExceeded as quota_err:
        for backup in backup_profiles():
            if not rate_gate("high"):
                break
            try:
                result = call_one_profile(backup, prompt, json_schema)
                print(f"[키 전환] {profile['name']} 한도 → {backup['name']}로 처리")
                return result
            except Exception:
                continue
        print(f"[LLM 한도] {quota_err}")
        if raise_errors:
            raise
        return None
    except Exception as e:
        print(f"[LLM 호출 실패] {type(e).__name__}: {e}")
        if raise_errors:
            raise
        return None


def _sdk_version():
    try:
        import importlib.metadata as md
        return md.version("openai")
    except Exception:
        return "미설치"


def explain_ai_error(e):
    """관리자가 바로 조치할 수 있게 원인을 한국어로 풀어준다."""
    if _is_quota_error(e):
        return "크레딧/결제 한도 소진(insufficient_quota) — platform.openai.com에서 결제 수단·크레딧을 확인하세요."
    if _is_rate_error(e):
        return "분당 사용량 한도 초과(429) — 잠시 후 자동으로 풀립니다."
    if _is_auth_error(e):
        return "API 키가 잘못됐습니다(401) — 키를 다시 복사해 등록하세요."
    if _is_model_missing(e):
        return "모델명을 찾을 수 없습니다(404) — 모델명을 확인하세요."
    return ""


def test_llm_connection():
    """관리자 페이지의 '연결 테스트' 버튼용. 실패 원인을 화면에 그대로 표시."""
    profile = get_active_profile()
    if not profile:
        return False, "활성 AI가 없거나 API 키가 비어 있습니다. (테스트 모드)"
    info = (f"{profile['name']} | {profile['model']} | SDK openai {_sdk_version()} | "
            f"키 {mask_key(profile['api_key'])}")
    try:
        # 예비 키로 넘어가지 않고 '선택한 키' 하나만 시험한다
        _call_times.append(time.time())
        result = call_one_profile(profile, "'연결 성공'이라고만 답해.")
        note = ""
        if _model_redirect.get(profile["model"]):
            note = (f"\n⚠️ '{profile['model']}' 모델이 없어 '{_model_redirect[profile['model']]}'로 "
                    f"대신 처리했습니다. 모델명을 고쳐두세요.")
        if result:
            return True, f"{info}\n응답: {result[:80]}{note}"
        return False, f"{info}\n예외 없이 빈 응답이 돌아왔습니다."
    except Exception as e:
        hint = explain_ai_error(e)
        return False, f"{info}\n{('→ ' + hint + chr(10)) if hint else ''}{type(e).__name__}: {str(e)[:400]}"


# ─────────────────────────────────────────────
# 사용자 / 토론 기록
# ─────────────────────────────────────────────
def hash_pin(username, pin):
    return hashlib.sha256(f"{username}:{pin}:academia".encode()).hexdigest()

def get_user_stats(username):
    row = db_fetchone("SELECT wins, losses, points FROM users WHERE username = %s", (username,))
    if not row:
        return None
    rank_row = db_fetchone("SELECT COUNT(*) FROM users WHERE points > %s", (row[2],))
    return {"wins": row[0], "losses": row[1], "points": row[2], "rank": rank_row[0] + 1}

def add_points(username, n):
    db_execute("UPDATE users SET points = points + %s WHERE username = %s", (n, username))

def record_win_loss(winner, loser):
    """승패만 기록한다. 점수는 심사 점수로 따로 쌓는다 (add_points)."""
    db_execute("UPDATE users SET wins = wins + 1 WHERE username = %s", (winner,))
    db_execute("UPDATE users SET losses = losses + 1 WHERE username = %s", (loser,))

def save_debate(topic, player_a, player_b, side_a, side_b, logs, winner, reason, pending=None):
    """토론 기록 저장. 새 기록의 id를 돌려준다 (판정 대기·재심사에서 다시 찾기 위해)."""
    engine = get_active_name()
    row = db_execute_returning(
        "INSERT INTO debates (played_at, topic, player_a, player_b, side_a, side_b, log_json, "
        "winner, reason, engine, pending_json) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id",
        (datetime.now().strftime("%Y-%m-%d %H:%M"), topic, player_a, player_b, side_a, side_b,
         json.dumps(logs, ensure_ascii=False), winner, reason, engine,
         json.dumps(pending, ensure_ascii=False) if pending else None))
    return row[0] if row else None


init_db()
seed_default_profile()
# 서버가 뜨자마자 주제를 미리 만들어둔다 (첫 매칭부터 AI 주제가 나오도록)
socketio.start_background_task(lambda: refill_topic_cache())


# ─────────────────────────────────────────────
# AI 기능
# ─────────────────────────────────────────────
_QUESTION_ENDINGS = [
    ("되어야 하는가", "되어야 한다"), ("돼야 하는가", "돼야 한다"),
    ("해야 하는가", "해야 한다"), ("해야 할까", "해야 한다"),
    ("야 하는가", "야 한다"), ("야 할까", "야 한다"),
    ("할 수 있는가", "할 수 있다"), ("수 있는가", "수 있다"),
    ("하는가", "한다"), ("되는가", "된다"), ("있는가", "있다"),
    ("옳은가", "옳다"), ("정당한가", "정당하다"), ("바람직한가", "바람직하다"),
    ("필요한가", "필요하다"), ("가능한가", "가능하다"), ("타당한가", "타당하다"),
    ("한가", "하다"), ("인가", "이다"), ("는가", "다"), ("을까", "다"), ("까", "다"),
]
# 긴 어미부터 검사해야 '낮춰야 하는가' 가 '하는가' 규칙에 먼저 걸리지 않는다
QUESTION_ENDINGS = sorted(_QUESTION_ENDINGS, key=lambda x: -len(x[0]))


def to_statement(topic):
    """AI가 의문문으로 만들어 오면 평서문으로 고친다."""
    t = (topic or "").strip().rstrip("?？").strip()
    for q, s in QUESTION_ENDINGS:
        if t.endswith(q):
            t = t[: -len(q)] + s
            break
    if not t.endswith("."):
        t += "."
    return t


def get_ai_topic(allow_fallback=True, priority="high"):
    recent_rows = db_fetchall("SELECT topic FROM debates ORDER BY id DESC LIMIT 30")
    # 창고에 쌓아둔 것 + 지금 진행 중인 토론 + 최근 기록을 모두 제외 대상에 넣는다
    recent_topics = list(dict.fromkeys(
        list(topic_cache) + list(topics_in_use()) + [r[0] for r in recent_rows]))[:25]
    recent_list = "\n".join(f"- {t}" for t in recent_topics) if recent_topics else "(없음)"
    prompt = f"""고등학생 토론 동아리의 1대1 즉흥 찬반 토론에 쓸 주제를 하나 추천해줘.

조건:
- 반드시 '~해야 한다', '~이다' 같은 **긍정의 평서문 한 문장**으로 쓸 것.
  의문문(~하는가?)이나 부정문(~하지 말아야 한다)은 절대 쓰지 말 것.
  찬성 측이 그 문장을 그대로 옹호하고, 반대 측이 반박하는 구조여야 한다.
- 찬성/반대가 명확히 갈리는 주제일 것
- 사형제, 인공지능의 위험성처럼 너무 뻔하고 진부한 주제는 피하고, 시사성 있거나 참신한 주제로
- 고등학생 수준에서 전문 지식 없이도 논리적으로 다룰 수 있을 것
- 아래 최근 사용된 주제와 겹치지 않을 것

[좋은 예]
- 학교는 학생의 스마트폰 사용을 제한해야 한다.
- 선거 연령을 더 낮춰야 한다.

[나쁜 예]
- 학교가 스마트폰을 제한해야 하는가?  ← 의문문이라 안 됨
- 선거 연령을 낮추면 안 된다.  ← 부정문이라 안 됨

[최근 사용된 주제]
{recent_list}

다른 설명 없이 주제 문장 한 줄만 출력해."""
    started = time.time()
    try:
        result = llm_call(prompt, raise_errors=True, priority=priority)
        log_ai_call("주제추천", True, "", (time.time() - started) * 1000)
        return to_statement(result.strip().strip('"').strip("'").splitlines()[0])
    except Exception as e:
        log_ai_call("주제추천", False, f"{type(e).__name__}: {e}", (time.time() - started) * 1000)
        if not allow_fallback:
            return None   # 창고를 채우는 중이었다면 예비 주제로 채우지 않고 나중에 다시 시도
        # AI가 안 되면 미리 준비된 주제 중 최근에 안 쓴 것을 고른다
        unused = [t for t in BACKUP_TOPICS if t not in recent_topics]
        return random.choice(unused or BACKUP_TOPICS)


def summaries_enabled():
    return get_setting("summary_enabled", "1") != "0"


# ── 주제 미리 만들어두기 ──────────────────────────────────
# 매칭 도중에 AI를 기다리면, 한도에 걸렸을 때 두 사람이 몇 분씩 묶인다.
# 그래서 주제는 평소에 미리 만들어 쌓아두고, 매칭 때는 꺼내 쓰기만 한다.
TOPIC_CACHE_SIZE = 7
topic_cache = []
_refilling = {"on": False}

def topics_in_use():
    """지금 진행 중인 토론들의 주제 (아직 DB에 저장되기 전이라 따로 봐야 한다)."""
    return {r['topic'] for r in rooms.values()}


def recent_db_topics(limit=10):
    try:
        rows = db_fetchall("SELECT topic FROM debates ORDER BY id DESC LIMIT %s", (limit,))
        return {r[0] for r in rows}
    except Exception:
        return set()


def take_topic():
    """
    즉시 반환. 이미 진행 중인 주제는 절대 다시 내주지 않는다.
    """
    in_use = topics_in_use()

    # 1) 미리 만들어둔 것 중 아직 안 쓰인 주제
    while topic_cache:
        candidate = topic_cache.pop(0)
        if candidate not in in_use:
            socketio.start_background_task(refill_topic_cache)
            return candidate
        # 이미 쓰이고 있는 주제였다면 버리고 다음 것을 본다

    # 2) 예비 주제 중 최근에 안 쓴 것
    recently_used = in_use | recent_db_topics()
    unused = [t for t in BACKUP_TOPICS if t not in recently_used]
    if not unused:
        # 최근 기록까지 따지면 남는 게 없을 때 — 최소한 동시 진행 중복만은 피한다
        unused = [t for t in BACKUP_TOPICS if t not in in_use]

    socketio.start_background_task(refill_topic_cache)
    return random.choice(unused) if unused else random.choice(BACKUP_TOPICS)

def refill_topic_cache():
    if _refilling["on"]:
        return
    _refilling["on"] = True
    try:
        misses = 0
        while len(topic_cache) < TOPIC_CACHE_SIZE:
            # 창고 채우기는 낮은 우선순위 — 심사·주제 즉시 요청이 밀리지 않게 한다.
            # 실패하면 예비 주제로 채우지 않고 멈춘다 (다음 매칭 때 다시 시도)
            topic = get_ai_topic(allow_fallback=False, priority="low")
            if not topic:
                break
            # 창고 안, 진행 중인 토론, 최근 기록과 겹치면 버린다
            if topic in topic_cache or topic in topics_in_use() or topic in recent_db_topics(20):
                misses += 1
                if misses >= 3:
                    break   # 같은 주제만 반복되면 그만
                continue
            topic_cache.append(topic)
    except Exception as e:
        print(f"[주제 준비 실패] {e}")
    finally:
        _refilling["on"] = False


def get_ai_summary(text):
    if not summaries_enabled():
        return None
    started = time.time()
    try:
        result = llm_call(f"다음 토론 발언을 한 문장으로 핵심만 요약해줘:\n{text}",
                          max_tokens=200, raise_errors=True, priority="low")
        log_ai_call("발언요약", True, "", (time.time() - started) * 1000)
        return result
    except Exception as e:
        log_ai_call("발언요약", False, f"{type(e).__name__}: {e}", (time.time() - started) * 1000)
        return None

def draws_allowed():
    return get_setting("allow_draw", "0") == "1"


def _judge_schema(allow_draw):
    winners = ["찬성", "반대"] + (["무승부"] if allow_draw else [])
    item_keys = [k for _, items in RUBRIC for k, _, _ in items]
    side_obj = {"type": "object", "additionalProperties": False, "required": item_keys,
                "properties": {k: {"type": "integer"} for k in item_keys}}
    return {
        "name": "debate_verdict",
        "strict": True,
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "required": ["pro", "con", "winner", "reason", "no_will_side", "no_will_reason"],
            "properties": {
                "pro": side_obj,
                "con": side_obj,
                "winner": {"type": "string", "enum": winners},
                "reason": {"type": "string"},
                "no_will_side": {"type": "string", "enum": ["없음", "찬성", "반대", "둘다"]},
                "no_will_reason": {"type": "string"},
            },
        },
    }


def _score_side(raw):
    """AI가 준 항목 점수를 배점 범위로 자르고, 단계별·총점은 서버가 직접 계산한다."""
    raw = raw or {}
    stages, total = [], 0
    for stage, items in RUBRIC:
        sub = 0
        for key, _, pts in items:
            try:
                v = int(raw.get(key, 0))
            except (TypeError, ValueError):
                v = 0
            sub += max(0, min(pts, v))
        stages.append((stage, sub))
        total += sub
    return total, stages


def _log_line(log):
    line = f"[{log['stage']}] {log['side']}: {log['text']}"
    if log.get('auto') and log['text'] != EMPTY_MESSAGE:
        line += f"\n   ({AUTO_NOTE})"
    return line


def judge_debate(topic, logs):
    """
    반환: {"ok": bool, "winner": 찬성/반대/무승부, "text": 결과 문구,
           "no_will_side": 없음/찬성/반대/둘다, "no_will_reason": str}
    실패하면 ok=False, winner='심사실패'.
    """
    allow_draw = draws_allowed()
    log_text = "\n".join(_log_line(l) for l in logs)
    rubric_text = "\n".join(
        f"{i}. {stage} (25점)\n" + "\n".join(f"   - {key}: {name} — {pts}점 만점" for key, name, pts in items)
        for i, (stage, items) in enumerate(RUBRIC, start=1))
    draw_rule = ("- 총점이 같고 실력이 정말 대등할 때만 winner를 '무승부'로 할 수 있다."
                 if allow_draw else
                 "- 이 대회는 토너먼트라 무승부가 없다. winner는 반드시 '찬성' 또는 '반대'다. "
                 "총점이 같을 때 이 값으로 승자를 정하므로, 조금이라도 나은 쪽을 고르라.")
    prompt = f"""너는 고등학교 토론 동아리의 심사위원이다. 아래 1대1 찬반 토론을 세부 항목별로 엄격하고 일관되게 채점하라.

[채점표 — 100점 만점, 찬성(pro)과 반대(con)를 각각 채점]
{rubric_text}

[채점 원칙]
- 항목마다 0점부터 배점까지 정수로 매긴다. 만점은 흠잡을 데 없을 때만 준다.
- 대략 기준: 배점의 90% 이상 = 탁월, 70~80% = 좋음, 50~60% = 보통, 30~40% = 부족, 20% 이하 = 매우 부족.
- 입론은 두 사람이 서로의 글을 보지 않고 동시에 작성했다. 입론에서 상대를 반박하지 않은 것은 감점하지 않는다.
- 발언 순서: 반론·반박은 찬성이 먼저, 최종변론은 반대가 먼저 말했다. 나중에 말한 쪽이 상대 글을 보고 썼다는 점을 감안하라.
- 글이 길다고 점수를 더 주지 않는다. 같은 말 반복은 감점한다.
- '{AUTO_NOTE}' 표시가 붙은 발언은 제한 시간이 끝나 쓰던 도중에 제출된 것이다. 적힌 내용만으로 채점하되,
  문장이 중간에 끊긴 것 자체를 따로 감점하지는 않는다. 다만 완성되지 않아 전달되지 못한 논지는 점수로 인정하지 않는다.
- '{EMPTY_MESSAGE}'로 적힌 발언은 그 단계 모든 항목 0점이다.

[상대 주장의 재구성(허수아비 공격)]
- 입론·반론·반박 단계에서 상대 주장을 자기에게 유리하게 재구성해 공격하는 것은 토론 기술로 보고 감점하지 않는다.
- 대신 상대가 그 왜곡을 정확히 지적해 무너뜨리면, 지적한 쪽의 해당 항목(허점 포착·직접 대응·재반박)을 높게 평가한다.
- 단, 최종변론에서 처음 꺼낸 왜곡은 상대가 반박할 기회가 없으므로 점수로 인정하지 않는다 (감점은 하지 않는다).

{draw_rule}

[토론 의지 없음 판정 — 매우 보수적으로]
다음에 해당할 때만 그 측을 no_will_side로 지목하라.
- 주제와 전혀 관계없는 내용(잡담, 장난, 욕설, 다른 주제 이야기)을 반복했다
- 의미 없는 글자·같은 문장 반복 등으로 글자 수만 채웠다
- 자기 진영과 반대되는 주장을 일부러 펼쳐 토론을 망쳤다
다음은 절대 해당하지 않는다: 논리가 약함, 근거가 부족함, 엉뚱하거나 서툰 주장, 맞춤법 실수,
짧지만 주제에 대해 성실히 쓴 발언, 시간이 끝나 자동 제출되거나 비어 있는 발언이 몇 번 있었던 경우.
조금이라도 애매하면 '없음'으로 하라. 해당하면 no_will_reason에 구체적인 발언을 근거로 한 문장으로 쓰고,
해당하지 않으면 no_will_reason은 빈 문자열로 둔다.

[주제] {topic}
[토론 로그 — 단계 순서대로]
{log_text}

reason에는 승패를 가른 결정적 차이를 단계별 근거와 함께 5문장 이내로 쓴다.
반드시 지정된 JSON 형식으로만 답하라. pro와 con에는 위 채점표의 키마다 정수 점수를 넣는다."""
    schema = _judge_schema(allow_draw)
    last_error = ""
    for attempt in range(3):
        started = time.time()
        try:
            raw = llm_call(prompt, raise_errors=True, json_schema=schema)
            data = json.loads(raw.replace("```json", "").replace("```", "").strip())
            if not isinstance(data.get("pro"), dict) or not isinstance(data.get("con"), dict):
                raise ValueError("항목별 점수가 없습니다")
            pro_total, pro_stages = _score_side(data["pro"])
            con_total, con_stages = _score_side(data["con"])
            # 승자는 서버가 계산한 총점으로 정한다. 동점일 때만 AI의 판단(winner)을 따른다.
            if pro_total > con_total:
                winner = "찬성"
            elif con_total > pro_total:
                winner = "반대"
            else:
                ai_pick = data.get("winner", "")
                if allow_draw and ai_pick == "무승부":
                    winner = "무승부"
                elif ai_pick in ("찬성", "반대"):
                    winner = ai_pick
                else:
                    winner = "무승부" if allow_draw else random.choice(["찬성", "반대"])
            no_will = data.get("no_will_side", "없음")
            if no_will not in ("없음", "찬성", "반대", "둘다"):
                no_will = "없음"
            log_ai_call("심사", True, "", (time.time() - started) * 1000)
            verdict_line = f"판정: {winner}측 승리" if winner != "무승부" else "판정: 무승부"
            tie_note = " (동점 — 세부 내용으로 승자 결정)" if pro_total == con_total and winner != "무승부" else ""
            stage_line = " · ".join(f"{st} {p}:{c}" for (st, p), (_, c) in zip(pro_stages, con_stages))
            text = (f"{verdict_line}{tie_note}\n"
                    f"점수: 찬성 {pro_total}점 vs 반대 {con_total}점 ({TOTAL_POINTS}점 만점)\n"
                    f"단계별(찬성:반대, 각 25점): {stage_line}\n"
                    f"근거: {data.get('reason','근거 없음')}")
            return {"ok": True, "winner": winner, "text": text,
                    "pro_total": pro_total, "con_total": con_total,
                    "no_will_side": no_will, "no_will_reason": data.get("no_will_reason", "")}
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log_ai_call("심사", False, last_error, (time.time() - started) * 1000)
            if isinstance(e, QuotaExceeded) and not e.retry_after:
                break   # 크레딧 소진은 기다려도 안 풀린다
            if attempt < 2:
                wait = e.retry_after if isinstance(e, QuotaExceeded) else 2 * (attempt + 1)
                print(f"[심사 재시도] {wait:.0f}초 대기")
                _pause(max(wait, 1))

    hint = explain_ai_error(Exception(last_error))
    return {"ok": False, "winner": "심사실패", "no_will_side": "없음", "no_will_reason": "",
            "text": ("⚠️ AI 심사에 실패했습니다.\n"
                     "승패는 아직 기록되지 않았으며, 운영자가 관리자 화면에서 다시 심사할 수 있습니다.\n"
                     f"원인: {hint or last_error[:200]}")}


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>동아리 익명 1대1 토론 리그</title>
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<style>
body{font-family:'Malgun Gothic',sans-serif;max-width:600px;margin:30px auto;padding:20px;background:#f3f4f6;}
#turn-status{white-space:pre-line;}
.box{border:1px solid #e5e7eb;padding:20px;background:white;margin-bottom:15px;border-radius:12px;box-shadow:0 4px 6px -1px rgba(0,0,0,0.1);}
#chat-box{height:320px;overflow-y:auto;border:1px solid #e5e7eb;padding:15px;background:#f9fafb;margin-bottom:15px;border-radius:8px;}
.hidden{display:none;} .turn-active{background-color:#eff6ff;border:2px solid #3b82f6;}
.msg-item{margin-bottom:12px;} .summary-item{color:#6b7280;font-size:0.85em;margin-top:2px;padding-left:10px;border-left:2px solid #d1d5db;}
.engine-tag{font-size:0.8em;color:#9ca3af;text-align:right;} .stats{background:#f0fdf4;border:1px solid #bbf7d0;padding:10px 14px;border-radius:8px;font-size:0.95em;}
body{-webkit-user-select:none;user-select:none;} input,textarea{user-select:auto;}
#stage-dots{display:flex;gap:8px;justify-content:center;margin-bottom:10px;}
.dot{width:12px;height:12px;border-radius:50%;background:#d1d5db;transition:background .3s,transform .3s;}
.dot.done{background:#93c5fd;} .dot.current{background:#2563eb;transform:scale(1.3);}
#stage-banner{text-align:center;font-size:1.1em;font-weight:bold;color:white;background:linear-gradient(90deg,#2563eb,#7c3aed);padding:10px;border-radius:8px;margin-bottom:10px;}
@keyframes stagePop{0%{transform:scale(.6) translateY(-10px);opacity:0;}60%{transform:scale(1.06);opacity:1;}100%{transform:scale(1);opacity:1;}}
.stage-pulse{animation:stagePop .55s ease;}
.side-tag{display:inline-block;padding:2px 8px;border-radius:6px;font-size:0.85em;font-weight:bold;}
.side-pro{background:#dcfce7;color:#166534;} .side-con{background:#fee2e2;color:#991b1b;}
.stage-label{color:#7c3aed;font-weight:bold;}
input,textarea{font-size:16px;background:#fff;color:#111827;-webkit-appearance:none;appearance:none;
  border:1px solid #d1d5db;border-radius:6px;font-family:inherit;}
button{-webkit-appearance:none;appearance:none;border:1px solid #93c5fd;border-radius:6px;
  background:#2563eb;color:#ffffff !important;-webkit-text-fill-color:#ffffff;
  cursor:pointer;font-size:15px;font-weight:bold;font-family:inherit;}
button:disabled{background:#9ca3af;border-color:#9ca3af;cursor:not-allowed;}
.summary-item.pending{color:#9ca3af;font-style:italic;}
/* 찬성 / 반대 구분 */
.msg-item{padding:8px 10px;border-radius:8px;border-left:4px solid #d1d5db;background:#fff;}
.msg-item.pro{border-left-color:#16a34a;background:#f0fdf4;}
.msg-item.con{border-left-color:#dc2626;background:#fef2f2;}
.msg-item.pro strong{color:#166534;} .msg-item.con strong{color:#991b1b;}
.summary-item.pro{border-left-color:#86efac;} .summary-item.con{border-left-color:#fca5a5;}
#turn-status.waiting{color:#6b7280;}
/* 내가 어느 편인지 한눈에 */
#side-banner{padding:14px;border-radius:10px;margin-bottom:12px;text-align:center;
  border:2px solid transparent;}
#side-banner.pro{background:#dcfce7;border-color:#16a34a;}
#side-banner.con{background:#fee2e2;border-color:#dc2626;}
#side-banner .big{font-size:2.1em;font-weight:bold;display:block;margin-bottom:6px;
  letter-spacing:0.08em;}
#side-banner.pro .big{color:#166534;} #side-banner.con .big{color:#991b1b;}
#side-banner .desc{font-size:0.9em;color:#374151;line-height:1.5;}
#side-banner .claim{font-weight:bold;}
#turn-status.urgent{color:#dc2626;font-size:1.15em;}
.nav{margin-top:14px;font-size:0.88em;}
.nav a{color:#2563eb;text-decoration:none;} .nav a:hover{text-decoration:underline;}
#len-hint{text-align:right;font-size:0.8em;color:#9ca3af;margin-top:2px;}
#len-hint.short{color:#dc2626;font-weight:bold;}
/* 항복은 실수로 눌리지 않도록 접어두고, 전송 버튼에서 멀리 떨어뜨린다 */
#surrender-wrap{margin-top:22px;border-top:1px dashed #e5e7eb;padding-top:10px;}
#surrender-wrap summary{font-size:0.8em;color:#9ca3af;cursor:pointer;list-style:none;}
#surrender-wrap summary::-webkit-details-marker{display:none;}
#surrender-wrap summary::before{content:'▸ ';}
#surrender-wrap[open] summary::before{content:'▾ ';}
#surrender-wrap .warn{font-size:0.8em;color:#9ca3af;margin:8px 0 6px;}
#surrender-btn{width:auto;padding:6px 14px;background:#fff !important;
  color:#b91c1c !important;-webkit-text-fill-color:#b91c1c;border:1px solid #fca5a5;
  font-size:0.85em;font-weight:normal;}
#surrender-btn:hover{background:#fef2f2 !important;}
#criteria-box{margin-top:14px;padding:10px 12px;background:#fafafa;border:1px solid #eee;
  border-radius:8px;font-size:0.78em;color:#6b7280;line-height:1.6;}
#criteria-box b{color:#4b5563;}
#criteria-box .ct{color:#7c3aed;font-weight:bold;}
@keyframes fadePulse{0%,100%{opacity:.45;}50%{opacity:1;}}
.summary-item.pending{animation:fadePulse 1.4s ease-in-out infinite;}
#chat-box{scroll-behavior:smooth;}
#result-overlay{position:fixed;inset:0;background:rgba(17,24,39,.55);z-index:1000;display:flex;
  align-items:center;justify-content:center;padding:16px;}
#result-overlay .result-card{background:#fff;border-radius:14px;max-width:560px;width:100%;max-height:85vh;
  overflow-y:auto;padding:20px;box-shadow:0 10px 30px rgba(0,0,0,.3);}
#result-overlay h3{margin:0 0 12px;}
#result-overlay .result-body{white-space:pre-wrap;line-height:1.6;font-size:0.95em;color:#111827;
  user-select:text;-webkit-user-select:text;}
#result-overlay button{width:100%;margin-top:16px;padding:12px;}
</style>
</head>
<body>
<h2>🗣️ 동아리 익명 1대1 토론 시스템</h2>
<p class="engine-tag">🤖 현재 AI 엔진: {{ engine_name }}</p>
<div id="login-area" class="box">
  <p style="font-size:0.9em;color:#555;">운영자가 발급한 아이디로 로그인하세요.<br>🎭 토론 중에는 상대가 누구인지 공개되지 않습니다 (끝나면 공개).</p>
  <input type="text" id="username" placeholder="아이디" style="padding:8px;width:40%;">
  <input type="password" id="pin" placeholder="PIN" style="padding:8px;width:35%;">
  <button onclick="login()" style="padding:8px 15px;cursor:pointer;">로그인</button>
  <p id="status-text" style="color:#2563eb;font-weight:bold;margin-top:10px;"></p>
  <p class="nav"><a href="/ranking">🏆 랭킹 보기</a> · <a href="/history">📚 토론 기록</a> · <a href="/admin">🛠️ 관리자</a></p>
</div>
<div id="profile-area" class="box hidden">
  <p>👤 <b id="my-name"></b>님, 환영합니다!</p>
  <p class="stats" id="my-stats"></p>
  <details id="last-result" class="hidden" style="background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:10px 12px;margin:8px 0;">
    <summary style="cursor:pointer;font-weight:bold;"></summary>
    <div class="lr-body" style="white-space:pre-wrap;font-size:0.9em;color:#374151;margin-top:8px;user-select:text;-webkit-user-select:text;"></div>
  </details>
  <p style="font-size:0.9em;color:#555;">📊 점수는 매 경기 AI 심사 점수(100점 만점)가 승패와 관계없이 누적됩니다.</p>
  <button onclick="joinQueue()" id="match-btn" style="padding:10px 20px;cursor:pointer;background:#2563eb;color:white;border:none;border-radius:6px;font-weight:bold;">토론 매칭 시작</button>
  <button onclick="cancelQueue()" id="cancel-btn" class="hidden" style="padding:10px 16px;cursor:pointer;background:#fff !important;color:#dc2626 !important;-webkit-text-fill-color:#dc2626;border:1px solid #fca5a5;border-radius:6px;font-weight:bold;">매칭 취소</button>
  <p id="queue-text" style="color:#2563eb;font-weight:bold;margin-top:10px;"></p>
  <p class="nav"><a href="/ranking">🏆 랭킹 보기</a> · <a href="/history">📚 토론 기록</a> · <a href="/admin">🛠️ 관리자</a> · <a href="#" onclick="logout();return false;">로그아웃</a></p>
</div>
<div id="debate-area" class="box hidden">
  <div id="stage-dots"></div>
  <div id="stage-banner">단계 준비 중...</div>
  <div id="side-banner"></div>
  <h3 id="topic-area" style="color:#1f2937;">주제: 추천 중...</h3>
  <p id="role-area" style="font-weight:bold;color:#4b5563;"></p>
  <p id="turn-status" style="color:#dc2626;font-weight:bold;margin-bottom:15px;"></p>
  <div id="chat-box"></div>
  <textarea id="msg-input" style="width:96%;height:70px;padding:10px;border-radius:6px;border:1px solid #ccc;" placeholder="여기에 논리를 펼쳐주세요 (복사/붙여넣기 금지)&#10;상대 차례에도 미리 작성할 수 있습니다. Ctrl(⌘)+엔터로 전송"></textarea>
  <div id="len-hint">0자</div>
  <button id="send-btn" onclick="sendMessage()" style="width:100%;margin-top:8px;padding:10px;background:#2563eb;color:white;border:none;border-radius:6px;cursor:pointer;font-weight:bold;">발언 완료 — 전송 (Ctrl+Enter)</button>
  <div id="criteria-box"></div>
  <details id="surrender-wrap">
    <summary>기타</summary>
    <p class="warn">항복하면 <b>패배로 기록</b>되고 토론이 즉시 끝납니다.<br>
    (창을 닫거나 연결이 끊겨도 패배하지 않습니다 — 다시 접속하면 이어서 진행)</p>
    <button id="surrender-btn" onclick="surrender()">🏳️ 항복하기</button>
  </details>
</div>
<script>
// 운영자가 관리자 화면에서 '복사·붙여넣기 허용'을 켜면 아래 차단이 모두 풀린다 (테스트용)
const ALLOW_PASTE={{ 'true' if allow_paste else 'false' }};
if(ALLOW_PASTE)document.body.style.userSelect='auto';
document.addEventListener('contextmenu',e=>{if(ALLOW_PASTE)return;e.preventDefault();alert('우클릭 금지');});
document.addEventListener('copy',e=>{if(!ALLOW_PASTE)e.preventDefault();});
document.addEventListener('cut',e=>{if(!ALLOW_PASTE)e.preventDefault();});
document.addEventListener('paste',e=>{if(ALLOW_PASTE)return;e.preventDefault();alert('⚠️ 붙여넣기 금지!');});
document.addEventListener('drop',e=>{if(!ALLOW_PASTE)e.preventDefault();});
document.addEventListener('dragover',e=>{if(!ALLOW_PASTE)e.preventDefault();});
document.addEventListener('beforeinput',e=>{
  if(ALLOW_PASTE)return;
  if(e.inputType==='insertFromPaste'||e.inputType==='insertFromDrop'||e.inputType==='insertFromPasteAsQuotation'){
    e.preventDefault();alert('⚠️ 붙여넣기 금지!');
  }
});
document.addEventListener('keydown',e=>{
  const isCtrl=e.ctrlKey||e.metaKey,key=e.key.toLowerCase(),isTyping=['INPUT','TEXTAREA'].includes(e.target.tagName);
  if(isCtrl&&key==='enter'){e.preventDefault();
    const btn=document.getElementById('send-btn');
    if(btn&&!btn.disabled)sendMessage();
    return;}
  if(isCtrl&&key==='a'&&isTyping)return;
  if(ALLOW_PASTE)return;
  if(isCtrl&&['c','v','x','a'].includes(key)){e.preventDefault();alert('⚠️ 단축키 금지!');}
});
const socket=io({reconnection:true,reconnectionAttempts:Infinity,
  reconnectionDelay:800,reconnectionDelayMax:4000,timeout:20000});
let currentRoom="",myTurn=false,totalStages=4,oppGone=false,judging=false;
// 입론은 두 사람이 동시에 작성한다
let opening=false,myOpeningDone=false,oppOpeningDone=false;
let wantQueue=false,kicked=false;
const TIMER_SECONDS={{ turn_seconds }};
let timerInterval=null,remaining=TIMER_SECONDS,deadlineAt=0,minLength={{ min_length }};
// 새로고침해도 이어지도록 접속 토큰과 작성 중인 초안을 브라우저에 보관한다
// 접속 토큰은 탭마다 따로(sessionStorage) — 같은 브라우저의 다른 탭이 서로의 아이디로 들어가지 않게.
// 초안은 브라우저에(localStorage) 방+진영별로 — 탭을 닫았다 다시 로그인해도 남아 있게.
function store(k,v){try{if(v===null)localStorage.removeItem(k);else localStorage.setItem(k,v);}catch(e){}}
function load(k){try{return localStorage.getItem(k);}catch(e){return null;}}
function tokSave(v){try{if(v===null)sessionStorage.removeItem('debate_token');else sessionStorage.setItem('debate_token',v);}catch(e){}}
function tokLoad(){try{return sessionStorage.getItem('debate_token');}catch(e){return null;}}
try{localStorage.removeItem('debate_token');}catch(e){}   // 예전 방식으로 저장된 토큰 정리
let resumeToken=tokLoad()||"";
function draftKey(){return 'debate_draft_'+currentRoom+'_'+mySide;}
function saveDraft(){if(currentRoom)store(draftKey(),document.getElementById('msg-input').value);}
function restoreDraft(){const el=document.getElementById('msg-input');const v=load(draftKey());
  if(v&&!el.value.trim())el.value=v;updateLenHint();}
function clearDrafts(){if(currentRoom)store(draftKey(),null);}

function canSend(){return !oppGone&&!judging&&(opening?!myOpeningDone:myTurn);}
// 타이머는 마감 시각 기준 — 휴대폰 화면이 꺼졌다 켜져도 정확하다
function startTimer(seconds){clearInterval(timerInterval);
  const s=(typeof seconds==='number'&&seconds>=0)?seconds:TIMER_SECONDS;
  deadlineAt=Date.now()+s*1000;tick();timerInterval=setInterval(tick,500);}
function tick(){remaining=Math.max(0,Math.ceil((deadlineAt-Date.now())/1000));updateTimerText();
  if(remaining<=0){clearInterval(timerInterval);if(canSend())autoSend();}}
function stopTimer(){clearInterval(timerInterval);}
function clockText(){const r=Math.max(0,remaining),m=Math.floor(r/60),s=String(r%60).padStart(2,'0');
  return m+":"+s;}
function updateTimerText(){const el=document.getElementById('turn-status');
  if(oppGone||judging){return;}
  const mine=canSend();
  el.classList.toggle('urgent', mine && remaining<=30);
  el.classList.toggle('waiting', !mine);
  if(opening){
    if(!myOpeningDone){
      el.innerText=(remaining<=30?"🚨":"✍️")+" 입론 작성 시간 — 남은 시간 "+clockText()+
        "\\n두 사람이 동시에 씁니다. 상대 입론은 둘 다 제출하면 함께 공개됩니다."+
        (oppOpeningDone?"\\n(상대는 제출을 마쳤습니다 — 서두를 필요 없이 남은 시간을 다 써도 됩니다)":"");
    }else{
      el.innerText="✅ 입론 제출 완료 — 상대가 입론을 마칠 때까지 기다리는 중 (남은 시간 "+clockText()+")\\n다음 단계 발언을 미리 적어둘 수 있습니다.";
    }
    return;}
  if(myTurn){el.innerText=(remaining<=30?"🚨":"⏰")+" 당신의 발언 차례입니다! (남은 시간 "+clockText()+")";}
  else{el.innerText="⏳ 상대방 발언 중 — 남은 시간 "+clockText()+" · 미리 작성해두세요";}}
// 시간이 끝나면 쓰던 내용을 길이와 상관없이 그대로 제출한다
function autoSend(){const el=document.getElementById('msg-input');
  stopTimer();
  socket.emit('send_message',{room_id:currentRoom,message:el.value.trim(),auto:true});
  el.value="";saveDraft();updateLenHint();}
// 작성 중인 글을 몇 초마다 서버에도 보관 — 화면이 꺼져도 서버가 대신 제출할 수 있게
let lastDraftSent=null;
setInterval(()=>{if(!currentRoom||!socket.connected)return;
  const v=document.getElementById('msg-input').value;
  if(v!==lastDraftSent){lastDraftSent=v;socket.emit('draft',{room_id:currentRoom,text:v});}},3000);
function renderCriteria(list){
  if(!list||!list.length)return;
  const box=document.getElementById('criteria-box');
  box.innerHTML='<b>📋 AI 심사 기준</b> — 단계당 25점, 총 100점<br>'+
    list.map(c=>"<span class='ct'>"+c.stage+"</span> "+c.desc).join('<br>')+
    '<br><span style="color:#9ca3af;">※ 주제와 무관한 내용·장난으로 일관하면 운영자 확인 후 몰수패가 될 수 있습니다.</span>';}
function updateLenHint(){const v=document.getElementById('msg-input').value.trim();
  const el=document.getElementById('len-hint');
  el.textContent=v.length+"자 / 최소 "+minLength+"자";
  el.className=(v.length<minLength)?'short':'';}
function surrender(){
  if(!currentRoom)return;
  if(!confirm('정말 항복하시겠습니까?\\n\\n패배로 기록되며 토론이 즉시 종료됩니다.'))return;
  socket.emit('surrender',{});}
function login(){const n=document.getElementById('username').value,p=document.getElementById('pin').value;
  if(!n.trim()||!p.trim())return alert('아이디와 PIN을 입력해주세요.');socket.emit('login',{username:n,pin:p});}
function logout(){tokSave(null);clearDrafts();location.reload();}
socket.on('login_ok',d=>{
  resumeToken=d.resume_token||"";tokSave(resumeToken);
  document.getElementById('login-area').classList.add('hidden');
  if(!currentRoom)document.getElementById('profile-area').classList.remove('hidden');
  document.getElementById('my-name').textContent=d.username;
  document.getElementById('my-stats').textContent="내 전적: "+d.wins+"승 "+d.losses+"패 | 점수: "+d.points+"점 | 현재 "+d.rank+"위";
  const lr=document.getElementById('last-result');
  if(d.last_result){const r=d.last_result;lr.classList.remove('hidden');
    lr.querySelector('summary').textContent="📋 최근 경기: "+r.outcome+" ("+r.my_side+") · "+r.played_at;
    lr.querySelector('.lr-body').textContent="📌 "+r.topic+"\\n\\n"+r.reason;}
  else{lr.classList.add('hidden');}
  // 대기실에 있다가 연결이 잠깐 끊겼으면 다시 줄을 선다
  if(wantQueue&&!currentRoom)socket.emit('join_queue',{});});

function setNetBanner(text,color){
  let el=document.getElementById('net-banner');
  if(!el){el=document.createElement('div');el.id='net-banner';
    el.style.cssText='position:fixed;top:0;left:0;right:0;padding:8px;text-align:center;'+
      'font-weight:bold;color:#fff;z-index:999;font-size:0.9em;';
    document.body.appendChild(el);}
  if(!text){el.remove();return;}
  el.textContent=text;el.style.background=color;}

socket.on('disconnect',()=>{if(kicked)return;stopTimer();
  setNetBanner('⚠️ 연결이 끊겼습니다. 자동으로 다시 연결하는 중... (작성 중인 내용은 그대로 있습니다)','#dc2626');});
socket.on('connect',()=>{
  if(kicked)return;
  if(resumeToken){socket.emit('resume',{token:resumeToken});}
  else{setNetBanner('','');}});
socket.on('kicked',()=>{kicked=true;stopTimer();socket.disconnect();
  setNetBanner('같은 아이디로 다른 곳에서 접속해 이 화면은 연결을 끊었습니다.','#6b7280');
  document.getElementById('send-btn').disabled=true;});
socket.on('resumed',d=>{
  setNetBanner('✅ 다시 연결되었습니다 — 이어서 진행합니다','#16a34a');
  setTimeout(()=>setNetBanner('',''),2500);
  currentRoom=d.room_id;totalStages=d.total_stages;myTurn=d.your_turn;
  opening=d.opening;myOpeningDone=d.my_opening_done;oppOpeningDone=d.opp_opening_done;judging=d.judging;
  document.getElementById('login-area').classList.add('hidden');
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  mySide=d.my_side;oppSide=d.opp_side;
  document.getElementById('role-area').innerHTML="나: "+whoTag(d.my_side)+"  vs  상대: "+whoTag(d.opp_side);
  renderSideBanner(d.topic);
  if(d.min_length)minLength=d.min_length;
  renderCriteria(d.criteria);
  const cb=document.getElementById('chat-box');cb.innerHTML='';
  d.logs.forEach(l=>{appendMessage(cb,l,false);
    if(l.summary){const sd=document.createElement('div');sd.className='summary-item '+sideClass(l.side);
      sd.textContent='🤖 AI 요약: '+l.summary;cb.appendChild(sd);}});
  cb.scrollTop=cb.scrollHeight;
  showStage(d.stage,d.stage_index,d.total_stages,false);
  restoreDraft();
  if(judging){showJudging('AI가 토론을 심사하고 있습니다...');return;}
  oppGone=!d.opp_connected;
  if(oppGone)showOppGone();
  updateTurnUI(d.seconds_left);});
socket.on('resume_failed',()=>{setNetBanner('','');
  tokSave(null);resumeToken="";
  if(currentRoom){alert('토론이 이미 종료되었습니다. 다시 로그인해주세요.');location.reload();}});
function showOppGone(){stopTimer();
  setNetBanner('⏸ 상대방 연결이 끊겼습니다. 돌아올 때까지 시계가 멈춥니다.','#d97706');
  const el=document.getElementById('turn-status');el.classList.add('waiting');
  el.innerText="⏸ 상대 재접속 대기 중 (시계 정지) — 계속 작성해도 됩니다";
  document.getElementById('send-btn').disabled=true;}
socket.on('opponent_disconnected',()=>{oppGone=true;showOppGone();});
// 결과는 브라우저 alert 대신 화면 안 팝업으로 보여준다.
// (alert는 탭이 뒤에 있거나 '대화상자 차단'이 켜지면 안 뜨고 바로 넘어갈 수 있다)
function showResult(title,body){
  stopTimer();clearDrafts();
  let ov=document.getElementById('result-overlay');
  if(!ov){ov=document.createElement('div');ov.id='result-overlay';document.body.appendChild(ov);}
  ov.innerHTML="<div class='result-card'><h3></h3><div class='result-body'></div>"+
    "<button id='result-ok'>확인하고 나가기</button></div>";
  ov.querySelector('h3').textContent=title;
  ov.querySelector('.result-body').textContent=body;
  document.getElementById('result-ok').onclick=()=>location.reload();
  document.title='🔔 '+title;}
socket.on('debate_aborted',d=>showResult("⏹️ 토론이 중단되었습니다",d.headline+"\\n승패는 기록되지 않았습니다.\\n\\n"+d.reveal));
socket.on('opponent_reconnected',d=>{oppGone=false;
  setNetBanner('✅ 상대방이 돌아왔습니다 — 시계가 다시 갑니다','#16a34a');
  setTimeout(()=>setNetBanner('',''),2500);
  updateTurnUI(d&&typeof d.seconds_left==='number'?d.seconds_left:remaining);});
socket.on('error_msg',d=>alert(d.msg));
function joinQueue(){wantQueue=true;socket.emit('join_queue',{});document.getElementById('match-btn').disabled=true;
  document.getElementById('cancel-btn').classList.remove('hidden');}
function cancelQueue(){wantQueue=false;socket.emit('leave_queue',{});
  document.getElementById('match-btn').disabled=false;
  document.getElementById('cancel-btn').classList.add('hidden');
  document.getElementById('queue-text').innerText='매칭을 취소했습니다.';}
socket.on('status',d=>{if(wantQueue)document.getElementById('queue-text').innerText=d.msg;});
// 매칭 버튼이 눌린 채 잠기지 않도록, 방이 안 생겼으면 다시 누를 수 있게 풀어준다
socket.on('error_msg',()=>{if(!currentRoom&&!wantQueue){
  const b=document.getElementById('match-btn');if(b)b.disabled=false;}});
function renderStageDots(idx,total){
  const wrap=document.getElementById('stage-dots');wrap.innerHTML='';
  for(let i=0;i<total;i++){const dot=document.createElement('div');
    dot.className='dot'+(i<idx?' done':i===idx?' current':'');wrap.appendChild(dot);}}
function showStage(stageName,idx,total,animate){
  const banner=document.getElementById('stage-banner');
  banner.textContent='📍 현재 단계: '+stageName+(idx===0?' (동시 작성)':'')+' ('+(idx+1)+'/'+total+')';
  renderStageDots(idx,total);
  if(animate){banner.classList.remove('stage-pulse');void banner.offsetWidth;banner.classList.add('stage-pulse');}}
let mySide="",oppSide="";
function sideTag(side){return "<span class='side-tag "+(side==='찬성'?'side-pro':'side-con')+"'>"+side+"</span>";}
// 채팅에서 [찬성(나)] / [반대(상대)] 로 표시
function whoTag(side){
  const mine=(side===mySide), label=side+(mine?'(나)':'(상대)');
  return "<span class='side-tag "+(side==='찬성'?'side-pro':'side-con')+"'>"+label+"</span>";}
function renderSideBanner(topic){
  const el=document.getElementById('side-banner');
  if(!mySide){el.className='';el.innerHTML='';return;}
  el.className=(mySide==='찬성')?'pro':'con';
  const verb=(mySide==='찬성')
    ? "이 주장이 <b>옳다</b>고 <b>옹호</b>하세요."
    : "이 주장이 <b>틀렸다</b>고 <b>반박</b>하세요.";
  el.innerHTML="<span class='big'>"+mySide+"</span>"+
    "<span class='desc'>「<span class='claim'>"+topic+"</span>」<br>"+verb+"</span>";
  document.title="["+mySide+"] 토론 중";}
socket.on('match_found',d=>{currentRoom=d.room_id;myTurn=d.your_turn;totalStages=d.total_stages;
  wantQueue=false;opening=!!d.opening;myOpeningDone=false;oppOpeningDone=false;judging=false;oppGone=false;
  if(d.min_length)minLength=d.min_length;
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  mySide=d.my_side;oppSide=d.opp_side;
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  document.getElementById('role-area').innerHTML="나: "+whoTag(d.my_side)+"  vs  상대: "+whoTag(d.opp_side);
  renderSideBanner(d.topic);
  renderCriteria(d.criteria);
  document.getElementById('chat-box').innerHTML='';
  restoreDraft();
  showStage(d.stage,d.stage_index,d.total_stages,true);
  updateTurnUI(d.seconds_left);});
socket.on('opening_submitted',()=>{myOpeningDone=true;updateTurnUI(remaining);});
socket.on('opp_opening_submitted',()=>{oppOpeningDone=true;updateTimerText();});
socket.on('turn_change',d=>{myTurn=d.your_turn;opening=false;
  showStage(d.stage,d.stage_index,totalStages,d.stage_changed);
  updateTurnUI(d.seconds_left);});
function updateTurnUI(seconds){const area=document.getElementById('debate-area'),
  input=document.getElementById('msg-input'),btn=document.getElementById('send-btn'),
  sur=document.getElementById('surrender-btn');
  // 상대 차례에도 입력창은 열어둔다 — 전송만 막는다
  input.disabled=false;
  btn.disabled=!canSend();
  if(sur)sur.disabled=oppGone;
  if(canSend()){area.classList.add('turn-active');}
  else{area.classList.remove('turn-active');}
  if(!oppGone)startTimer(typeof seconds==='number'?seconds:remaining);
  updateLenHint();}
function sendMessage(isAuto){const el=document.getElementById('msg-input'),msg=el.value.trim();
  if(!canSend()){
    if(opening&&myOpeningDone)return alert('입론은 이미 제출했습니다.\\n작성해둔 내용은 다음 단계에 쓸 수 있도록 그대로 유지됩니다.');
    return alert('지금은 당신의 발언 차례가 아닙니다.\\n작성해둔 내용은 그대로 유지됩니다.');}
  if(!msg)return alert('내용을 입력해주세요.');
  if(!isAuto&&msg.length<minLength){
    alert('⚠️ 발언이 너무 짧습니다.\\n\\n현재 '+msg.length+'자 / 최소 '+minLength+'자\\n\\n논거를 더 채워서 다시 시도해주세요.');
    el.focus();return;}
  if(!isAuto&&opening&&!confirm('입론을 제출할까요?\\n\\n제출하면 고칠 수 없습니다. 남은 시간이 있으면 더 다듬어도 됩니다.'))return;
  stopTimer();
  socket.emit('send_message',{room_id:currentRoom,message:msg});
  el.value="";saveDraft();updateLenHint();}
socket.on('message_rejected',d=>{
  alert('⚠️ '+d.msg+'\\n\\n작성하신 내용은 입력창에 그대로 복원했습니다.');
  const el=document.getElementById('msg-input');
  if(!el.value.trim())el.value=d.text||"";
  saveDraft();updateLenHint();el.focus();
  startTimer(remaining);});
document.addEventListener('input',e=>{if(e.target.id==='msg-input'){updateLenHint();saveDraft();}});
socket.on('surrendered',d=>showResult("🏳️ 항복했습니다",d.headline+"\\n\\n"+d.reveal));
function sideClass(side){return side==='찬성'?'pro':(side==='반대'?'con':'');}
function appendMessage(cb,d,withPending){
  const md=document.createElement('div');md.className='msg-item '+sideClass(d.side);
  const ne=document.createElement('strong');
  ne.innerHTML="<span class='stage-label'>["+d.stage+"]</span> "+whoTag(d.side)+" ";
  const te=document.createElement('span');te.textContent=d.message;
  md.appendChild(ne);md.appendChild(te);cb.appendChild(md);
  if(d.auto){const tag=document.createElement('div');tag.className='summary-item';
    tag.textContent='⏰ 시간 종료로 자동 제출됨';md.appendChild(tag);}
  if(withPending&&d.message!=='(작성 없음)'){const sd=document.createElement('div');
    sd.className='summary-item pending '+sideClass(d.side);
    sd.id='sum-'+d.msg_id;sd.textContent='🤖 AI 요약 생성 중...';cb.appendChild(sd);}}
socket.on('receive_message',d=>{const cb=document.getElementById('chat-box');
  appendMessage(cb,d,true);cb.scrollTop=cb.scrollHeight;});
socket.on('receive_summary',d=>{const sd=document.getElementById('sum-'+d.msg_id);
  if(!sd)return;sd.classList.remove('pending');
  if(d.summary){sd.textContent='🤖 AI 요약: '+d.summary;}
  else{sd.remove();}   // 요약 실패 시 '생성 중'을 남겨두지 않는다
  const cb=document.getElementById('chat-box');cb.scrollTop=cb.scrollHeight;});
function showJudging(msg){stopTimer();judging=true;
  document.getElementById('turn-status').innerText="⚖️ "+msg;
  document.getElementById('msg-input').disabled=true;
  document.getElementById('send-btn').disabled=true;}
socket.on('judging',d=>{showJudging(d.msg);
  const cb=document.getElementById('chat-box');
  const el=document.createElement('div');el.className='summary-item pending';
  el.textContent='⚖️ '+d.msg;cb.appendChild(el);cb.scrollTop=cb.scrollHeight;});
socket.on('opponent_left',d=>showResult("🚪 몰수승!",(d.headline||"")+"\\n\\n"+d.reveal));
socket.on('debate_end',d=>showResult("🔔 토론 완료 — AI 판정",d.result));
</script>
</body></html>"""

ADMIN_TEMPLATE = r"""<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>운영자 설정</title>
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<style>
body{font-family:'Malgun Gothic',sans-serif;max-width:760px;margin:20px auto;padding:0 14px 40px;background:#f3f4f6;color:#111827;}
h2{margin:10px 0 12px;}
.box{border:1px solid #e5e7eb;padding:16px;background:#fff;margin-bottom:14px;border-radius:12px;}
.box h3{margin:0 0 10px;font-size:1.05em;}
.tabs{display:flex;gap:6px;flex-wrap:wrap;margin-bottom:14px;position:sticky;top:0;background:#f3f4f6;padding:8px 0;z-index:5;}
.tabs button{background:#fff !important;color:#374151 !important;-webkit-text-fill-color:#374151;border:1px solid #d1d5db;}
.tabs button.on{background:#2563eb !important;color:#fff !important;-webkit-text-fill-color:#fff;border-color:#2563eb;}
.tabs button.danger.on{background:#dc2626 !important;border-color:#dc2626;}
.lockbox{text-align:center;padding:30px 16px;}
.lockbox .lk{font-size:2em;}
.hint{font-size:0.82em;color:#6b7280;}
.ok{color:#16a34a}.bad{color:#dc2626}
table{width:100%;border-collapse:collapse;font-size:0.9em;margin:6px 0;}
th{text-align:left;border-bottom:1px solid #e5e7eb;padding:5px 4px;color:#6b7280;font-weight:normal;}
td{border-bottom:1px solid #f3f4f6;padding:6px 4px;}
input[type=text],input[type=password],select{padding:8px;border:1px solid #d1d5db;border-radius:6px;
  font-size:16px;background:#fff;color:#111827;max-width:100%;box-sizing:border-box;font-family:inherit;}
button{-webkit-appearance:none;appearance:none;border:1px solid #93c5fd;border-radius:6px;background:#2563eb;
  color:#fff !important;-webkit-text-fill-color:#fff;cursor:pointer;font-size:14px;font-weight:bold;
  padding:8px 13px;margin:3px 4px 3px 0;font-family:inherit;}
button.sub{background:#fff !important;color:#2563eb !important;-webkit-text-fill-color:#2563eb;}
button.red{background:#dc2626 !important;border-color:#dc2626;}
button.redline{background:#fff !important;color:#dc2626 !important;-webkit-text-fill-color:#dc2626;border-color:#fca5a5;}
button:disabled{opacity:.5;cursor:not-allowed;}
.row{display:flex;gap:8px;flex-wrap:wrap;align-items:center;margin:6px 0;}
.chip{display:inline-block;padding:6px 10px;border:1px solid #d1d5db;border-radius:16px;margin:3px;cursor:pointer;background:#fff;font-size:0.92em;}
.chip.sel{background:#dbeafe;border-color:#2563eb;font-weight:bold;}
.chip .w{color:#9ca3af;font-size:0.85em;}
.chip.res{border-style:dashed;}
.switch{display:inline-flex;align-items:center;gap:8px;font-weight:bold;cursor:pointer;}
.switch input{width:20px;height:20px;}
.side-tag{display:inline-block;padding:1px 7px;border-radius:6px;font-size:0.8em;font-weight:bold;}
.side-pro{background:#dcfce7;color:#166534;} .side-con{background:#fee2e2;color:#991b1b;}
.room{border:1px solid #e5e7eb;border-radius:8px;margin-bottom:8px;background:#f9fafb;overflow:hidden;}
.room[open]{background:#fff;border-color:#c7d2fe;}
.room.alert{border-color:#f59e0b;background:#fffbeb;}
.room > summary{padding:10px 12px;cursor:pointer;line-height:1.5;list-style:none;}
.room > summary::-webkit-details-marker{display:none;}
.room .body{padding:0 12px 12px;}
.room .chat{height:240px;overflow-y:auto;border:1px solid #e5e7eb;padding:8px;background:#f9fafb;border-radius:8px;font-size:0.9em;}
.chat p{margin:5px 0;padding:6px 8px;border-radius:6px;border-left:3px solid #d1d5db;white-space:pre-line;}
.chat p.pro{border-left-color:#16a34a;background:#f0fdf4;}
.chat p.con{border-left-color:#dc2626;background:#fef2f2;}
.chat p.sys{border-left-color:#7c3aed;background:#faf5ff;color:#6b21a8;font-weight:bold;}
.chat p.sum{border-left-color:#e5e7eb;background:transparent;color:#6b7280;font-size:0.92em;}
.chat p.endmsg{border-left-color:#dc2626;background:#fef2f2;color:#991b1b;font-weight:bold;}
.pend{border:1px solid #fcd34d;background:#fffbeb;border-radius:8px;padding:10px;margin-bottom:8px;font-size:0.92em;}
.pend.fail{border-color:#fca5a5;background:#fef2f2;}
.pend .why{white-space:pre-line;color:#374151;margin:6px 0;}
.warn{background:#fef2f2;border:1px solid #fca5a5;color:#991b1b;padding:10px;border-radius:8px;font-size:0.9em;}
#toast{position:fixed;left:50%;transform:translateX(-50%);bottom:18px;max-width:92%;background:#111827;color:#fff;
  padding:10px 16px;border-radius:10px;font-size:0.92em;white-space:pre-line;display:none;z-index:50;box-shadow:0 4px 12px rgba(0,0,0,.25);}
#toast.err{background:#b91c1c;}
.nav{font-size:0.88em;} .nav a{color:#2563eb;text-decoration:none;}
</style></head><body>
<h2>🛠️ 운영자 설정</h2>
<div class="tabs" id="tabs">
{% for key, label in tabs.items() %}<button data-tab="{{ key }}" class="{{ 'danger' if key == 'danger' else '' }}" onclick="openTab('{{ key }}')">{{ {'ops':'🎮','ai':'🤖','users':'👤','danger':'🗑️'}[key] }} {{ label }}</button>{% endfor %}
</div>

<div id="lock" class="box lockbox">
  <div class="lk">🔒</div>
  <p id="lock-title" style="font-weight:bold;">탭을 선택하세요</p>
  <div id="lock-form" style="display:none;">
    <input type="password" id="lock-code" placeholder="운영자 코드" autocomplete="off" style="width:200px;">
    <button onclick="unlock()">열기</button>
    <p class="hint">한 번 열면 이 탭 안에서는 코드 없이 작업할 수 있습니다.<br>다른 탭으로 가거나 새로고침하면 다시 잠깁니다.</p>
  </div>
</div>

<!-- ───────── 대회 운영 ───────── -->
<div id="panel-ops" class="panel" style="display:none;">
  <div class="box">
    <h3>🎯 매칭 방식</h3>
    <p id="mode-line" style="font-weight:bold;margin:4px 0 8px;"></p>
    <button id="mode-btn" onclick="toggleMode()"></button>
    <p class="hint">기본은 <b>랜덤 매칭</b> — 대기실에 들어온 사람끼리 자동으로 붙습니다.<br>
    <b>주작 매칭 모드</b>를 켜면 랜덤 매칭이 멈추고, 아래 대기실에서 운영자가 직접 붙이거나 예약할 수 있습니다.</p>
    <label class="switch" style="margin-top:8px;"><input type="checkbox" id="allow-paste" onchange="setPaste(this.checked)">
      <span id="paste-label">복사·붙여넣기 허용 (테스트용)</span></label>
    <p class="hint">테스트할 때만 켜세요. <b>대회 전에 반드시 끄기.</b> 바꾼 뒤 참가자가 새로고침해야 적용됩니다.</p>
  </div>

  <div class="box">
    <h3>🧑‍🤝‍🧑 대기실 <span id="wait-count" class="hint"></span></h3>
    <div id="waiting"></div>
    <p class="hint" id="manual-off-note" style="color:#b45309;font-weight:bold;">🔒 지정 매칭·예약은 위에서 '주작 매칭 모드'를 켜야 쓸 수 있습니다.</p>
    <fieldset id="manual-ctrl" style="border:none;padding:0;margin:0;">
    <p class="hint">이름을 누르면 아래 A → B 칸에 차례로 들어갑니다. 점선 = 예약된 사람</p>
    <div class="row">
      <input type="text" id="pa" placeholder="A 아이디" style="width:130px;">
      <span>vs</span>
      <input type="text" id="pb" placeholder="B 아이디" style="width:130px;">
      <select id="pside">
        <option value="random">진영 랜덤</option>
        <option value="a_pro">A 찬성 / B 반대</option>
        <option value="a_con">A 반대 / B 찬성</option>
      </select>
    </div>
    <div class="row">
      <input type="text" id="ptopic" placeholder="주제 직접 지정 (비우면 AI 주제)" style="flex:1;min-width:220px;">
    </div>
    <div class="row">
      <button onclick="pairNow()">⚡ 지금 매칭</button>
      <button class="sub" onclick="reserve()">📌 예약 등록</button>
      <button class="sub" onclick="clearPair()">비우기</button>
    </div>
    <p class="hint"><b>지금 매칭</b>: 두 사람이 대기실에 있어야 합니다. 바로 시작.<br>
    <b>예약 등록</b>: 아직 안 들어왔어도 됩니다. 둘 다 대기실에 들어오면 자동으로 시작.</p>
    </fieldset>
    <div id="reservations"></div>
  </div>

  <div class="box" id="pending-box" style="display:none;">
    <h3>⚖️ 운영자 확인 필요</h3>
    <div id="pending"></div>
  </div>

  <div class="box">
    <h3>🔴 진행 중인 토론 <span id="room-count" class="hint"></span></h3>
    <p class="hint">눌러서 펼치면 실시간으로 볼 수 있습니다. 목록은 4초마다 갱신됩니다.</p>
    <div id="rooms"></div>
  </div>
</div>

<!-- ───────── AI 설정 ───────── -->
<div id="panel-ai" class="panel" style="display:none;">
  <div class="box">
    <h3>🤖 사용할 AI (ChatGPT)</h3>
    <div id="ai-warn"></div>
    <div id="profiles"></div>
    <div class="row"><button onclick="switchAI()">선택한 AI로 변경</button>
      <button class="sub" onclick="testAI()">🔌 연결 테스트</button></div>
    <pre id="test-out" class="hint" style="white-space:pre-wrap;"></pre>
  </div>
  <div class="box">
    <h3>🔑 키 등록 / 변경</h3>
    <p class="hint">같은 이름으로 다시 저장하면 덮어씁니다. 키는 DB에 저장돼 재배포해도 유지됩니다.</p>
    <div class="row"><input type="text" id="ai-name" placeholder="이름 (예: ChatGPT)" style="width:150px;">
      <span class="hint" id="ai-model-note"></span></div>
    <div class="row"><input type="password" id="ai-key" placeholder="API 키 (비우면 기존 키 유지)" style="flex:1;min-width:240px;" autocomplete="off"></div>
    <label class="hint"><input type="checkbox" id="ai-activate" checked> 저장 후 바로 이 AI 사용</label>
    <div class="row"><button onclick="saveAI()">저장</button></div>
  </div>
  <div class="box">
    <h3>⚙️ 심사·호출 설정</h3>
    <p><label><input type="checkbox" id="set-draw"> 무승부 허용 <span class="hint">(끄면 AI가 반드시 승자를 정함 — 토너먼트용)</span></label></p>
    <p><label><input type="checkbox" id="set-summary"> 발언 요약 사용 <span class="hint">(끄면 토론당 호출 약 10회 → 2회)</span></label></p>
    <p>분당 호출 상한 <input type="text" id="set-rpm" style="width:80px;"> <span class="hint" id="rpm-now"></span></p>
    <button onclick="saveSettings()">설정 저장</button>
  </div>
  <div class="box">
    <h3>🩺 오늘 AI 호출</h3>
    <div id="ai-stats"></div>
    <details><summary class="hint">최근 15건</summary><div id="ai-recent"></div></details>
  </div>
</div>

<!-- ───────── 아이디 관리 ───────── -->
<div id="panel-users" class="panel" style="display:none;">
  <div class="box">
    <h3>➕ 아이디 생성</h3>
    <div class="row"><input type="text" id="nu" placeholder="새 아이디" style="width:140px;">
      <input type="text" id="np" placeholder="PIN (4자리 이상)" style="width:140px;">
      <button onclick="createUser()">생성</button></div>
    <h3 style="margin-top:16px;">🔁 PIN 변경</h3>
    <div class="row"><input type="text" id="eu" placeholder="아이디" style="width:140px;">
      <input type="text" id="ep" placeholder="새 PIN" style="width:140px;">
      <button onclick="changePin()">변경</button></div>
  </div>
  <div class="box"><h3>📋 아이디 목록 <span id="user-count" class="hint"></span></h3><div id="user-table"></div></div>
</div>

<!-- ───────── 삭제 ───────── -->
<div id="panel-danger" class="panel" style="display:none;">
  <div class="box">
    <h3>🗑️ 아이디 삭제</h3>
    <p class="hint">삭제하면 전적도 함께 사라지고 되돌릴 수 없습니다.</p>
    <div id="del-users"></div>
  </div>
  <div class="box">
    <h3>📚 토론 기록 삭제 <span id="debate-count" class="hint"></span></h3>
    <p class="hint">기록만 지워지고 승패·점수는 바뀌지 않습니다. 점수까지 되돌리려면 아래 초기화를 쓰세요.</p>
    <div id="del-debates" style="max-height:360px;overflow-y:auto;"></div>
    <button class="redline" onclick="delDebate(null,true)">기록 전체 삭제</button>
  </div>
  <div class="box">
    <h3>🔄 점수·전적 초기화</h3>
    <p class="hint">모든 아이디의 점수·승·패를 0으로 되돌립니다. 아이디와 토론 기록은 그대로 남습니다. 되돌릴 수 없습니다.</p>
    <button class="red" onclick="resetScores()">전체 점수·전적 0으로 초기화</button>
  </div>
  <div class="box">
    <h3>🗑️ AI 키 삭제</h3>
    <div id="del-profiles"></div>
  </div>
</div>

<p class="nav"><a href="/">← 토론 화면</a> · <a href="/ranking">🏆 랭킹</a> · <a href="/history">📚 토론 기록</a></p>
<div id="toast"></div>

<script>
const TAB_NAMES={{ tabs|tojson }};
let token=null, tab=null, poll=null;
const wsocket=io();
const openRooms=new Set();
const roomEls={};

function esc(s){return String(s==null?'':s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function toast(msg,isErr){const t=document.getElementById('toast');t.textContent=msg;
  t.className=isErr?'err':'';t.style.display='block';clearTimeout(toast._t);
  toast._t=setTimeout(()=>t.style.display='none',isErr?6000:3500);}
function sideTag(s){return "<span class='side-tag "+(s==='찬성'?'side-pro':'side-con')+"'>"+esc(s)+"</span>";}
function fmtSec(s){s=Math.max(0,s|0);return Math.floor(s/60)+":"+String(s%60).padStart(2,'0');}

// ── 탭 잠금 ──
function lockCurrent(){
  if(token){const t=token;token=null;
    fetch('/admin/api/lock',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({token:t}),keepalive:true});}
  clearInterval(poll);poll=null;
}
function showLock(t){
  document.querySelectorAll('.panel').forEach(p=>p.style.display='none');
  document.getElementById('lock').style.display='';
  document.getElementById('lock-title').textContent='🔐 '+TAB_NAMES[t]+' 탭 — 운영자 코드를 입력하세요';
  document.getElementById('lock-form').style.display='';
  const c=document.getElementById('lock-code');c.value='';setTimeout(()=>c.focus(),50);
}
function openTab(t){
  if(t===tab&&token)return;
  lockCurrent();tab=t;
  document.querySelectorAll('#tabs button').forEach(b=>b.classList.toggle('on',b.dataset.tab===t));
  showLock(t);
}
async function unlock(){
  const code=document.getElementById('lock-code').value;
  if(!code)return;
  const r=await fetch('/admin/api/unlock',{method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({tab,code})});
  const d=await r.json().catch(()=>({error:'서버 응답 오류'}));
  if(!r.ok){alert(r.status===403?'❌ 운영자 코드가 틀렸습니다.':(d.error||'실패'));
    const c=document.getElementById('lock-code');c.value='';c.focus();return;}
  token=d.token;
  document.getElementById('lock').style.display='none';
  document.getElementById('panel-'+tab).style.display='';
  loadTab();
}
document.getElementById('lock-code').addEventListener('keydown',e=>{if(e.key==='Enter')unlock();});
window.addEventListener('pagehide',lockCurrent);

async function api(path,body){
  const r=await fetch('/admin/api/'+path,{method:'POST',
    headers:{'Content-Type':'application/json','X-Admin-Token':token||''},body:JSON.stringify(body||{})});
  const d=await r.json().catch(()=>({error:'서버 응답 오류 ('+r.status+')'}));
  if(r.status===401&&d.locked){token=null;clearInterval(poll);toast('탭이 잠겼습니다. 코드를 다시 입력해주세요.',true);showLock(tab);throw new Error('locked');}
  if(!r.ok||d.error){toast(d.error||'실패',true);throw new Error(d.error);}
  if(d.msg)toast(d.msg,d.ok===false);
  return d;
}
function loadTab(){
  if(tab==='ops'){refreshOps();wsocket.emit('admin_watch',{token});poll=setInterval(refreshOps,4000);
    openRooms.forEach(id=>wsocket.emit('admin_watch',{token,room_id:id}));}
  if(tab==='ai')refreshAI();
  if(tab==='users')refreshUsers();
  if(tab==='danger')refreshDanger();
}

// ── 대회 운영 ──
let opsState=null;
async function refreshOps(){
  if(tab!=='ops'||!token)return;
  let d;try{d=await api('ops/state');}catch(e){return;}
  opsState=d;
  const manual=!d.auto_match;
  document.getElementById('mode-line').innerHTML=manual?
    "<span style='color:#b45309;'>🎯 주작 매칭 모드 — 랜덤 매칭 멈춤, 운영자가 직접 배정</span>":
    "<span style='color:#16a34a;'>🎲 랜덤 매칭 중</span>";
  const mb=document.getElementById('mode-btn');
  mb.textContent=manual?'🎲 랜덤 매칭으로 돌아가기':'🎯 주작 매칭 모드 켜기';
  mb.className=manual?'sub':'';
  document.getElementById('manual-ctrl').disabled=!manual;
  document.getElementById('manual-ctrl').style.opacity=manual?'1':'.45';
  document.getElementById('manual-off-note').style.display=manual?'none':'';
  const ap=document.getElementById('allow-paste');ap.checked=d.allow_paste;
  document.getElementById('paste-label').innerHTML=d.allow_paste?
    "복사·붙여넣기 <b style='color:#dc2626;'>허용 중 ⚠️ 대회 전에 끄세요</b>":"복사·붙여넣기 허용 (테스트용) — 지금은 금지";
  document.getElementById('wait-count').textContent='('+d.waiting.length+'명)';
  const pa=document.getElementById('pa').value,pb=document.getElementById('pb').value;
  document.getElementById('waiting').innerHTML=d.waiting.length?d.waiting.map(w=>
    "<span class='chip"+(w.reserved?' res':'')+(w.username===pa||w.username===pb?' sel':'')+"' onclick=\"pick('"+esc(w.username)+"')\">"+
    esc(w.username)+" <span class='w'>"+fmtSec(w.wait_sec)+"</span></span>").join(''):
    "<p class='hint'>대기 중인 사람이 없습니다.</p>";
  const sideTxt={random:'진영 랜덤',a_pro:'A 찬성',a_con:'A 반대'};
  document.getElementById('reservations').innerHTML=d.reservations.length?
    "<p style='margin:10px 0 4px;font-weight:bold;'>📌 예약 ("+d.reservations.length+")</p>"+
    d.reservations.map(r=>"<div class='row'><span><b>"+esc(r.a)+"</b> vs <b>"+esc(r.b)+"</b> · "+sideTxt[r.side]+
      (r.topic?" · 주제: "+esc(r.topic):"")+"</span><button class='redline' onclick=\"unreserve('"+r.id+"')\">삭제</button></div>").join(''):'';
  renderPending(d.pending);
  renderRooms(d.rooms,d.absent_alert);
}
function pick(name){
  if(document.getElementById('manual-ctrl').disabled)return;
  const a=document.getElementById('pa'),b=document.getElementById('pb');
  if(a.value===name){a.value='';}else if(b.value===name){b.value='';}
  else if(!a.value){a.value=name;}else{b.value=name;}
  if(opsState)refreshOpsChips();
}
function refreshOpsChips(){const pa=document.getElementById('pa').value,pb=document.getElementById('pb').value;
  document.querySelectorAll('#waiting .chip').forEach(c=>{const n=c.firstChild.textContent.trim();
    c.classList.toggle('sel',n===pa||n===pb);});}
function clearPair(){['pa','pb','ptopic'].forEach(i=>document.getElementById(i).value='');refreshOpsChips();}
function pairBody(){return {a:document.getElementById('pa').value.trim(),b:document.getElementById('pb').value.trim(),
  side:document.getElementById('pside').value,topic:document.getElementById('ptopic').value.trim()};}
async function pairNow(){try{await api('ops/pair',pairBody());clearPair();}catch(e){}refreshOps();}
async function reserve(){try{await api('ops/reserve',pairBody());clearPair();}catch(e){}refreshOps();}
async function unreserve(id){try{await api('ops/unreserve',{id});}catch(e){}refreshOps();}
async function setPaste(on){try{await api('ops/paste',{on});}catch(e){}refreshOps();}
async function toggleMode(){
  const toManual=!!(opsState&&opsState.auto_match);
  if(!toManual&&opsState&&opsState.reservations.length&&
     !confirm('남아 있는 예약 '+opsState.reservations.length+'건은 랜덤 매칭 중에도 그대로 유지됩니다.\n(예약된 사람은 랜덤으로 붙지 않고 서로를 기다립니다)\n\n랜덤 매칭으로 돌아갈까요?'))return;
  try{await api('ops/auto',{on:!toManual});}catch(e){}refreshOps();}

function renderPending(list){
  const box=document.getElementById('pending-box');
  box.style.display=list.length?'':'none';
  document.getElementById('pending').innerHTML=list.map(p=>{
    const head="<b>📌 "+esc(p.topic)+"</b><br><span class='hint'>"+esc(p.played_at)+" · "+
      esc(p.player_a)+" "+sideTag(p.side_a)+" vs "+esc(p.player_b)+" "+sideTag(p.side_b)+"</span>";
    if(p.winner==='판정대기'){const pd=p.pending||{};
      return "<div class='pend'>"+head+"<div class='why'>⚠️ AI 지적: <b>"+esc(pd.no_will_side)+"</b>측 토론 의지 없음\n"+
        esc(pd.no_will_reason||'')+"\n\n[AI 원래 판정]\n"+esc(pd.text||'')+"</div>"+
        "<button class='red' onclick=\"resolve("+p.id+",'forfeit_pro')\">찬성 몰수패</button>"+
        "<button class='red' onclick=\"resolve("+p.id+",'forfeit_con')\">반대 몰수패</button>"+
        "<button class='sub' onclick=\"resolve("+p.id+",'dismiss')\">기각 → AI 판정대로</button>"+
        "<button class='sub' onclick='hidePending("+p.id+")'>목록에서 숨기기</button></div>";}
    return "<div class='pend fail'>"+head+"<div class='why'>"+esc((p.reason||'').split('\n\n')[0])+"</div>"+
      (p.rejudging?"<span class='hint'>⏳ 다시 심사하는 중...</span>":
      "<button onclick='rejudge("+p.id+")'>🔁 다시 심사</button>")+
      " <button class='sub' onclick='hidePending("+p.id+")'>목록에서 숨기기</button></div>";
  }).join('');
}
async function resolve(id,action){
  const label={forfeit_pro:'찬성측 몰수패',forfeit_con:'반대측 몰수패',dismiss:'기각(AI 판정 적용)'}[action];
  if(!confirm(label+'(으)로 처리할까요?'))return;
  try{await api('ops/resolve',{id,action});}catch(e){}refreshOps();}
async function hidePending(id){if(!confirm('이 건을 확인 목록에서 숨길까요?\n승패·점수는 바뀌지 않고, 토론 기록에는 남습니다.'))return;
  try{await api('ops/hide',{id});}catch(e){}refreshOps();}
async function rejudge(id){try{await api('ops/rejudge',{id});}catch(e){}refreshOps();}

function renderRooms(list,alertSec){
  document.getElementById('room-count').textContent='('+list.length+'개)';
  const wrap=document.getElementById('rooms');
  const live=new Set(list.map(r=>r.room_id));
  list.forEach(rm=>{
    let el=roomEls[rm.room_id];
    if(!el){
      el=document.createElement('details');el.className='room';
      el.innerHTML="<summary></summary><div class='body'><div class='chat' id='chat-"+rm.room_id+"'><p class='sum'>불러오는 중...</p></div>"+
        "<div class='row' style='margin-top:8px;'><button class='redline' onclick=\"stopRoom('"+rm.room_id+"')\">⏹️ 토론 중단 (승패 없음·재경기)</button></div></div>";
      el.addEventListener('toggle',()=>{if(el.open){openRooms.add(rm.room_id);wsocket.emit('admin_watch',{token,room_id:rm.room_id});}
        else openRooms.delete(rm.room_id);});
      roomEls[rm.room_id]=el;wrap.appendChild(el);
    }
    const absent=rm.players.some(p=>!p.connected&&p.absent_sec>=alertSec);
    el.classList.toggle('alert',absent);
    const who=rm.players.map(p=>{
      let st='';
      if(!p.connected)st=" <span class='bad'>⚠️끊김 "+fmtSec(p.absent_sec)+"</span>";
      else if(p.opening_done)st=" <span class='ok'>✓제출</span>";
      else if(p.speaking)st=" <span style='color:#2563eb;'>◀ 작성 중</span>";
      return "<b>"+esc(p.username)+"</b> "+sideTag(p.side)+st;}).join(" &nbsp;vs&nbsp; ");
    el.querySelector('summary').innerHTML="▸ <b>"+esc(rm.topic)+"</b><div class='hint'>"+esc(rm.stage)+
      " · 발언 "+rm.turns+"회 · "+rm.elapsed_min+"분 경과 · 남은 "+fmtSec(rm.seconds_left)+(rm.paused?" (⏸ 정지)":"")+"</div>"+who+
      (absent?"<div class='bad' style='font-size:0.85em;margin-top:4px;'>오래 끊겨 있습니다. 계속 기다리거나, 중단 후 재경기를 배정하세요.</div>":"");
  });
  Object.keys(roomEls).forEach(id=>{if(!live.has(id)&&!roomEls[id].open){roomEls[id].remove();delete roomEls[id];openRooms.delete(id);}});
}
async function stopRoom(id){
  if(!confirm('이 토론을 중단할까요?\n승패는 기록되지 않고 두 사람은 대기 화면으로 돌아갑니다.'))return;
  try{await api('ops/stop',{room_id:id});}catch(e){}refreshOps();}

function addLine(roomId,text,cls){const box=document.getElementById('chat-'+roomId);if(!box)return null;
  const el=document.createElement('p');if(cls)el.className=cls;el.textContent=text;box.appendChild(el);
  box.scrollTop=box.scrollHeight;return el;}
function sideCls(s){return s==='찬성'?'pro':'con';}
wsocket.on('admin_history',d=>{const box=document.getElementById('chat-'+d.room_id);if(!box)return;box.innerHTML='';
  if(!d.logs.length)addLine(d.room_id,'아직 공개된 발언이 없습니다. (입론은 둘 다 내면 함께 공개)','sum');
  d.logs.forEach(l=>addLine(d.room_id,'['+l.stage+'] '+l.side+' '+(l.username||'')+': '+l.message,sideCls(l.side)));});
wsocket.on('admin_message',d=>addLine(d.room_id,'['+d.stage+'] '+d.side+' '+(d.username||'')+': '+d.message,sideCls(d.side)));
wsocket.on('admin_summary',d=>addLine(d.room_id,'🤖 요약: '+d.summary,'sum'));
wsocket.on('admin_stage',d=>addLine(d.room_id,'▶ '+d.stage,'sys'));
wsocket.on('admin_end',d=>{addLine(d.room_id,'🔔 토론 종료\n'+d.result,'endmsg');refreshOps();});
wsocket.on('admin_alert',d=>{toast(d.msg);refreshOps();});
wsocket.on('admin_locked',()=>{});
wsocket.on('error_msg',d=>toast(d.msg,true));
wsocket.on('connect',()=>{if(tab==='ops'&&token){wsocket.emit('admin_watch',{token});
  openRooms.forEach(id=>wsocket.emit('admin_watch',{token,room_id:id}));}});

// ── AI 설정 ──
async function refreshAI(){
  let d;try{d=await api('ai/state');}catch(e){return;}
  let warn='';
  if(d.billing_hit)warn+="<p class='warn'>⚠️ 최근 <b>크레딧/결제 한도 소진(insufficient_quota)</b>이 있었습니다. platform.openai.com → Billing에서 크레딧을 확인하세요. 이 상태면 심사가 전부 실패합니다.</p>";
  if(d.rate_hit)warn+="<p class='warn' style='background:#fffbeb;border-color:#fcd34d;color:#92400e;'>최근 분당 한도(429)에 걸린 적이 있습니다. 보통 몇 초 안에 풀립니다. 자주 뜨면 발언 요약을 끄세요.</p>";
  document.getElementById('ai-warn').innerHTML=warn;
  const all=d.profiles.concat([{name:'테스트 모드',model:'AI 없이 동작',key_ok:true,masked:''}]);
  document.getElementById('profiles').innerHTML=all.map(p=>
    "<label style='display:block;padding:8px;border:"+(p.name===d.active?'2px solid #2563eb':'1px solid #e5e7eb')+
    ";border-radius:8px;margin-bottom:6px;'><input type='radio' name='prof' value='"+esc(p.name)+"' "+(p.name===d.active?'checked':'')+
    "> <b>"+esc(p.name)+"</b> — "+esc(p.model)+" "+(p.masked?"<span class='hint'>"+esc(p.masked)+"</span>":"")+
    (p.redirect?" <span class='bad'>(모델 없음 → "+esc(p.redirect)+" 사용 중)</span>":"")+"</label>").join('')+
    (d.profiles.length?'':"<p class='warn'>등록된 ChatGPT 키가 없습니다. 아래에서 등록하세요.</p>");
  document.getElementById('ai-model-note').textContent='모델: '+d.default_model+' (고정)';
  document.getElementById('set-draw').checked=d.allow_draw;
  document.getElementById('set-summary').checked=d.summary_on;
  document.getElementById('set-rpm').value=d.rpm_limit;
  document.getElementById('rpm-now').textContent='최근 1분 '+d.rpm_used+'회 사용';
  const st=Object.entries(d.stats);
  document.getElementById('ai-stats').innerHTML=st.length?"<table><tr><th>종류</th><th>성공</th><th>실패</th></tr>"+
    st.map(([k,s])=>"<tr><td>"+esc(k)+"</td><td class='ok'>"+s.ok+"</td><td class='"+(s.fail?'bad':'')+"'>"+s.fail+"</td></tr>").join('')+"</table>":
    "<p class='hint'>오늘 호출 기록이 없습니다.</p>";
  document.getElementById('ai-recent').innerHTML="<table>"+d.recent.map(c=>"<tr><td>"+esc(c.at.slice(11))+"</td><td>"+esc(c.kind)+
    "</td><td class='"+(c.ok?'ok':'bad')+"'>"+(c.ok?'성공':'실패')+"</td><td style='font-size:0.85em;word-break:break-all;'>"+
    esc(c.error?c.error.slice(0,120):c.ms+'ms · '+c.model)+"</td></tr>").join('')+"</table>";
}
async function switchAI(){const v=document.querySelector('input[name=prof]:checked');if(!v)return;
  try{await api('ai/switch',{profile:v.value});}catch(e){}refreshAI();}
async function testAI(){const out=document.getElementById('test-out');out.textContent='테스트 중...';
  try{const d=await api('ai/test');out.textContent=d.msg;}catch(e){out.textContent='';}refreshAI();}
async function saveAI(){try{await api('ai/save',{name:document.getElementById('ai-name').value,
  key:document.getElementById('ai-key').value,
  activate:document.getElementById('ai-activate').checked});document.getElementById('ai-key').value='';}catch(e){}refreshAI();}
async function saveSettings(){try{await api('ai/settings',{allow_draw:document.getElementById('set-draw').checked,
  summary_on:document.getElementById('set-summary').checked,max_rpm:document.getElementById('set-rpm').value});}catch(e){}refreshAI();}

// ── 아이디 관리 ──
function userTable(users){return "<table><tr><th>아이디</th><th>전적</th><th>점수</th></tr>"+
  users.map(u=>"<tr><td>"+esc(u.username)+"</td><td>"+u.wins+"승 "+u.losses+"패</td><td>"+u.points+"</td></tr>").join('')+"</table>";}
async function refreshUsers(){let d;try{d=await api('users/state');}catch(e){return;}
  document.getElementById('user-count').textContent='('+d.users.length+'명)';
  document.getElementById('user-table').innerHTML=userTable(d.users);}
async function createUser(){try{await api('users/create',{username:document.getElementById('nu').value,pin:document.getElementById('np').value});
  document.getElementById('nu').value='';document.getElementById('np').value='';}catch(e){}refreshUsers();}
async function changePin(){try{await api('users/pin',{username:document.getElementById('eu').value,pin:document.getElementById('ep').value});
  document.getElementById('ep').value='';}catch(e){}refreshUsers();}

// ── 삭제 ──
async function refreshDanger(){let d;try{d=await api('danger/state');}catch(e){return;}
  document.getElementById('del-users').innerHTML=d.users.length?"<table>"+d.users.map(u=>
    "<tr><td><b>"+esc(u.username)+"</b> <span class='hint'>"+u.wins+"승 "+u.losses+"패 · "+u.points+"점</span></td>"+
    "<td style='text-align:right;'><button class='redline' onclick=\"delUser('"+esc(u.username)+"')\">삭제</button></td></tr>").join('')+"</table>":
    "<p class='hint'>아이디가 없습니다.</p>";
  document.getElementById('debate-count').textContent='(최근 '+d.debates.length+'건)';
  document.getElementById('del-debates').innerHTML=d.debates.length?"<table>"+d.debates.map(x=>
    "<tr><td><b>"+esc(x.topic)+"</b><br><span class='hint'>"+esc(x.played_at)+" · "+esc(x.player_a)+" vs "+esc(x.player_b)+
    " · "+esc(x.winner)+"</span></td><td style='text-align:right;'><button class='redline' onclick='delDebate("+x.id+")'>삭제</button></td></tr>").join('')+"</table>":
    "<p class='hint'>토론 기록이 없습니다.</p>";
  document.getElementById('del-profiles').innerHTML=d.profiles.length?"<table>"+d.profiles.map(p=>
    "<tr><td><b>"+esc(p.name)+"</b> <span class='hint'>"+esc(p.model)+(p.name===d.active?' · 사용 중':'')+"</span></td>"+
    "<td style='text-align:right;'><button class='redline' onclick=\"delProfile('"+esc(p.name)+"')\">삭제</button></td></tr>").join('')+"</table>":
    "<p class='hint'>등록된 키가 없습니다.</p>";}
async function delUser(n){if(!confirm("'"+n+"' 아이디를 삭제할까요? 되돌릴 수 없습니다."))return;
  try{await api('danger/delete_user',{username:n});}catch(e){}refreshDanger();}
async function delDebate(id,all){
  if(!confirm(all?'토론 기록을 전부 삭제할까요? 되돌릴 수 없습니다.':'이 토론 기록을 삭제할까요?'))return;
  try{await api('danger/delete_debate',all?{all:true}:{id});}catch(e){}refreshDanger();}
async function resetScores(){
  if(!confirm('모든 아이디의 점수와 전적을 0으로 초기화할까요?\n되돌릴 수 없습니다.'))return;
  try{await api('danger/reset_scores');}catch(e){}refreshDanger();}
async function delProfile(n){if(!confirm("'"+n+"' 키를 삭제할까요?"))return;
  try{await api('danger/delete_profile',{name:n});}catch(e){}refreshDanger();}

openTab('ops');
</script>
</body></html>"""

RANKING_TEMPLATE = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>랭킹</title>
<style>
body{font-family:'Malgun Gothic',sans-serif;max-width:700px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:20px;background:white;border-radius:12px;}
h2{margin-top:0;}
.sub{color:#6b7280;font-size:0.9em;margin-bottom:16px;}
table{width:100%;border-collapse:collapse;}
th{text-align:left;border-bottom:2px solid #e5e7eb;padding:10px 6px;font-size:0.85em;color:#6b7280;}
td{border-bottom:1px solid #f3f4f6;padding:12px 6px;}
th.num,td.num{text-align:right;}
.rank{font-weight:bold;color:#6b7280;width:52px;}
.top1 .rank,.top2 .rank,.top3 .rank{font-size:1.15em;}
.top1{background:#fffbeb;} .top2{background:#f8fafc;} .top3{background:#fff7ed;}
.top1 .rank{color:#b45309;} .top2 .rank{color:#64748b;} .top3 .rank{color:#c2410c;}
.name{font-weight:bold;}
.points{font-weight:bold;color:#1d4ed8;}
.rec{color:#6b7280;font-size:0.9em;}
.win{color:#16a34a;font-weight:bold;} .loss{color:#dc2626;font-weight:bold;}
.bar{height:6px;background:#f1f5f9;border-radius:3px;overflow:hidden;margin-top:4px;min-width:70px;}
.bar span{display:block;height:100%;background:#22c55e;}
.empty{color:#9ca3af;text-align:center;padding:30px 0;}
.nav{margin-top:18px;font-size:0.92em;}
.nav a{color:#2563eb;text-decoration:none;} .nav a:hover{text-decoration:underline;}
.show-sm{display:none;}
@media(max-width:520px){.hide-sm{display:none;} td,th{padding:10px 4px;}
  .show-sm{display:block;margin-top:3px;font-size:0.85em;}}
</style></head><body>
<div class="box">
<h2>🏆 랭킹</h2>
<p class="sub">참가자 {{ totals.players }}명 · 누적 토론 {{ totals.debates }}회<br>
점수 = 매 경기 AI 심사 점수(100점 만점)의 누적 · 항복으로 끝난 경기는 점수 없음</p>
<table>
<tr><th class="rank">순위</th><th>아이디</th><th class="hide-sm">전적</th>
<th class="num hide-sm">승률</th><th class="num">점수</th></tr>
{% for r in table %}
<tr class="{% if r.rank == 1 %}top1{% elif r.rank == 2 %}top2{% elif r.rank == 3 %}top3{% endif %}">
  <td class="rank">{% if r.rank == 1 %}🥇{% elif r.rank == 2 %}🥈{% elif r.rank == 3 %}🥉{% else %}{{ r.rank }}{% endif %}</td>
  <td><span class="name">{{ r.username }}</span>
      <div class="rec show-sm"><span class="win">{{ r.wins }}승</span> <span class="loss">{{ r.losses }}패</span>{% if r.rate is not none %} · 승률 {{ r.rate }}%{% endif %}</div></td>
  <td class="hide-sm"><span class="win">{{ r.wins }}승</span> <span class="loss">{{ r.losses }}패</span>
      {% if r.played %}<div class="bar"><span style="width:{{ r.rate }}%"></span></div>{% endif %}</td>
  <td class="num hide-sm">{% if r.rate is not none %}{{ r.rate }}%{% else %}<span style="color:#d1d5db;">–</span>{% endif %}</td>
  <td class="num points">{{ r.points }}</td>
</tr>
{% else %}
<tr><td colspan="5" class="empty">아직 등록된 참가자가 없습니다.</td></tr>
{% endfor %}
</table>
<p class="nav"><a href="/">← 토론 화면</a> · <a href="/history">📚 토론 기록</a> · <a href="/admin">🛠️ 관리자</a></p>
</div>
</body></html>"""

HISTORY_TEMPLATE = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>토론 기록</title>
<style>body{font-family:'Malgun Gothic',sans-serif;max-width:700px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:16px;background:white;margin-bottom:12px;border-radius:12px;}
.meta{font-size:0.85em;color:#6b7280;}.reason{background:#f9fafb;padding:10px;border-radius:8px;font-size:0.9em;margin-top:8px;white-space:pre-line;}
.log-line{margin:6px 0;font-size:0.92em;padding:6px 10px;border-radius:6px;border-left:4px solid #d1d5db;}
.log-line.pro{border-left-color:#16a34a;background:#f0fdf4;}
.log-line.con{border-left-color:#dc2626;background:#fef2f2;}
.log-line .sum{color:#6b7280;font-size:0.9em;}
.winner-tag{display:inline-block;padding:2px 8px;border-radius:6px;font-weight:bold;font-size:0.9em;}
.w-fail{background:#fef3c7;color:#92400e;}
.w-stop{background:#f1f5f9;color:#475569;}</style></head><body>
<h2>📚 지난 토론 기록 (최근 {{ debates|length }}건)</h2>
{% for d in debates %}
<div class="box"><b>📌 {{ d.topic }}</b>
<p class="meta">{{ d.played_at }} | A: {{ d.player_a }} ({{ d.side_a }}) vs B: {{ d.player_b }} ({{ d.side_b }}) | 엔진: {{ d.engine }}</p>
<p><b>결과: </b>{% if d.winner == '심사실패' %}<span class="winner-tag w-fail">⚠️ 심사 실패 (전적 미반영)</span>
{% elif d.winner == '중단' %}<span class="winner-tag w-stop">⏹️ 중단 (전적 미반영)</span>
{% else %}<b>{{ d.winner }}</b>{% endif %}</p>
<div class="reason">{{ d.reason }}</div>
<details><summary>토론 전문 보기</summary>
{% for line in d.logs %}<p class="log-line {{ 'pro' if line.side == '찬성' else 'con' }}"><b>[{{ line.stage }}] {{ line.side }}:</b> {{ line.text }}{% if line.auto %} <span class="sum">⏰ 시간 종료 자동 제출</span>{% endif %}{% if line.summary %}<br><span class="sum">🤖 요약: {{ line.summary }}</span>{% endif %}</p>{% endfor %}
</details></div>{% else %}<p>아직 토론 기록이 없습니다.</p>{% endfor %}
<p><a href="/">← 토론 화면</a> · <a href="/ranking">🏆 랭킹</a> · <a href="/admin">🛠️ 관리자</a></p></body></html>"""


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, engine_name=get_active_name(),
                                  turn_seconds=TURN_SECONDS, min_length=MIN_MESSAGE_LENGTH,
                                  allow_paste=paste_allowed())


@app.route('/history')
def history():
    rows = db_fetchall("SELECT played_at, topic, player_a, player_b, side_a, side_b, log_json, winner, reason, engine "
                        "FROM debates ORDER BY id DESC LIMIT 30")
    debates = []
    for r in rows:
        try:
            logs = json.loads(r[6])
        except Exception:
            logs = []
        debates.append({"played_at": r[0], "topic": r[1], "player_a": r[2], "player_b": r[3],
                         "side_a": r[4], "side_b": r[5], "logs": logs, "winner": r[7], "reason": r[8], "engine": r[9]})
    return render_template_string(HISTORY_TEMPLATE, debates=debates)


@app.route('/leaderboard')
def leaderboard():
    rows = db_fetchall("SELECT username, wins, losses, points FROM users ORDER BY points DESC")
    return jsonify([{"username": r[0], "wins": r[1], "losses": r[2], "points": r[3]} for r in rows])


@app.route('/ranking')
def ranking():
    rows = db_fetchall("SELECT username, wins, losses, points FROM users "
                       "ORDER BY points DESC, wins DESC, username ASC")
    table = []
    prev_points, prev_rank = None, 0
    for i, (username, wins, losses, points) in enumerate(rows, start=1):
        played = wins + losses
        rate = round(wins * 100 / played) if played else None
        rank = prev_rank if points == prev_points else i   # 동점자는 같은 순위
        prev_points, prev_rank = points, rank
        table.append({"rank": rank, "username": username, "wins": wins, "losses": losses,
                      "played": played, "rate": rate, "points": points})
    totals = {
        "players": len(table),
        "debates": (db_fetchone("SELECT COUNT(*) FROM debates") or [0])[0],
    }
    return render_template_string(RANKING_TEMPLATE, table=table, totals=totals)


# ═════════════════════════════════════════════
# 관리자 인증 — 탭 단위 잠금
# 탭에 들어갈 때 코드를 한 번 입력하면 그 탭 안에서는 자유롭게 작업한다.
# 탭을 나가면(다른 탭으로 이동·새로고침·창 닫기) 잠기고, 다시 들어올 때 코드를 묻는다.
# ═════════════════════════════════════════════
ADMIN_TABS = {"ops": "대회 운영", "ai": "AI 설정", "users": "아이디 관리", "danger": "삭제"}
ADMIN_IDLE_SECONDS = 30 * 60      # 탭을 열어둔 채 30분간 아무 요청이 없으면 잠김
admin_tokens = {}                 # token -> {"tab", "last"}


def _check_admin_code(code):
    return hmac.compare_digest((code or "").encode(), ADMIN_CODE.encode())


def _token_ok(token, tab):
    info = admin_tokens.get(token or "")
    if not info or info["tab"] != tab:
        return False
    if time.time() - info["last"] > ADMIN_IDLE_SECONDS:
        admin_tokens.pop(token, None)
        return False
    info["last"] = time.time()
    return True


def admin_api(tab):
    """해당 탭의 잠금을 푼 토큰이 있어야 실행된다."""
    def deco(fn):
        def wrapper(*args, **kwargs):
            if not _token_ok(request.headers.get("X-Admin-Token"), tab):
                return jsonify({"locked": True, "error": "잠긴 탭입니다. 코드를 다시 입력해주세요."}), 401
            try:
                return fn(request.get_json(silent=True) or {}, *args, **kwargs)
            except Exception as e:
                print(f"[관리자 API 오류] {fn.__name__}: {type(e).__name__}: {e}")
                return jsonify({"error": f"처리 중 오류: {type(e).__name__}: {e}"}), 500
        wrapper.__name__ = f"admin_{tab}_{fn.__name__}"
        return wrapper
    return deco


@app.route('/admin')
def admin():
    if ADMIN_CODE == "1234":
        print("[경고] DEBATE_ADMIN_CODE가 설정되지 않아 기본 코드(1234)를 쓰고 있습니다.")
    return render_template_string(ADMIN_TEMPLATE, tabs=ADMIN_TABS)


@app.route('/admin/api/unlock', methods=['POST'])
def admin_unlock():
    data = request.get_json(silent=True) or {}
    tab = data.get("tab")
    if tab not in ADMIN_TABS:
        return jsonify({"error": "알 수 없는 탭입니다."}), 400
    if not _check_admin_code(data.get("code")):
        return jsonify({"error": "코드가 틀렸습니다."}), 403
    # 오래된 토큰 정리
    now = time.time()
    for t in [t for t, v in admin_tokens.items() if now - v["last"] > ADMIN_IDLE_SECONDS]:
        admin_tokens.pop(t, None)
    token = secrets.token_urlsafe(24)
    admin_tokens[token] = {"tab": tab, "last": now}
    return jsonify({"token": token, "tab": tab})


@app.route('/admin/api/lock', methods=['POST'])
def admin_lock():
    data = request.get_json(silent=True) or {}
    admin_tokens.pop(data.get("token") or request.headers.get("X-Admin-Token") or "", None)
    return jsonify({"ok": True})


# ─────────────────────────────────────────────
# 관리자 — 대회 운영 탭
# ─────────────────────────────────────────────
def paste_allowed():
    return get_setting("allow_paste", "0") == "1"


def auto_match_on():
    return get_setting("auto_match", "1") != "0"


def get_reservations():
    try:
        return json.loads(get_setting("reservations", "[]") or "[]")
    except Exception:
        return []


def set_reservations(lst):
    set_setting("reservations", json.dumps(lst, ensure_ascii=False))


def reserved_usernames():
    names = set()
    for r in get_reservations():
        names.add(r["a"])
        names.add(r["b"])
    return names


def room_view(rid, r):
    now = time.time()
    stage_idx = min(r['turn_count'], TOTAL_STAGES - 1)
    if r.get('judging'):
        stage = 'AI 심사 중'
    elif r['turn_count'] == 0:
        stage = '입론 (동시 작성)'
    else:
        stage = STAGES[stage_idx]
    speaker = r['players'][r['current_speaker']]
    return {
        'room_id': rid, 'topic': r['topic'], 'stage': stage,
        'stage_index': stage_idx, 'total_stages': TOTAL_STAGES,
        'turns': len(r['logs']),
        'elapsed_min': int((now - r.get('started_at', now)) // 60),
        'seconds_left': seconds_left(r),
        'paused': bool(r.get('paused_at')),
        'players': [{
            'username': p['username'], 'side': p['side'],
            'connected': p.get('connected', True),
            'absent_sec': (int(now - p['disconnect_at'])
                           if not p.get('connected', True) and p.get('disconnect_at') else 0),
            'speaking': (not r.get('judging')) and (
                (r['turn_count'] == 0 and r['openings'][i] is None) or
                (r['turn_count'] > 0 and p is speaker)),
            'opening_done': r['turn_count'] == 0 and r['openings'][i] is not None,
        } for i, p in enumerate(r['players'])],
    }


def pending_debates():
    rows = db_fetchall("SELECT id, played_at, topic, player_a, player_b, side_a, side_b, winner, "
                       "reason, pending_json FROM debates WHERE winner IN ('판정대기','심사실패') "
                       "ORDER BY id DESC LIMIT 20")
    out = []
    for r in rows:
        try:
            pend = json.loads(r[9]) if r[9] else {}
        except Exception:
            pend = {}
        if pend.get("hidden"):
            continue
        out.append({"id": r[0], "played_at": r[1], "topic": r[2],
                    "player_a": r[3], "player_b": r[4], "side_a": r[5], "side_b": r[6],
                    "winner": r[7], "reason": r[8], "pending": pend,
                    "rejudging": r[0] in _rejudging})
    return out


@app.route('/admin/api/ops/state', methods=['POST'])
@admin_api("ops")
def ops_state(data):
    now = time.time()
    reserved = reserved_usernames()
    return jsonify({
        "auto_match": auto_match_on(),
        "allow_paste": paste_allowed(),
        "waiting": [{"username": p["username"], "wait_sec": int(now - p.get("since", now)),
                     "reserved": p["username"] in reserved} for p in waiting_pool],
        "reservations": get_reservations(),
        "rooms": sorted([room_view(rid, r) for rid, r in rooms.items()], key=lambda x: -x['turns']),
        "pending": pending_debates(),
        "absent_alert": ABSENT_ALERT_SECONDS,
    })


@app.route('/admin/api/ops/auto', methods=['POST'])
@admin_api("ops")
def ops_auto(data):
    on = bool(data.get("on"))
    set_setting("auto_match", "1" if on else "0")
    notify_waiting_status()
    if on:
        try_match()
    return jsonify({"ok": True, "msg": "랜덤 매칭으로 전환" if on else "주작 매칭 모드 — 랜덤 매칭을 멈췄습니다"})


@app.route('/admin/api/ops/paste', methods=['POST'])
@admin_api("ops")
def ops_paste(data):
    on = bool(data.get("on"))
    set_setting("allow_paste", "1" if on else "0")
    return jsonify({"ok": True, "msg": "복사·붙여넣기 " + ("허용 (테스트용)" if on else "금지")
                    + " — 참가자가 새로고침하면 적용됩니다"})


SIDE_MODES = ("random", "a_pro", "a_con")


@app.route('/admin/api/ops/pair', methods=['POST'])
@admin_api("ops")
def ops_pair(data):
    if auto_match_on():
        return jsonify({"error": "주작 매칭 모드를 먼저 켜주세요."}), 400
    a, b = (data.get("a") or "").strip(), (data.get("b") or "").strip()
    side = data.get("side") if data.get("side") in SIDE_MODES else "random"
    topic = (data.get("topic") or "").strip()
    if not a or not b or a == b:
        return jsonify({"error": "서로 다른 두 사람을 골라주세요."}), 400
    pa = next((p for p in waiting_pool if p["username"] == a), None)
    pb = next((p for p in waiting_pool if p["username"] == b), None)
    if not pa or not pb:
        return jsonify({"error": "두 사람 모두 대기실에 있어야 합니다. (토론 매칭 시작을 누른 상태)"}), 400
    waiting_pool.remove(pa)
    waiting_pool.remove(pb)
    # 지정 매칭으로 붙은 두 사람이 다른 예약에 걸려 있으면 그 예약은 지운다
    set_reservations([r for r in get_reservations() if not ({r["a"], r["b"]} & {a, b})])
    if not launch_debate(pa, pb, side, topic):
        return jsonify({"error": "방을 만들지 못해 두 사람을 대기실로 돌려보냈습니다."}), 500
    return jsonify({"ok": True, "msg": f"{a} vs {b} 매칭 완료"})


@app.route('/admin/api/ops/reserve', methods=['POST'])
@admin_api("ops")
def ops_reserve(data):
    if auto_match_on():
        return jsonify({"error": "주작 매칭 모드를 먼저 켜주세요."}), 400
    a, b = (data.get("a") or "").strip(), (data.get("b") or "").strip()
    side = data.get("side") if data.get("side") in SIDE_MODES else "random"
    topic = (data.get("topic") or "").strip()
    if not a or not b or a == b:
        return jsonify({"error": "서로 다른 두 아이디를 입력해주세요."}), 400
    for name in (a, b):
        if not db_fetchone("SELECT username FROM users WHERE username = %s", (name,)):
            return jsonify({"error": f"'{name}' 아이디가 없습니다."}), 400
    lst = get_reservations()
    busy = reserved_usernames()
    if a in busy or b in busy:
        return jsonify({"error": "이미 다른 예약에 들어 있는 사람이 있습니다. 기존 예약을 먼저 지워주세요."}), 400
    lst.append({"id": uuid.uuid4().hex[:8], "a": a, "b": b, "side": side, "topic": topic})
    set_reservations(lst)
    notify_waiting_status()
    try_match()
    return jsonify({"ok": True, "msg": f"예약 등록: {a} vs {b} — 둘 다 대기실에 오면 자동으로 시작"})


@app.route('/admin/api/ops/unreserve', methods=['POST'])
@admin_api("ops")
def ops_unreserve(data):
    rid = data.get("id")
    set_reservations([r for r in get_reservations() if r["id"] != rid])
    notify_waiting_status()
    try_match()
    return jsonify({"ok": True, "msg": "예약을 지웠습니다."})


@app.route('/admin/api/ops/stop', methods=['POST'])
@admin_api("ops")
def ops_stop(data):
    room_id = data.get("room_id")
    room = rooms.get(room_id)
    if not room:
        return jsonify({"error": "이미 끝난 토론입니다."}), 400
    if room.get("judging"):
        return jsonify({"error": "AI 심사 중이라 중단할 수 없습니다. 잠시만 기다려주세요."}), 400
    end_without_result(room_id, "운영자가 토론을 중단했습니다 (승패 미기록 — 재경기 필요)")
    return jsonify({"ok": True, "msg": "토론을 중단했습니다. 두 사람은 대기 화면으로 돌아갑니다."})


@app.route('/admin/api/ops/resolve', methods=['POST'])
@admin_api("ops")
def ops_resolve(data):
    """'토론 의지 없음' 판정 대기 건 처리. action: forfeit_pro / forfeit_con / dismiss"""
    did = data.get("id")
    action = data.get("action")
    row = db_fetchone("SELECT player_a, player_b, side_a, side_b, winner, reason, pending_json "
                      "FROM debates WHERE id = %s", (did,))
    if not row or row[4] != "판정대기":
        return jsonify({"error": "이미 처리됐거나 없는 기록입니다."}), 400
    pa, pb, sa, sb, _, reason, pend_raw = row
    pend = json.loads(pend_raw or "{}")
    by_side = {sa: pa, sb: pb}
    reveal = pend.get("reveal", "")
    if action in ("forfeit_pro", "forfeit_con"):
        loser_side = "찬성" if action == "forfeit_pro" else "반대"
        winner_side = "반대" if loser_side == "찬성" else "찬성"
        record_win_loss(by_side[winner_side], by_side[loser_side])
        add_points(by_side[winner_side], pend.get("scores", {}).get(winner_side, 0))   # 몰수된 쪽은 0점
        headline = (f"{by_side[loser_side]}({loser_side}) 토론 의지 없음 → "
                    f"{by_side[winner_side]}({winner_side}) 몰수승 (운영자 승인)")
        new_reason = (f"{headline}\nAI 지적: {pend.get('no_will_reason','')}\n\n"
                      f"[참고: AI 원래 판정]\n{pend.get('text','')}\n\n{reveal}")
        db_execute("UPDATE debates SET winner = %s, reason = %s, pending_json = NULL WHERE id = %s",
                   ("몰수", new_reason, did))
        return jsonify({"ok": True, "msg": headline})
    if action == "dismiss":
        ai_winner = pend.get("winner")
        if ai_winner in by_side:
            loser = sb if ai_winner == sa else sa
            record_win_loss(by_side[ai_winner], by_side[loser])
        for side, user in by_side.items():
            add_points(user, pend.get("scores", {}).get(side, 0))
        new_reason = f"{pend.get('text','')}\n(토론 의지 없음 지적은 운영자가 기각)\n\n{reveal}"
        db_execute("UPDATE debates SET winner = %s, reason = %s, pending_json = NULL WHERE id = %s",
                   (ai_winner or "무승부", new_reason, did))
        return jsonify({"ok": True, "msg": f"기각 — AI 판정대로 {ai_winner} 승리 적용"})
    return jsonify({"error": "알 수 없는 처리입니다."}), 400


_rejudging = set()


@app.route('/admin/api/ops/hide', methods=['POST'])
@admin_api("ops")
def ops_hide(data):
    """심사실패·판정대기 건을 확인 목록에서 숨긴다 (기록은 그대로, 전적·점수 변화 없음)."""
    did = data.get("id")
    row = db_fetchone("SELECT pending_json FROM debates WHERE id = %s", (did,))
    if not row:
        return jsonify({"error": "없는 기록입니다."}), 400
    try:
        pend = json.loads(row[0] or "{}")
    except Exception:
        pend = {}
    pend["hidden"] = True
    db_execute("UPDATE debates SET pending_json = %s WHERE id = %s", (json.dumps(pend, ensure_ascii=False), did))
    return jsonify({"ok": True, "msg": "확인 목록에서 숨겼습니다. (토론 기록에는 남아 있음)"})


@app.route('/admin/api/ops/rejudge', methods=['POST'])
@admin_api("ops")
def ops_rejudge(data):
    did = data.get("id")
    row = db_fetchone("SELECT winner FROM debates WHERE id = %s", (did,))
    if not row or row[0] != "심사실패":
        return jsonify({"error": "심사실패 기록만 다시 심사할 수 있습니다."}), 400
    if did in _rejudging:
        return jsonify({"error": "이미 다시 심사하는 중입니다."}), 400
    _rejudging.add(did)
    socketio.start_background_task(rejudge_in_background, did)
    return jsonify({"ok": True, "msg": "다시 심사를 시작했습니다. 잠시 후 목록에 반영됩니다."})


def rejudge_in_background(did):
    try:
        row = db_fetchone("SELECT topic, player_a, player_b, side_a, side_b, log_json, pending_json "
                          "FROM debates WHERE id = %s", (did,))
        if not row:
            return
        topic, pa, pb, sa, sb, log_json, pend_raw = row
        logs = json.loads(log_json or "[]")
        reveal = (json.loads(pend_raw or "{}")).get("reveal", "")
        result = judge_debate(topic, logs)
        winner, reason, pending = settle_result(result, (pa, sa), (pb, sb), reveal)
        db_execute("UPDATE debates SET winner = %s, reason = %s, pending_json = %s WHERE id = %s",
                   (winner, reason, json.dumps(pending, ensure_ascii=False) if pending else None, did))
        socketio.emit('admin_alert', {'msg': f"재심사 완료 — {topic}: {winner}"}, room="admins")
    finally:
        _rejudging.discard(did)


# ─────────────────────────────────────────────
# 관리자 — AI 설정 탭
# ─────────────────────────────────────────────
@app.route('/admin/api/ai/state', methods=['POST'])
@admin_api("ai")
def ai_state(data):
    stats, recent, billing_hit, rate_hit = ai_call_summary()
    return jsonify({
        "active": get_active_name(),
        "profiles": [{"name": p["name"], "model": p["model"],
                      "key_ok": bool(p["api_key"].strip()), "masked": mask_key(p["api_key"]),
                      "redirect": _model_redirect.get(p["model"], "")} for p in list_profiles()],
        "default_model": DEFAULT_MODEL,
        "stats": stats, "recent": recent, "billing_hit": billing_hit, "rate_hit": rate_hit,
        "summary_on": summaries_enabled(), "allow_draw": draws_allowed(),
        "rpm_used": current_rpm(), "rpm_limit": max_rpm(),
        "backup_count": len(backup_profiles()),
    })


@app.route('/admin/api/ai/switch', methods=['POST'])
@admin_api("ai")
def ai_switch(data):
    chosen = data.get("profile") or TEST_MODE
    if chosen not in [p['name'] for p in list_profiles()] + [TEST_MODE]:
        return jsonify({"error": "존재하지 않는 AI입니다."}), 400
    set_setting("active_profile", chosen)
    if chosen != TEST_MODE:
        socketio.start_background_task(refill_topic_cache)
    return jsonify({"ok": True, "msg": f"'{chosen}'(으)로 변경했습니다."})


@app.route('/admin/api/ai/test', methods=['POST'])
@admin_api("ai")
def ai_test(data):
    ok, detail = test_llm_connection()
    return jsonify({"ok": ok, "msg": ("✅ 연결 성공! " if ok else "❌ 연결 실패: ") + detail})


@app.route('/admin/api/ai/save', methods=['POST'])
@admin_api("ai")
def ai_save(data):
    name = (data.get("name") or "").strip()
    key = (data.get("key") or "").strip()
    if not name:
        return jsonify({"error": "이름을 입력해주세요."}), 400
    if name == TEST_MODE:
        return jsonify({"error": f"'{TEST_MODE}'는 예약된 이름입니다."}), 400
    existing = db_fetchone("SELECT model, api_key FROM ai_profiles WHERE name = %s", (name,))
    model = DEFAULT_MODEL
    key = key or ((existing[1] if existing else '') or '')
    if not key:
        return jsonify({"error": "API 키를 입력해주세요. (신규 등록은 키가 필요합니다)"}), 400
    upsert_profile(name, PROVIDER, model, key)
    _model_redirect.pop(model, None)
    if data.get("activate"):
        set_setting("active_profile", name)
        socketio.start_background_task(refill_topic_cache)
    return jsonify({"ok": True, "msg": f"'{name}' {'수정' if existing else '등록'} 완료 ({model})"
                    + (" — 활성화됨" if data.get("activate") else "")})


@app.route('/admin/api/ai/settings', methods=['POST'])
@admin_api("ai")
def ai_settings(data):
    parts = []
    if "summary_on" in data:
        set_setting("summary_enabled", "1" if data["summary_on"] else "0")
        parts.append("발언 요약 " + ("사용" if data["summary_on"] else "끔"))
    if "allow_draw" in data:
        set_setting("allow_draw", "1" if data["allow_draw"] else "0")
        parts.append("무승부 " + ("허용" if data["allow_draw"] else "금지(토너먼트용)"))
    if str(data.get("max_rpm") or "").strip():
        try:
            val = max(1, min(1000, int(data["max_rpm"])))
            set_setting("ai_max_rpm_v2", str(val))
            parts.append(f"분당 상한 {val}회")
        except (TypeError, ValueError):
            return jsonify({"error": "분당 상한은 숫자로 입력해주세요."}), 400
    return jsonify({"ok": True, "msg": "저장: " + ", ".join(parts) if parts else "변경 사항 없음"})


# ─────────────────────────────────────────────
# 관리자 — 아이디 관리 / 삭제 탭
# ─────────────────────────────────────────────
def users_list():
    rows = db_fetchall("SELECT username, wins, losses, points FROM users ORDER BY username")
    return [{"username": r[0], "wins": r[1], "losses": r[2], "points": r[3]} for r in rows]


@app.route('/admin/api/users/state', methods=['POST'])
@admin_api("users")
def users_state(data):
    return jsonify({"users": users_list()})


@app.route('/admin/api/users/create', methods=['POST'])
@admin_api("users")
def users_create(data):
    new_user = (data.get("username") or "").strip()
    new_pin = (data.get("pin") or "").strip()
    if not new_user or len(new_pin) < 4:
        return jsonify({"error": "아이디와 4자리 이상 PIN을 입력해주세요."}), 400
    if db_fetchone("SELECT username FROM users WHERE username = %s", (new_user,)):
        return jsonify({"error": f"'{new_user}' 아이디가 이미 존재합니다."}), 400
    db_execute("INSERT INTO users (username, pin_hash) VALUES (%s, %s)", (new_user, hash_pin(new_user, new_pin)))
    return jsonify({"ok": True, "msg": f"'{new_user}' 생성 완료"})


@app.route('/admin/api/users/pin', methods=['POST'])
@admin_api("users")
def users_pin(data):
    target = (data.get("username") or "").strip()
    new_pin = (data.get("pin") or "").strip()
    if not target or len(new_pin) < 4:
        return jsonify({"error": "아이디와 4자리 이상 새 PIN을 입력해주세요."}), 400
    if not db_fetchone("SELECT username FROM users WHERE username = %s", (target,)):
        return jsonify({"error": f"'{target}' 아이디를 찾을 수 없습니다."}), 400
    db_execute("UPDATE users SET pin_hash = %s WHERE username = %s", (hash_pin(target, new_pin), target))
    return jsonify({"ok": True, "msg": f"'{target}'의 PIN을 변경했습니다."})


@app.route('/admin/api/danger/state', methods=['POST'])
@admin_api("danger")
def danger_state(data):
    debates = db_fetchall("SELECT id, played_at, topic, player_a, player_b, winner FROM debates "
                          "ORDER BY id DESC LIMIT 100")
    return jsonify({"users": users_list(),
                    "profiles": [{"name": p["name"], "model": p["model"]} for p in list_profiles()],
                    "active": get_active_name(),
                    "debates": [{"id": r[0], "played_at": r[1], "topic": r[2], "player_a": r[3],
                                 "player_b": r[4], "winner": r[5]} for r in debates]})


@app.route('/admin/api/danger/delete_debate', methods=['POST'])
@admin_api("danger")
def danger_delete_debate(data):
    if data.get("all"):
        db_execute("DELETE FROM debates")
        return jsonify({"ok": True, "msg": "토론 기록을 모두 삭제했습니다. (전적·점수는 그대로)"})
    did = data.get("id")
    if not db_fetchone("SELECT id FROM debates WHERE id = %s", (did,)):
        return jsonify({"error": "없는 기록입니다."}), 400
    db_execute("DELETE FROM debates WHERE id = %s", (did,))
    return jsonify({"ok": True, "msg": "토론 기록 1건을 삭제했습니다. (전적·점수는 그대로)"})


@app.route('/admin/api/danger/delete_user', methods=['POST'])
@admin_api("danger")
def danger_delete_user(data):
    target = (data.get("username") or "").strip()
    if not db_fetchone("SELECT username FROM users WHERE username = %s", (target,)):
        return jsonify({"error": f"'{target}' 아이디를 찾을 수 없습니다."}), 400
    if target in user_to_room:
        return jsonify({"error": "토론 중인 아이디는 삭제할 수 없습니다."}), 400
    db_execute("DELETE FROM users WHERE username = %s", (target,))
    return jsonify({"ok": True, "msg": f"'{target}' 삭제 완료"})


@app.route('/admin/api/danger/reset_scores', methods=['POST'])
@admin_api("danger")
def danger_reset_scores(data):
    db_execute("UPDATE users SET wins = 0, losses = 0, points = 0")
    return jsonify({"ok": True, "msg": "모든 아이디의 점수와 전적을 0으로 초기화했습니다. (토론 기록은 남아 있음)"})


@app.route('/admin/api/danger/delete_profile', methods=['POST'])
@admin_api("danger")
def danger_delete_profile(data):
    target = (data.get("name") or "").strip()
    if not db_fetchone("SELECT name FROM ai_profiles WHERE name = %s", (target,)):
        return jsonify({"error": f"'{target}'을(를) 찾을 수 없습니다."}), 400
    delete_profile(target)
    return jsonify({"ok": True, "msg": f"'{target}' 삭제 완료"})


@socketio.on('admin_watch')
def handle_admin_watch(data):
    data = data or {}
    if not _token_ok(data.get('token'), "ops"):
        emit('admin_locked', {})
        return
    join_room("admins")
    room_id = data.get('room_id')
    if not room_id:
        return
    room = rooms.get(room_id)
    if not room:
        emit('error_msg', {'msg': '존재하지 않거나 이미 종료된 토론입니다.'})
        return
    join_room(f"watch_{room_id}")
    by = {p['side']: p['username'] for p in room['players']}
    history_logs = [{'stage': l['stage'], 'side': l['side'], 'username': by.get(l['side'], ''),
                     'message': l['text']} for l in room['logs']]
    emit('admin_history', {'room_id': room_id, 'topic': room['topic'], 'logs': history_logs})


# ═════════════════════════════════════════════
# 토론 진행
# ═════════════════════════════════════════════
def seconds_left(room):
    """남은 시간. 누가 끊겨 시계가 멈춰 있으면 멈춘 시점 기준."""
    ref = room.get('paused_at') or time.time()
    return max(0, int(round(room['deadline'] - ref)))


def pause_clock(room):
    if not room.get('paused_at'):
        room['paused_at'] = time.time()


def resume_clock(room):
    """모두 돌아왔으면 멈춘 시간만큼 마감을 미룬다."""
    if room.get('paused_at') and all(p.get('connected', True) for p in room['players']):
        room['deadline'] += time.time() - room['paused_at']
        room['paused_at'] = None


def close_room(room_id):
    room = rooms.pop(room_id, None)
    if room:
        for p in room['players']:
            sid_to_room.pop(p['sid'], None)
            user_to_room.pop(p['username'], None)


def make_reveal_text(room):
    a, b = room['players'][0], room['players'][1]
    by = {p['side']: p['username'] for p in room['players']}
    return f"🎭 정체 공개!\n찬성 = {by.get('찬성','?')}\n반대 = {by.get('반대','?')}"


def who(player):
    """기록에 남길 이름 표기 — 아이디(찬성/반대)"""
    return f"{player['username']}({player['side']})"


def end_by_surrender(room_id, loser, winner):
    """항복 — 본인이 직접 버튼을 누른 경우에만 몰수패."""
    room = rooms.get(room_id)
    if not room:
        return
    record_win_loss(winner['username'], loser['username'])
    reveal = make_reveal_text(room)
    headline = f"{who(loser)} 항복으로 {who(winner)} 몰수승"
    save_debate(room['topic'], room['players'][0]['username'], room['players'][1]['username'],
                room['players'][0]['side'], room['players'][1]['side'],
                room['logs'], "몰수", f"{headline}\n\n{reveal}")
    socketio.emit('opponent_left', {'reveal': reveal, 'headline': headline}, room=winner['sid'])
    socketio.emit('surrendered', {'reveal': reveal, 'headline': headline}, room=loser['sid'])
    socketio.emit('admin_end', {'room_id': room_id, 'result': f"{headline}\n\n{reveal}"}, room=f"watch_{room_id}")
    close_room(room_id)


def end_without_result(room_id, headline):
    """승패 없이 토론을 끝낸다 (운영자 중단). 두 사람에게 알리고 기록은 '중단'으로 남긴다."""
    room = rooms.get(room_id)
    if not room:
        return
    reveal = make_reveal_text(room)
    save_debate(room['topic'], room['players'][0]['username'], room['players'][1]['username'],
                room['players'][0]['side'], room['players'][1]['side'],
                room['logs'], "중단", f"{headline}\n\n{reveal}")
    for p in room['players']:
        socketio.emit('debate_aborted', {'reveal': reveal, 'headline': headline}, room=p['sid'])
    socketio.emit('admin_end', {'room_id': room_id, 'result': f"{headline}\n\n{reveal}"},
                  room=f"watch_{room_id}")
    close_room(room_id)


@socketio.on('surrender')
def handle_surrender(data):
    sid = request.sid
    room_id = sid_to_room.get(sid)
    room = rooms.get(room_id) if room_id else None
    if not room or room.get('judging'):
        return
    me = next((p for p in room['players'] if p['sid'] == sid), None)
    opp = next((p for p in room['players'] if p['sid'] != sid), None)
    if not me or not opp:
        return
    end_by_surrender(room_id, me, opp)


def _take_over_session(username, new_sid):
    """
    같은 아이디가 새로 접속하면 새 접속을 살린다 (새로고침·기기 변경·네트워크 전환).
    예전 접속은 조용히 떼어내고, 그 화면에는 '다른 곳에서 접속됨'을 띄운다.
    """
    for old_sid in [s for s, u in sid_to_user.items() if u == username and s != new_sid]:
        sid_to_user.pop(old_sid, None)
        sid_to_room.pop(old_sid, None)
        waiting_pool[:] = [p for p in waiting_pool if p['sid'] != old_sid]
        socketio.emit('kicked', {}, room=old_sid)
    sid_to_user[new_sid] = username


def last_result(username):
    """이 참가자의 가장 최근 경기 결과 (결과창을 놓쳤을 때 로비에서 다시 보도록)."""
    row = db_fetchone("SELECT played_at, topic, player_a, side_a, side_b, winner, reason FROM debates "
                      "WHERE player_a = %s OR player_b = %s ORDER BY id DESC LIMIT 1", (username, username))
    if not row:
        return None
    played_at, topic, pa, sa, sb, winner, reason = row
    my_side = sa if pa == username else sb
    if winner in ("찬성", "반대"):
        outcome = "승리" if winner == my_side else "패배"
    elif winner == "몰수":
        # 몰수 문구에서 승자 아이디를 찾는다: '... → 아이디(진영) 몰수승' / '아이디(진영) 항복으로 아이디(진영) 몰수승'
        head = (reason or "").split("\n")[0]
        winner_part = head.split("으로 ")[-1] if "항복으로" in head else head.split("→")[-1]
        outcome = "몰수승" if winner_part.strip().startswith(f"{username}(") else "몰수패"
    else:
        outcome = {"무승부": "무승부", "중단": "중단 (승패 없음)", "판정대기": "운영자 확인 중",
                   "심사실패": "심사 대기 중"}.get(winner, winner)
    return {"played_at": played_at, "topic": topic, "my_side": my_side,
            "outcome": outcome, "reason": reason or ""}


def _profile_payload(username, token):
    stats = get_user_stats(username) or {"wins": 0, "losses": 0, "points": 0, "rank": "-"}
    return {'username': username, 'resume_token': token,
            'wins': stats['wins'], 'losses': stats['losses'],
            'points': stats['points'], 'rank': stats['rank'],
            'last_result': last_result(username)}


def _seat_in_room(username, sid):
    """진행 중인 토론이 있으면 그 자리에 다시 앉히고 True."""
    room_id = user_to_room.get(username)
    room = rooms.get(room_id) if room_id else None
    if not room:
        return False
    me = next((p for p in room['players'] if p['username'] == username), None)
    opp = next((p for p in room['players'] if p['username'] != username), None)
    if not me or not opp:
        return False
    my_index = room['players'].index(me)
    if me['sid'] != sid:
        sid_to_room.pop(me['sid'], None)
    me['sid'] = sid
    me['connected'] = True
    me.pop('disconnect_at', None)
    sid_to_room[sid] = room_id
    socketio.server.enter_room(sid, room_id, namespace='/')
    resume_clock(room)

    opening = room['turn_count'] == 0
    stage_idx = min(room['turn_count'], TOTAL_STAGES - 1)
    socketio.emit('resumed', {
        'room_id': room_id, 'topic': room['topic'],
        'my_side': me['side'], 'opp_side': opp['side'],
        'stage': STAGES[stage_idx], 'stage_index': stage_idx, 'total_stages': TOTAL_STAGES,
        'opening': opening,
        'my_opening_done': opening and room['openings'][my_index] is not None,
        'opp_opening_done': opening and room['openings'][1 - my_index] is not None,
        'your_turn': (not opening) and room['current_speaker'] == my_index,
        'judging': bool(room.get('judging')),
        'seconds_left': seconds_left(room),
        'opp_connected': opp.get('connected', True),
        'speaker_side': room['players'][room['current_speaker']]['side'],
        'criteria': [{'stage': s, 'desc': d} for s, d in JUDGE_CRITERIA],
        'min_length': MIN_MESSAGE_LENGTH,
        'logs': [{'side': l['side'], 'stage': l['stage'], 'message': l['text'], 'auto': l.get('auto', False),
                  'summary': l.get('summary')} for l in room['logs']],
    }, room=sid)
    if opp.get('connected', True):
        socketio.emit('opponent_reconnected', {'seconds_left': seconds_left(room)}, room=opp['sid'])
    return True


@socketio.on('login')
def handle_login(data):
    username = (data.get('username') or '').strip()
    pin = (data.get('pin') or '').strip()
    if not username or len(pin) < 4:
        emit('error_msg', {'msg': '아이디를 입력하고 PIN은 4자리 이상으로 해주세요.'})
        return
    row = db_fetchone("SELECT pin_hash FROM users WHERE username = %s", (username,))
    if not row:
        emit('error_msg', {'msg': '존재하지 않는 아이디입니다. 운영자에게 문의하세요.'})
        return
    if row[0] != hash_pin(username, pin):
        emit('error_msg', {'msg': 'PIN이 틀렸습니다.'})
        return
    _take_over_session(username, request.sid)
    token = uuid.uuid4().hex
    resume_tokens[token] = username
    emit('login_ok', _profile_payload(username, token))
    _seat_in_room(username, request.sid)   # 토론 도중 튕겨서 다시 로그인한 경우 바로 복귀


@socketio.on('resume')
def handle_resume(data):
    """새로고침·재접속. 토론 중이면 원래 자리로, 아니면 로그인 상태로 복귀."""
    token = (data or {}).get('token')
    username = resume_tokens.get(token)
    if not username:
        emit('resume_failed', {})
        return
    _take_over_session(username, request.sid)
    if not _seat_in_room(username, request.sid):
        emit('login_ok', _profile_payload(username, token))


def notify_waiting_status():
    """
    대기 중인 사람들에게 안내. 랜덤인지, 운영자가 지정·예약한 매칭인지
    참가자가 알 수 없도록 모두 같은 문구를 보낸다.
    """
    for p in waiting_pool:
        socketio.emit('status', {'msg': '상대방 매칭을 기다리는 중... 매칭되면 바로 시작합니다.'}, room=p['sid'])


@socketio.on('join_queue')
def handle_join_queue(data):
    user_sid = request.sid
    username = sid_to_user.get(user_sid)
    if not username:
        emit('error_msg', {'msg': '먼저 로그인해주세요.'})
        return
    if user_sid in sid_to_room or username in user_to_room:
        return
    if any(p['username'] == username for p in waiting_pool):
        return
    waiting_pool.append({"sid": user_sid, "username": username, "since": time.time()})
    notify_waiting_status()
    try_match()


def try_match():
    """
    1) 예약 매칭: 예약된 두 사람이 모두 대기실에 있으면 바로 시작.
    2) 자동 매칭(켜져 있을 때만): 예약이 없는 사람끼리 랜덤으로.
    """
    lst = get_reservations()
    remaining = []
    changed = False
    for r in lst:
        pa = next((p for p in waiting_pool if p['username'] == r['a']), None)
        pb = next((p for p in waiting_pool if p['username'] == r['b']), None)
        if pa and pb:
            waiting_pool.remove(pa)
            waiting_pool.remove(pb)
            changed = True
            launch_debate(pa, pb, r.get('side', 'random'), r.get('topic', ''))
        else:
            remaining.append(r)
    if changed:
        set_reservations(remaining)

    if auto_match_on():
        reserved = {n for r in remaining for n in (r['a'], r['b'])}
        free = [p for p in waiting_pool if p['username'] not in reserved]
        while len(free) >= 2:
            p1 = free.pop(random.randrange(len(free)))
            p2 = free.pop(random.randrange(len(free)))
            waiting_pool.remove(p1)
            waiting_pool.remove(p2)
            launch_debate(p1, p2)
    notify_waiting_status()


def launch_debate(p1, p2, side_mode="random", topic=""):
    """방을 만든다. 실패하면 두 사람을 대기실로 되돌린다."""
    try:
        start_debate(p1, p2, side_mode, topic)
        return True
    except Exception as e:
        print(f"[매칭 실패] {type(e).__name__}: {e}")
        for p in (p1, p2):
            if p not in waiting_pool:
                waiting_pool.append(p)
            socketio.emit('error_msg',
                          {'msg': '매칭 중 문제가 생겨 대기실로 돌아갔습니다. 잠시 후 다시 시도됩니다.'},
                          room=p['sid'])
        return False


def start_debate(p1, p2, side_mode="random", topic=""):
    if side_mode == "a_pro":
        side_a, side_b = "찬성", "반대"
    elif side_mode == "a_con":
        side_a, side_b = "반대", "찬성"
    else:
        side_a, side_b = random.sample(["찬성", "반대"], 2)
    p1.update(side=side_a, connected=True)
    p2.update(side=side_b, connected=True)
    players = [p1, p2]
    random.shuffle(players)            # 반론 이후 누가 먼저 말할지는 무작위
    room_id = f"room_{uuid.uuid4().hex[:8]}"
    topic = to_statement(topic) if topic else take_topic()
    now = time.time()
    rooms[room_id] = {"players": players, "turn_count": 0, "current_speaker": 0, "turn_pos": 0,
                      "topic": topic, "logs": [], "openings": [None, None],
                      "deadline": now + OPENING_SECONDS, "paused_at": None,
                      "started_at": now}
    for p in players:
        p.pop('since', None)
        sid_to_room[p['sid']] = room_id
        user_to_room[p['username']] = room_id
        socketio.server.enter_room(p['sid'], room_id, namespace='/')
    common = {'room_id': room_id, 'topic': topic, 'stage': STAGES[0], 'stage_index': 0,
              'total_stages': TOTAL_STAGES, 'seconds_left': OPENING_SECONDS,
              'opening': True, 'your_turn': False,
              'criteria': [{'stage': s, 'desc': d} for s, d in JUDGE_CRITERIA],
              'min_length': MIN_MESSAGE_LENGTH}
    for me, opp in ((players[0], players[1]), (players[1], players[0])):
        socketio.emit('match_found', {**common,
                                      'my_side': me['side'], 'opp_side': opp['side']}, room=me['sid'])


def summarize_in_background(room_id, msg_id, text, log_entry=None):
    """AI 요약을 따로 돌려 준비되는 대로 채워 넣는다. 토론 기록에도 함께 남긴다."""
    summary = get_ai_summary(text)
    if summary and log_entry is not None:
        log_entry['summary'] = summary
    socketio.emit('receive_summary', {'msg_id': msg_id, 'summary': summary}, room=room_id)
    if summary:
        socketio.emit('admin_summary', {'room_id': room_id, 'msg_id': msg_id, 'summary': summary},
                      room=f"watch_{room_id}")


def settle_result(result, a, b, reveal):
    """
    AI 심사 결과를 전적에 반영할지 결정한다. a, b = (아이디, 진영)
    반환: (winner 칸에 적을 값, 결과 문구, pending_json)
    - 심사 실패 → '심사실패' (운영자가 재심사)
    - 토론 의지 없음 지적 → '판정대기' (운영자가 승인/기각할 때까지 전적 미반영)
    - 그 외 → 바로 반영
    """
    by_side = {a[1]: a[0], b[1]: b[0]}
    if not result["ok"]:
        return "심사실패", result["text"] + "\n\n" + reveal, {"type": "rejudge", "reveal": reveal}
    scores = {"찬성": result.get("pro_total", 0), "반대": result.get("con_total", 0)}
    if result["no_will_side"] != "없음":
        pending = {"type": "no_will", "winner": result["winner"], "text": result["text"], "scores": scores,
                   "no_will_side": result["no_will_side"],
                   "no_will_reason": result["no_will_reason"], "reveal": reveal}
        text = ("⏳ AI가 '토론 의지 없음'을 지적해 운영자가 확인 중입니다.\n"
                "최종 결과는 운영자 확인 후 랭킹·기록에 반영됩니다.\n\n" + reveal)
        socketio.emit('admin_alert', {'msg': f"⚠️ 판정 대기 1건 — {result['no_will_side']}측 토론 의지 없음 의심"},
                      room="admins")
        return "판정대기", text, pending
    winner = result["winner"]
    if winner in by_side:
        loser_side = b[1] if winner == a[1] else a[1]
        record_win_loss(by_side[winner], by_side[loser_side])
    for side, user in by_side.items():
        add_points(user, scores.get(side, 0))     # 승패와 상관없이 각자 받은 심사 점수를 누적
    return winner, result["text"] + "\n\n" + reveal, None


def judge_in_background(room_id):
    """AI 심사를 따로 돌린다. 그동안 참가자에게는 심사 중 안내가 떠 있다."""
    room = rooms.get(room_id)
    if not room:
        return
    try:
        result = judge_debate(room['topic'], room['logs'])
        pa, pb = room['players'][0], room['players'][1]
        reveal = make_reveal_text(room)
        winner, final_text, pending = settle_result(result, (pa['username'], pa['side']),
                                                    (pb['username'], pb['side']), reveal)
        save_debate(room['topic'], pa['username'], pb['username'], pa['side'], pb['side'],
                    room['logs'], winner, final_text, pending)
    except Exception as e:
        print(f"[심사 처리 오류] {type(e).__name__}: {e}")
        final_text = "⚠️ 심사 처리 중 오류가 났습니다. 운영자에게 알려주세요."
    socketio.emit('debate_end', {'result': final_text}, room=room_id)
    socketio.emit('admin_end', {'room_id': room_id, 'result': final_text}, room=f"watch_{room_id}")
    close_room(room_id)


def _post_speech(room_id, room, speaker, text, auto=False):
    """발언 하나를 기록하고 양쪽·관전자에게 보낸다. auto=시간 종료로 자동 제출된 것."""
    stage_name = STAGES[min(room['turn_count'], TOTAL_STAGES - 1)]
    msg_id = uuid.uuid4().hex[:12]
    entry = {"side": speaker['side'], "stage": stage_name, "text": text, "auto": auto}
    room['logs'].append(entry)
    socketio.emit('receive_message', {'side': speaker['side'], 'auto': auto,
                                      'message': text, 'msg_id': msg_id, 'stage': stage_name},
                  room=room_id)
    socketio.emit('admin_message', {'room_id': room_id, 'side': speaker['side'], 'msg_id': msg_id,
                                    'username': speaker['username'], 'auto': auto,
                                    'message': text, 'stage': stage_name}, room=f"watch_{room_id}")
    if text != EMPTY_MESSAGE:
        socketio.start_background_task(summarize_in_background, room_id, msg_id, text, entry)


def _advance(room_id, room):
    """다음 발언자로 넘기거나, 마지막이면 심사로."""
    if room['turn_count'] >= TOTAL_STAGES:
        room['judging'] = True
        socketio.emit('judging', {'msg': 'AI가 토론을 심사하고 있습니다...'}, room=room_id)
        socketio.emit('admin_stage', {'room_id': room_id, 'stage': 'AI 심사 중'}, room=f"watch_{room_id}")
        socketio.start_background_task(judge_in_background, room_id)
        return
    next_idx = room['current_speaker']
    room['deadline'] = time.time() + turn_limit(room['turn_count'])
    if room.get('paused_at'):
        room['paused_at'] = time.time()
    for i, p in enumerate(room['players']):
        socketio.emit('turn_change', {
            'your_turn': i == next_idx, 'stage': STAGES[room['turn_count']],
            'stage_index': room['turn_count'], 'stage_changed': room.get('_stage_changed', False),
            'seconds_left': seconds_left(room),
            'speaker_side': room['players'][next_idx]['side']}, room=p['sid'])
    if room.get('_stage_changed'):
        socketio.emit('admin_stage', {'room_id': room_id, 'stage': STAGES[room['turn_count']]},
                      room=f"watch_{room_id}")
    room['_stage_changed'] = False


def submit_speech(room_id, idx, text, auto=False):
    """
    발언 제출 (사람이 보낸 것이든 시간 종료로 자동 제출된 것이든 같은 길로 처리).
    반환: 오류 문구 또는 None
    """
    room = rooms.get(room_id)
    if not room or room.get('judging'):
        return "진행 중인 토론이 아닙니다."
    me = room['players'][idx]
    text = (text or "").strip() or EMPTY_MESSAGE
    room.setdefault('drafts', {}).pop(idx, None)

    # ── 입론: 두 사람이 동시에 쓰고, 둘 다 내면(또는 시간이 끝나면) 함께 공개 ──
    if room['turn_count'] == 0:
        if room['openings'][idx] is not None:
            return "이미 입론을 제출했습니다."
        room['openings'][idx] = text
        room.setdefault('opening_auto', {})[idx] = auto
        socketio.emit('opening_submitted', {}, room=me['sid'])
        opp = room['players'][1 - idx]
        socketio.emit('opp_opening_submitted', {}, room=opp['sid'])
        socketio.emit('admin_stage', {'room_id': room_id,
                                      'stage': f"{me['side']} 입론 제출 (상대 대기 중)"},
                      room=f"watch_{room_id}")
        if all(o is not None for o in room['openings']):
            for i, p in enumerate(room['players']):
                _post_speech(room_id, room, p, room['openings'][i], room['opening_auto'].get(i, False))
            room['turn_count'] = 1
            room['turn_pos'] = 0
            room['current_speaker'] = _speaker_index(room)
            room['_stage_changed'] = True
            _advance(room_id, room)
        return None

    # ── 반론 이후: 단계별로 정해진 진영 순서대로 (SPEAK_ORDER) ──
    if room['current_speaker'] != idx:
        return "지금은 당신의 발언 차례가 아닙니다."
    _post_speech(room_id, room, me, text, auto)
    if room.get('turn_pos', 0) == 0:
        room['turn_pos'] = 1
    else:
        room['turn_count'] += 1
        room['turn_pos'] = 0
        room['_stage_changed'] = True
    if room['turn_count'] < TOTAL_STAGES:
        room['current_speaker'] = _speaker_index(room)
    _advance(room_id, room)
    return None


def _speaker_index(room):
    """지금 단계·순번에 말할 진영의 참가자 번호."""
    side = SPEAK_ORDER[room['turn_count']][room.get('turn_pos', 0)]
    return next(i for i, p in enumerate(room['players']) if p['side'] == side)


@socketio.on('send_message')
def handle_send_message(data):
    room_id = data.get('room_id')
    msg_text = (data.get('message') or '').strip()
    auto = bool(data.get('auto'))       # 시간이 끝나 브라우저가 쓰던 내용을 그대로 보낸 것
    room = rooms.get(room_id)
    if not room or (not msg_text and not auto):
        return
    idx = next((i for i, p in enumerate(room['players']) if p['sid'] == request.sid), None)
    if idx is None:
        emit('error_msg', {'msg': '이 토론의 참가자가 아닙니다. 새로고침해주세요.'})
        return
    # 너무 짧은 발언은 반려한다 (시간 종료 자동 제출은 길이와 상관없이 그대로 받는다)
    if not auto and len(msg_text) < MIN_MESSAGE_LENGTH:
        emit('message_rejected', {
            'msg': f'발언이 너무 짧습니다. 최소 {MIN_MESSAGE_LENGTH}자 이상 작성해주세요.',
            'text': msg_text, 'length': len(msg_text), 'required': MIN_MESSAGE_LENGTH})
        return
    err = submit_speech(room_id, idx, msg_text, auto)
    if err and not auto:
        emit('error_msg', {'msg': err})


@socketio.on('draft')
def handle_draft(data):
    """작성 중인 글을 서버에 보관 — 화면이 꺼져 자동 제출이 안 와도 서버가 이걸 대신 제출한다."""
    room = rooms.get((data or {}).get('room_id'))
    if not room:
        return
    idx = next((i for i, p in enumerate(room['players']) if p['sid'] == request.sid), None)
    if idx is not None:
        room.setdefault('drafts', {})[idx] = (data.get('text') or '')[:20000]


def deadline_watcher():
    """
    브라우저가 잠들거나(모바일 화면 꺼짐) 자동 전송을 못 보내도 토론이 멈추지 않도록,
    마감이 지난 차례는 서버가 마지막으로 받은 초안을 자동 제출한다. 누가 끊겨 시계가 멈춘 방은 건드리지 않는다.
    """
    while True:
        socketio.sleep(2)
        try:
            now = time.time()
            for room_id, room in list(rooms.items()):
                if room.get('judging') or room.get('paused_at'):
                    continue
                if now < room['deadline'] + SERVER_TIMEOUT_GRACE:
                    continue
                if room['turn_count'] == 0:
                    for i in (0, 1):
                        if rooms.get(room_id) and room['openings'][i] is None and room['turn_count'] == 0:
                            submit_speech(room_id, i, room.get('drafts', {}).get(i, ''), auto=True)
                else:
                    i = room['current_speaker']
                    submit_speech(room_id, i, room.get('drafts', {}).get(i, ''), auto=True)
        except Exception as e:
            print(f"[마감 감시 오류] {type(e).__name__}: {e}")


socketio.start_background_task(deadline_watcher)


@socketio.on('disconnect')
def handle_disconnect(*args):
    sid = request.sid
    sid_to_user.pop(sid, None)
    before = len(waiting_pool)
    waiting_pool[:] = [p for p in waiting_pool if p['sid'] != sid]
    if len(waiting_pool) != before:
        notify_waiting_status()
    room_id = sid_to_room.pop(sid, None)
    if not room_id or room_id not in rooms:
        return
    room = rooms[room_id]
    if room.get('judging'):
        return
    leaver = next((p for p in room['players'] if p['sid'] == sid), None)
    stayer = next((p for p in room['players'] if p['sid'] != sid), None)
    if not leaver or not stayer:
        return
    # 토론은 끝내지 않는다. 시계를 멈추고 돌아올 때까지 기다린다.
    leaver['connected'] = False
    leaver['disconnect_at'] = time.time()
    pause_clock(room)
    socketio.emit('opponent_disconnected', {}, room=stayer['sid'])
    socketio.emit('admin_stage', {'room_id': room_id, 'stage': f"⚠️ {leaver['username']} 연결 끊김 — 시계 정지"},
                  room=f"watch_{room_id}")


@socketio.on('leave_queue')
def handle_leave_queue(data=None):
    waiting_pool[:] = [p for p in waiting_pool if p['sid'] != request.sid]
    notify_waiting_status()


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
