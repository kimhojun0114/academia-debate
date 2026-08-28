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

RECONNECT_GRACE_SECONDS = 60   # 이 시간 안에 돌아오면 몰수패로 치지 않는다

STAGES = ["입론", "반론", "반박", "최종변론"]
TOTAL_STAGES = len(STAGES)
TURN_SECONDS = 300
MIN_MESSAGE_LENGTH = 30        # 이보다 짧은 발언은 반려한다
TIMEOUT_MESSAGE = "(시간 초과)"  # 시간 초과 자동 전송은 길이 검사에서 예외

JUDGE_CRITERIA = [
    ("입론", "논지의 명확성과 근거의 타당성"),
    ("반론", "상대 논리의 허점을 정확히 짚어내는 능력"),
    ("반박", "반론에 대한 재반박의 논리성"),
    ("최종변론", "전체 논지 정리와 설득력"),
]

ADMIN_CODE = os.environ.get("DEBATE_ADMIN_CODE", "1234")
TEST_MODE = "테스트 모드"

# 기본 Gemini 키 (환경변수 GEMINI_API_KEY가 있으면 그쪽이 우선)
DEFAULT_GEMINI_KEY = "AQ.Ab8RN6LuDcj_jU7-uZHXTq8uQELxQPoh6mrDkzldT3RfG5pPYA"

PROVIDERS = ["gemini", "openai", "anthropic"]
DEFAULT_MODELS = {
    "gemini": "gemini-3.7-flash",
    "openai": "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5",
}

# 지원 종료된 모델들. Google이 "모델 없음" 대신 401 인증 오류를 반환해
# 키 문제처럼 보이게 만들기 때문에, 서버 시작 시 자동으로 최신 모델로 교체한다.
DEPRECATED_MODELS = {
    "gemini": ["gemini-2.5-flash", "gemini-2.0-flash", "gemini-1.5-flash",
               "gemini-2.5-pro", "gemini-1.5-pro"],
}

# 과부하(503)로 막힐 때 순서대로 넘어가며 시도할 모델
GEMINI_FALLBACK_MODELS = ["gemini-3.7-flash", "gemini-3.6-flash", "gemini-3.5-flash"]

# AI가 주제를 못 만들 때 쓸 예비 주제. 예전처럼 한 주제만 반복되지 않도록 여러 개를 둔다.
BACKUP_TOPICS = [
    "학교는 학생의 스마트폰 사용을 제한해야 하는가?",
    "대학 입시에서 정시 비중을 늘려야 하는가?",
    "인공지능이 만든 창작물에 저작권을 인정해야 하는가?",
    "청소년의 SNS 이용에 연령 제한을 두어야 하는가?",
    "학교 급식은 채식 선택권을 보장해야 하는가?",
    "교내 CCTV 확대는 정당한가?",
    "선거 연령을 더 낮춰야 하는가?",
    "학생부 기재에서 봉사활동을 제외해야 하는가?",
    "온라인 수업은 대면 수업을 대체할 수 있는가?",
    "기업의 주 4일제 도입은 확대되어야 하는가?",
    "동물 실험은 전면 금지되어야 하는가?",
    "자율주행차 사고의 책임은 제조사에 있는가?",
    "공공장소 얼굴 인식 기술 사용을 허용해야 하는가?",
    "학교 시험에서 절대평가로 전환해야 하는가?",
    "유명인의 사생활 보도는 어디까지 허용되는가?",
]

ALIAS_POOL = [
    "대한민국의 첫 번째 왕좌를 차지한 자", "왕좌를 놓지 않는 건국의 노인",
    "왕좌에서 내려오기를 끝까지 거부한 노회한 독재자", "한강의 다리를 건너간 도망의 성자",
    "한강의 기적을 설계한 철권의 통치자", "산업화를 명령한 철의 군인",
    "유신의 밤을 밝힌 철의 군주", "반신의 왕좌에 앉은 재규어의 그림자",
    "긴 세월을 다스린 강철의 아버지", "강철 전차를 몰고 온 새벽의 장군",
    "군홧발로 권력을 장악한 철의 사내", "헌법보다 총을 믿었던 자",
    "광주의 밤을 가로지른 불의 성좌", "친구 따라 청와대까지 걸어간 자",
    "친구와 함께 쿠데타하고 민주화를 선언한 자", "세 당을 하나로 묶은 중재의 그림자",
    "문민의 깃발을 휘두른 거인", "외환의 폭풍을 맞은 항해자",
    "햇볕을 내려준 호남의 대부", "남북의 다리를 놓은 DJ의 성좌",
    "권위주의의 벽을 넘어선 시민의 대통령", "반미의 깃발을 들고 부시의 전쟁에 병사를 보낸 자",
    "이라크의 밤하늘에 한국군의 그림자를 남긴 자", "거인의 어깨 위에서 뛰어내린 청계천의 주인",
    "한반도의 4대 강을 지배하는 자", "청와대로 돌아온 공주",
    "수첩에 적은 운명을 따른 공주", "촛불의 파도 위에 선 자",
    "탈원전의 밤을 선포한 녹색의 군주", "빨간 버튼을 어루만진 검사의 성좌",
    "여섯 시간의 겨울을 선포한 윤의 별", "대출의 문을 잠근 재이의 성좌",
    "형수의 목소리를 남긴 찢어진 별",
]


# ─────────────────────────────────────────────
# DB 기본 유틸
# ─────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def db_execute(query, params=()):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    cursor.close()
    conn.close()

def db_fetchall(query, params=()):
    conn = get_conn()
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return rows

def db_fetchone(query, params=()):
    rows = db_fetchall(query, params)
    return rows[0] if rows else None


def init_db():
    db_execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, pin_hash TEXT,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, points INTEGER DEFAULT 1000)''')
    db_execute('''CREATE TABLE IF NOT EXISTS debates (
        id SERIAL PRIMARY KEY, played_at TEXT, topic TEXT,
        player_a TEXT, player_b TEXT, side_a TEXT, side_b TEXT,
        log_json TEXT, winner TEXT, reason TEXT, engine TEXT)''')
    db_execute("ALTER TABLE debates ADD COLUMN IF NOT EXISTS side_a TEXT")
    db_execute("ALTER TABLE debates ADD COLUMN IF NOT EXISTS side_b TEXT")
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
def get_setting(key, default=None):
    row = db_fetchone("SELECT value FROM app_settings WHERE key = %s", (key,))
    return row[0] if row else default

def set_setting(key, value):
    db_execute("INSERT INTO app_settings (key, value) VALUES (%s, %s) "
               "ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", (key, value))

def list_profiles():
    rows = db_fetchall("SELECT name, provider, model, api_key FROM ai_profiles ORDER BY name")
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
    """Gemini 프로필 한 개를 보장. 키가 비어 있으면 기본 키로 채움.
    환경변수 GEMINI_API_KEY가 있으면 그 값을 우선 사용."""
    seed_key = os.environ.get("GEMINI_API_KEY", "").strip() or DEFAULT_GEMINI_KEY
    row = db_fetchone("SELECT api_key FROM ai_profiles WHERE name = %s", ("Gemini",))
    if not row:
        upsert_profile("Gemini", "gemini", DEFAULT_MODELS["gemini"], seed_key)
    elif not (row[0] or "").strip():
        # 프로필은 있는데 키가 비어 있으면 채워줌 (관리자가 넣은 키는 건드리지 않음)
        db_execute("UPDATE ai_profiles SET api_key = %s WHERE name = %s", (seed_key, "Gemini"))
    if not get_setting("active_profile"):
        set_setting("active_profile", "Gemini")
    migrate_deprecated_models()


def migrate_deprecated_models():
    """지원 종료된 모델을 쓰는 프로필을 최신 기본 모델로 자동 교체."""
    for provider, old_models in DEPRECATED_MODELS.items():
        new_model = DEFAULT_MODELS[provider]
        for old in old_models:
            rows = db_fetchall("SELECT name FROM ai_profiles WHERE provider = %s AND model = %s",
                               (provider, old))
            for (name,) in rows:
                db_execute("UPDATE ai_profiles SET model = %s WHERE name = %s", (new_model, name))
                print(f"[모델 자동 교체] {name}: {old} → {new_model}")

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
    try:
        db_execute("INSERT INTO ai_calls (called_at, kind, ok, engine, model, error, ms) "
                   "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                   (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), kind, ok,
                    get_active_name(), (profile or {}).get("model", "-"),
                    (error or "")[:400], int(ms)))
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
    # 한도 초과가 최근에 있었는지
    quota_hit = any(("429" in (r["error"] or "")) or ("RESOURCE_EXHAUSTED" in (r["error"] or ""))
                    for r in recent_view)
    return stats, recent_view, quota_hit


def _is_auth_error(e):
    s = str(e)
    return "401" in s or "UNAUTHENTICATED" in s or "ACCESS_TOKEN_TYPE_UNSUPPORTED" in s

def _is_quota_error(e):
    """무료 한도 초과(429). 한도는 계정 단위라 모델을 바꿔도 소용없다."""
    s = str(e)
    return "429" in s or "RESOURCE_EXHAUSTED" in s or "exceeded your current quota" in s

def _is_busy_error(e):
    """모델 과부하(503). 잠시 후 재시도하거나 다른 모델로 넘기면 풀린다."""
    if _is_quota_error(e):
        return False
    s = str(e)
    return "503" in s or "UNAVAILABLE" in s or "overloaded" in s.lower()

def _retry_after_seconds(e, default=30):
    """'Please retry in 30.15s' 같은 안내에서 대기 시간을 뽑아낸다."""
    import re as _re
    m = _re.search(r"retry in ([0-9.]+)s", str(e))
    if m:
        try:
            return min(float(m.group(1)) + 1, 90)
        except ValueError:
            pass
    return default


# ── 분당 호출량 제한 ───────────────────────────────────────
# 무료 한도(분당 20회)에 닿기 전에 우리가 먼저 조절한다.
from collections import deque
_call_times = deque()

def current_rpm():
    now = time.time()
    while _call_times and now - _call_times[0] > 60:
        _call_times.popleft()
    return len(_call_times)

def max_rpm():
    try:
        return int(get_setting("ai_max_rpm", "15") or 15)
    except (TypeError, ValueError):
        return 15

def rate_gate(priority="high", max_wait=70):
    """
    호출 허가. 낮은 우선순위(요약)는 붐비면 즉시 포기한다.
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
        import time
        time.sleep(seconds)


def _gemini_once(api_key, model, prompt):
    """API 버전을 순차 시도. 인증 오류일 때만 다음 버전으로 넘어간다."""
    from google import genai
    from google.genai import types as genai_types
    last_err = None
    for api_version in (None, "v1", "v1alpha"):
        kwargs = {"api_key": api_key}
        if api_version:
            kwargs["http_options"] = genai_types.HttpOptions(api_version=api_version)
        try:
            client = genai.Client(**kwargs)
            return client.models.generate_content(model=model, contents=prompt).text.strip()
        except Exception as e:
            last_err = e
            if not _is_auth_error(e):
                raise
    raise last_err


def _gemini_call(api_key, model, prompt):
    """
    과부하(503)일 때만 재시도·모델 전환을 한다.
    한도 초과(429)는 즉시 포기한다 — 무료 한도는 계정 전체가 공유하므로
    모델을 바꿔가며 재시도하면 남은 한도만 더 빨리 태운다.
    """
    candidates = [model] + [m for m in GEMINI_FALLBACK_MODELS if m != model]
    last_err = None
    first = True
    for idx, candidate in enumerate(candidates):
        for attempt in range(2):
            if not first:
                _call_times.append(time.time())   # 재시도도 한도에 포함시킨다
            first = False
            try:
                return _gemini_once(api_key, candidate, prompt)
            except Exception as e:
                last_err = e
                if _is_quota_error(e):
                    raise QuotaExceeded(str(e), _retry_after_seconds(e))
                if _is_busy_error(e) and attempt == 0:
                    print(f"[Gemini 과부하] {candidate} 1회 재시도")
                    _pause(1.5)
                    continue
                break
        if not _is_busy_error(last_err):
            break   # 과부하가 아니면 다른 모델을 시도해봐야 의미 없다
    raise last_err


class QuotaExceeded(Exception):
    """무료 한도 초과. 재시도나 모델 전환으로 풀리지 않는다."""
    def __init__(self, message, retry_after=0):
        super().__init__(message)
        self.retry_after = retry_after


def call_one_profile(profile, prompt, max_tokens):
    """프로필 하나로 실제 호출."""
    provider = profile["provider"]
    if provider == "openai":
        from openai import OpenAI
        c = OpenAI(api_key=profile["api_key"])
        r = c.chat.completions.create(model=profile["model"],
                                      messages=[{"role": "user", "content": prompt}])
        return r.choices[0].message.content.strip()
    elif provider == "anthropic":
        from anthropic import Anthropic
        c = Anthropic(api_key=profile["api_key"])
        r = c.messages.create(model=profile["model"], max_tokens=max_tokens,
                              messages=[{"role": "user", "content": prompt}])
        return r.content[0].text.strip()
    elif provider == "gemini":
        return _gemini_call(profile["api_key"], profile["model"], prompt)
    raise RuntimeError(f"지원하지 않는 제공사: {provider}")


def backup_profiles():
    """활성 프로필이 한도에 걸렸을 때 대신 쓸 프로필들 (키가 등록된 것만)."""
    active = get_active_name()
    return [p for p in list_profiles()
            if p["name"] != active and (p.get("api_key") or "").strip()]


def llm_call(prompt, max_tokens=600, raise_errors=False, priority="high"):
    """
    priority='high'  — 심사·주제. 자리가 날 때까지 기다린다.
    priority='low'   — 발언 요약. 한도가 빠듯하면 건너뛴다.
    """
    profile = get_active_profile()
    if not profile:
        if raise_errors:
            raise RuntimeError("활성 프로필이 없거나 API 키가 비어 있습니다.")
        return None

    if not rate_gate(priority):
        msg = "분당 호출 한도에 근접해 이번 요약은 건너뜁니다."
        if raise_errors:
            raise QuotaExceeded(msg)
        return None

    try:
        return call_one_profile(profile, prompt, max_tokens)
    except QuotaExceeded as quota_err:
        # 한도 초과 — 같은 키로 재시도해봐야 소용없다. 다른 키가 있으면 그쪽으로.
        for backup in backup_profiles():
            if not rate_gate("high"):
                break
            try:
                result = call_one_profile(backup, prompt, max_tokens)
                print(f"[키 전환] {profile['name']} 한도 초과 → {backup['name']}로 처리")
                return result
            except QuotaExceeded:
                continue
            except Exception:
                continue
        print(f"[LLM 한도 초과] 사용 가능한 키가 없습니다: {quota_err}")
        if raise_errors:
            raise
        return None
    except ImportError as e:
        print(f"[LLM 오류] {profile['provider']} SDK 미설치: {e}")
        if raise_errors:
            raise
        return None
    except Exception as e:
        print(f"[LLM 호출 실패] {profile['provider']}: {e}")
        if raise_errors:
            raise
        return None


def _sdk_version(provider):
    pkg = {"gemini": "google-genai", "openai": "openai", "anthropic": "anthropic"}.get(provider)
    if not pkg:
        return "?"
    try:
        import importlib.metadata as md
        return md.version(pkg)
    except Exception:
        return "미설치"


def test_llm_connection():
    """관리자 페이지의 '연결 테스트' 버튼용. 실패 원인을 화면에 그대로 표시."""
    profile = get_active_profile()
    if not profile:
        return False, "활성 프로필이 없거나 API 키가 비어 있습니다."
    info = (f"프로필 {profile['name']} | {profile['provider']} / {profile['model']} | "
            f"SDK {_sdk_version(profile['provider'])} | 키 {mask_key(profile['api_key'])}")
    try:
        result = llm_call("'연결 성공'이라고만 답해.", max_tokens=50, raise_errors=True)
        if result:
            return True, f"{info}\n응답: {result[:80]}"
        return False, f"{info}\n예외 없이 빈 응답이 돌아왔습니다."
    except Exception as e:
        return False, f"{info}\n{type(e).__name__}: {str(e)[:500]}"


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
    db_execute("UPDATE users SET wins = wins + 1, points = points + 20 WHERE username = %s", (winner,))
    db_execute("UPDATE users SET losses = losses + 1, points = points - 15 WHERE username = %s", (loser,))

def save_debate(topic, player_a, player_b, side_a, side_b, logs, winner, reason):
    engine = get_active_name()
    db_execute(
        "INSERT INTO debates (played_at, topic, player_a, player_b, side_a, side_b, log_json, winner, reason, engine) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (datetime.now().strftime("%Y-%m-%d %H:%M"), topic, player_a, player_b, side_a, side_b,
         json.dumps(logs, ensure_ascii=False), winner, reason, engine))


init_db()
seed_default_profile()
# 서버가 뜨자마자 주제를 미리 만들어둔다 (첫 매칭부터 AI 주제가 나오도록)
socketio.start_background_task(lambda: refill_topic_cache())


# ─────────────────────────────────────────────
# AI 기능
# ─────────────────────────────────────────────
def get_ai_topic(allow_fallback=True, priority="high"):
    recent_rows = db_fetchall("SELECT topic FROM debates ORDER BY id DESC LIMIT 30")
    # 이미 창고에 쌓아둔 주제도 제외 대상에 넣어야 7개가 서로 다르게 만들어진다
    recent_topics = list(dict.fromkeys(list(topic_cache) + [r[0] for r in recent_rows]))[:20]
    recent_list = "\n".join(f"- {t}" for t in recent_topics) if recent_topics else "(없음)"
    prompt = f"""고등학생 토론 동아리의 1대1 즉흥 찬반 토론에 쓸 주제를 하나 추천해줘.

조건:
- 찬성/반대가 명확히 갈리는 주제일 것
- 사형제, 인공지능의 위험성처럼 너무 뻔하고 진부한 주제는 피하고, 시사성 있거나 참신한 주제로
- 고등학생 수준에서 전문 지식 없이도 논리적으로 다룰 수 있을 것
- 아래 최근 사용된 주제와 겹치지 않을 것

[최근 사용된 주제]
{recent_list}

다른 설명 없이 주제 제목 한 줄만 출력해."""
    started = time.time()
    try:
        result = llm_call(prompt, raise_errors=True, priority=priority)
        log_ai_call("주제추천", True, "", (time.time() - started) * 1000)
        return result.strip().strip('"').strip("'").splitlines()[0]
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

def take_topic():
    """즉시 반환. 쌓인 게 없으면 예비 주제를 쓴다."""
    topic = topic_cache.pop(0) if topic_cache else random.choice(BACKUP_TOPICS)
    socketio.start_background_task(refill_topic_cache)
    return topic

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
            if topic in topic_cache:
                misses += 1
                if misses >= 2:
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

def judge_debate(topic, logs):
    log_text = "\n".join([f"[{log['stage']}] {log['side']}: {log['text']}" for log in logs])
    prompt = f"""너는 고등학교 토론 동아리의 심사위원이다. 아래 1대1 찬반 토론을 단계별 심사 기준에 따라 엄격히 평가하라.

[심사 기준 — 단계당 5점 만점, 총 20점]
1. 입론: 논지의 명확성과 근거의 타당성
2. 반론: 상대 논리의 허점을 정확히 짚어내는 능력
3. 반박: 반론에 대한 재반박의 논리성
4. 최종변론: 전체 논지 정리와 설득력

[주제] {topic}
[토론 로그 — 단계 순서대로]
{log_text}

반드시 아래 JSON 형식으로만 답하라.
{{"winner": "찬성" 또는 "반대" 또는 "무승부", "score_pro": 총점숫자, "score_con": 총점숫자, "reason": "단계별 근거를 포함해 4문장 이내로"}}"""
    # 심사는 토론의 결론이므로 조용히 무승부로 넘기지 않고 여러 번 시도한다
    last_error = ""
    for attempt in range(3):
        started = time.time()
        try:
            raw = llm_call(prompt, max_tokens=900, raise_errors=True)
            data = json.loads(raw.replace("```json", "").replace("```", "").strip())
            winner = data.get("winner", "무승부")
            if winner not in ("찬성", "반대", "무승부"):
                raise ValueError(f"알 수 없는 판정값: {winner}")
            log_ai_call("심사", True, "", (time.time() - started) * 1000)
            verdict_line = f"판정: {winner}측 승리" if winner != "무승부" else "판정: 무승부"
            result_text = (verdict_line + "\n" +
                           f"점수: 찬성 {data.get('score_pro','?')}점 vs 반대 {data.get('score_con','?')}점 (20점 만점)\n" +
                           f"근거: {data.get('reason','근거 없음')}")
            return winner, result_text
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"
            log_ai_call("심사", False, last_error, (time.time() - started) * 1000)
            if attempt < 2:
                # 한도 초과면 서버가 알려준 시간만큼 기다린다 (보통 30초 안팎)
                wait = _retry_after_seconds(e, 0) if _is_quota_error(e) else 2 * (attempt + 1)
                if wait:
                    print(f"[심사 재시도] {wait:.0f}초 대기")
                _pause(wait or 2 * (attempt + 1))

    # 세 번 다 실패 — 무승부로 위장하지 않고 심사 실패로 남긴다 (전적에 반영하지 않음)
    return "심사실패", ("⚠️ AI 심사에 실패했습니다.\n"
                        "승패는 기록되지 않았으며, 운영자가 확인 후 다시 심사할 수 있습니다.\n"
                        f"원인: {last_error[:200]}")


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>동아리 익명 1대1 토론 리그</title>
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<style>
body{font-family:'Malgun Gothic',sans-serif;max-width:600px;margin:30px auto;padding:20px;background:#f3f4f6;}
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
</style>
</head>
<body>
<h2>🗣️ 동아리 익명 1대1 토론 시스템</h2>
<p class="engine-tag">🤖 현재 AI 엔진: {{ engine_name }}</p>
<div id="login-area" class="box">
  <p style="font-size:0.9em;color:#555;">운영자가 발급한 아이디로 로그인하세요.<br>🎭 토론 중에는 익명 이름만 표시됩니다.</p>
  <input type="text" id="username" placeholder="아이디" style="padding:8px;width:40%;">
  <input type="password" id="pin" placeholder="PIN" style="padding:8px;width:35%;">
  <button onclick="login()" style="padding:8px 15px;cursor:pointer;">로그인</button>
  <p id="status-text" style="color:#2563eb;font-weight:bold;margin-top:10px;"></p>
  <p class="nav"><a href="/ranking">🏆 랭킹 보기</a> · <a href="/history">📚 토론 기록</a></p>
</div>
<div id="profile-area" class="box hidden">
  <p>👤 <b id="my-name"></b>님, 환영합니다!</p>
  <p class="stats" id="my-stats"></p>
  <p style="font-size:0.9em;color:#555;">✅ 끝까지 완주하면 승패와 관계없이 <b>+5점</b>!</p>
  <button onclick="joinQueue()" id="match-btn" style="padding:10px 20px;cursor:pointer;background:#2563eb;color:white;border:none;border-radius:6px;font-weight:bold;">토론 매칭 시작</button>
  <p id="queue-text" style="color:#2563eb;font-weight:bold;margin-top:10px;"></p>
  <p class="nav"><a href="/ranking">🏆 랭킹 보기</a> · <a href="/history">📚 토론 기록</a></p>
</div>
<div id="debate-area" class="box hidden">
  <div id="stage-dots"></div>
  <div id="stage-banner">단계 준비 중...</div>
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
    <p class="warn">항복하면 <b>패배로 기록</b>되고 토론이 즉시 끝납니다.</p>
    <button id="surrender-btn" onclick="surrender()">🏳️ 항복하기</button>
  </details>
</div>
<script>
document.addEventListener('contextmenu',e=>{e.preventDefault();alert('우클릭 금지');});
document.addEventListener('copy',e=>e.preventDefault());
document.addEventListener('cut',e=>e.preventDefault());
document.addEventListener('paste',e=>{e.preventDefault();alert('⚠️ 붙여넣기 금지!');});
document.addEventListener('drop',e=>e.preventDefault());
document.addEventListener('dragover',e=>e.preventDefault());
document.addEventListener('beforeinput',e=>{
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
  if(isCtrl&&['c','v','x','a'].includes(key)){e.preventDefault();alert('⚠️ 단축키 금지!');}
});
const socket=io({reconnection:true,reconnectionAttempts:Infinity,
  reconnectionDelay:800,reconnectionDelayMax:4000,timeout:20000});
let currentRoom="",myTurn=false,totalStages=4,resumeToken="",oppGone=false;
const TIMER_SECONDS={{ turn_seconds }};
let timerInterval=null,remaining=TIMER_SECONDS,minLength={{ min_length }};
let speakerAlias="",speakerSide="";
// 타이머는 내 차례든 상대 차례든 항상 돈다. 시간이 다 되면 발언자만 자동 전송한다.
function startTimer(seconds){clearInterval(timerInterval);
  remaining=(typeof seconds==='number'&&seconds>=0)?seconds:TIMER_SECONDS;updateTimerText();
  timerInterval=setInterval(()=>{remaining--;updateTimerText();
    if(remaining<=0){clearInterval(timerInterval);if(myTurn)autoSend();}},1000);}
function stopTimer(){clearInterval(timerInterval);}
function clockText(){const r=Math.max(0,remaining),m=Math.floor(r/60),s=String(r%60).padStart(2,'0');
  return m+":"+s;}
function updateTimerText(){const el=document.getElementById('turn-status');
  if(oppGone){return;}
  if(myTurn){el.classList.remove('waiting');
    el.innerText="⏰ 당신의 발언 차례입니다! (남은 시간 "+clockText()+")";}
  else{el.classList.add('waiting');
    el.innerText="⏳ 상대방 발언 중 — 남은 시간 "+clockText()+" · 미리 작성해두세요";}}
function autoSend(){const i=document.getElementById('msg-input');
  if(i.value.trim().length<minLength)i.value="(시간 초과)";sendMessage(true);}
function renderCriteria(list){
  if(!list||!list.length)return;
  const box=document.getElementById('criteria-box');
  box.innerHTML='<b>📋 AI 심사 기준</b> — 단계당 5점, 총 20점<br>'+
    list.map(c=>"<span class='ct'>"+c.stage+"</span> "+c.desc).join('<br>');}
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
socket.on('login_ok',d=>{
  resumeToken=d.resume_token||"";
  document.getElementById('login-area').classList.add('hidden');
  document.getElementById('profile-area').classList.remove('hidden');
  document.getElementById('my-name').textContent=d.username;
  document.getElementById('my-stats').textContent="내 전적: "+d.wins+"승 "+d.losses+"패 | 점수: "+d.points+"점 | 현재 "+d.rank+"위";});

function setNetBanner(text,color){
  let el=document.getElementById('net-banner');
  if(!el){el=document.createElement('div');el.id='net-banner';
    el.style.cssText='position:fixed;top:0;left:0;right:0;padding:8px;text-align:center;'+
      'font-weight:bold;color:#fff;z-index:999;font-size:0.9em;';
    document.body.appendChild(el);}
  if(!text){el.remove();return;}
  el.textContent=text;el.style.background=color;}

socket.on('disconnect',()=>{stopTimer();
  setNetBanner('⚠️ 연결이 끊겼습니다. 다시 연결하는 중...','#dc2626');});
socket.on('connect',()=>{
  if(resumeToken&&currentRoom){socket.emit('resume',{token:resumeToken});}
  else{setNetBanner('','');}});
socket.on('resumed',d=>{
  setNetBanner('✅ 다시 연결되었습니다','#16a34a');
  setTimeout(()=>setNetBanner('',''),2500);
  currentRoom=d.room_id;totalStages=d.total_stages;myTurn=d.your_turn;
  document.getElementById('login-area').classList.add('hidden');
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  document.getElementById('role-area').innerHTML="🎭 내 이름: ["+d.my_alias+"] "+sideTag(d.my_side)+"  vs  상대: ["+d.opp_alias+"] "+sideTag(d.opp_side);
  if(d.min_length)minLength=d.min_length;
  renderCriteria(d.criteria);
  const cb=document.getElementById('chat-box');cb.innerHTML='';
  d.logs.forEach(l=>appendMessage(cb,l,false));
  cb.scrollTop=cb.scrollHeight;
  showStage(d.stage,d.stage_index,d.total_stages,false);
  oppGone=false;updateTurnUI(d.seconds_left);});
socket.on('resume_failed',()=>{setNetBanner('','');
  if(currentRoom){alert('토론이 이미 종료되었습니다.');location.reload();}});
socket.on('opponent_disconnected',d=>{oppGone=true;stopTimer();
  setNetBanner('⏳ 상대방 연결이 끊겼습니다. '+d.seconds+'초 안에 돌아오지 않으면 몰수승 처리됩니다.','#d97706');});
socket.on('opponent_reconnected',()=>{oppGone=false;
  setNetBanner('✅ 상대방이 돌아왔습니다','#16a34a');
  setTimeout(()=>setNetBanner('',''),2500);
  updateTurnUI(remaining);});
socket.on('error_msg',d=>alert(d.msg));
function joinQueue(){socket.emit('join_queue',{});document.getElementById('match-btn').disabled=true;}
socket.on('status',d=>{document.getElementById('queue-text').innerText=d.msg;});
// 매칭이 실패해 대기열로 돌아갔을 때 자동으로 다시 시도
socket.on('rejoin_queue',()=>{setTimeout(()=>{
  if(!currentRoom)socket.emit('join_queue',{});},3000);});
// 매칭 버튼이 눌린 채 잠기지 않도록, 방이 안 생겼으면 다시 누를 수 있게 풀어준다
socket.on('error_msg',()=>{if(!currentRoom){
  const b=document.getElementById('match-btn');if(b)b.disabled=false;}});
function renderStageDots(idx,total){
  const wrap=document.getElementById('stage-dots');wrap.innerHTML='';
  for(let i=0;i<total;i++){const dot=document.createElement('div');
    dot.className='dot'+(i<idx?' done':i===idx?' current':'');wrap.appendChild(dot);}}
function showStage(stageName,idx,total,animate){
  const banner=document.getElementById('stage-banner');
  banner.textContent='📍 현재 단계: '+stageName+' ('+(idx+1)+'/'+total+')';
  renderStageDots(idx,total);
  if(animate){banner.classList.remove('stage-pulse');void banner.offsetWidth;banner.classList.add('stage-pulse');}}
function sideTag(side){return "<span class='side-tag "+(side==='찬성'?'side-pro':'side-con')+"'>"+side+"</span>";}
socket.on('match_found',d=>{currentRoom=d.room_id;myTurn=d.your_turn;totalStages=d.total_stages;
  if(d.min_length)minLength=d.min_length;
  speakerAlias=d.speaker_alias;speakerSide=d.speaker_side;
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  document.getElementById('role-area').innerHTML="🎭 내 이름: ["+d.my_alias+"] "+sideTag(d.my_side)+"  vs  상대: ["+d.opp_alias+"] "+sideTag(d.opp_side);
  renderCriteria(d.criteria);updateLenHint();
  showStage(d.stage,d.stage_index,d.total_stages,true);
  updateTurnUI(d.seconds_left);});
socket.on('turn_change',d=>{myTurn=d.your_turn;
  speakerAlias=d.speaker_alias;speakerSide=d.speaker_side;
  showStage(d.stage,d.stage_index,totalStages,d.stage_changed);
  updateTurnUI(d.seconds_left);});
function updateTurnUI(seconds){const area=document.getElementById('debate-area'),
  input=document.getElementById('msg-input'),btn=document.getElementById('send-btn'),
  sur=document.getElementById('surrender-btn');
  // 상대 차례에도 입력창은 열어둔다 — 전송만 막는다
  input.disabled=false;
  btn.disabled=!myTurn||oppGone;
  if(sur)sur.disabled=oppGone;
  if(myTurn){area.classList.add('turn-active');input.focus();}
  else{area.classList.remove('turn-active');}
  if(!oppGone)startTimer(typeof seconds==='number'?seconds:remaining);
  updateLenHint();}
function sendMessage(isAuto){const el=document.getElementById('msg-input'),msg=el.value.trim();
  if(!myTurn)return alert('지금은 당신의 발언 차례가 아닙니다.\\n작성해둔 내용은 그대로 유지됩니다.');
  if(!msg)return alert('내용을 입력해주세요.');
  if(!isAuto&&msg.length<minLength){
    alert('⚠️ 발언이 너무 짧습니다.\\n\\n현재 '+msg.length+'자 / 최소 '+minLength+'자\\n\\n논거를 더 채워서 다시 시도해주세요.');
    el.focus();return;}
  stopTimer();
  socket.emit('send_message',{room_id:currentRoom,message:msg});
  el.value="";updateLenHint();}
socket.on('message_rejected',d=>{
  alert('⚠️ '+d.msg+'\\n\\n작성하신 내용은 입력창에 그대로 복원했습니다.');
  const el=document.getElementById('msg-input');
  if(!el.value.trim())el.value=d.text||"";
  updateLenHint();el.focus();
  if(myTurn)startTimer(remaining);});
document.addEventListener('input',e=>{if(e.target.id==='msg-input')updateLenHint();});
socket.on('surrendered',d=>{stopTimer();
  alert("🏳️ 항복했습니다.\\n\\n"+d.headline+"\\n\\n"+d.reveal);location.reload();});
function sideClass(side){return side==='찬성'?'pro':(side==='반대'?'con':'');}
function appendMessage(cb,d,withPending){
  const md=document.createElement('div');md.className='msg-item '+sideClass(d.side);
  const ne=document.createElement('strong');
  ne.innerHTML="<span class='stage-label'>["+d.stage+"]</span> "+sideTag(d.side)+" "+d.sender+': ';
  const te=document.createElement('span');te.textContent=d.message;
  md.appendChild(ne);md.appendChild(te);cb.appendChild(md);
  if(withPending){const sd=document.createElement('div');
    sd.className='summary-item pending '+sideClass(d.side);
    sd.id='sum-'+d.msg_id;sd.textContent='🤖 AI 요약 생성 중...';cb.appendChild(sd);}}
socket.on('receive_message',d=>{const cb=document.getElementById('chat-box');
  appendMessage(cb,d,true);cb.scrollTop=cb.scrollHeight;});
socket.on('receive_summary',d=>{const sd=document.getElementById('sum-'+d.msg_id);
  if(!sd)return;sd.classList.remove('pending');
  if(d.summary){sd.textContent='🤖 AI 요약: '+d.summary;}
  else{sd.remove();}   // 요약 실패 시 '생성 중'을 남겨두지 않는다
  const cb=document.getElementById('chat-box');cb.scrollTop=cb.scrollHeight;});
socket.on('judging',d=>{stopTimer();
  document.getElementById('turn-status').innerText="⚖️ "+d.msg;
  document.getElementById('msg-input').disabled=true;
  document.getElementById('send-btn').disabled=true;
  const cb=document.getElementById('chat-box');
  const el=document.createElement('div');el.className='summary-item pending';
  el.textContent='⚖️ '+d.msg;cb.appendChild(el);cb.scrollTop=cb.scrollHeight;});
socket.on('opponent_left',d=>{stopTimer();
  alert("🚪 몰수승!\\n\\n"+(d.headline||"")+"\\n\\n"+d.reveal);location.reload();});
socket.on('debate_end',d=>{stopTimer();alert("🔔 토론 완료! (+5점)\\n\\n[AI 판정]\\n"+d.result);location.reload();});
</script>
</body></html>"""

ADMIN_TEMPLATE = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>운영자 설정</title>
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<style>body{font-family:'Malgun Gothic',sans-serif;max-width:700px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:20px;background:white;margin-bottom:15px;border-radius:12px;}
.profile{padding:10px;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:8px;}
.active{border:2px solid #2563eb;background:#eff6ff;}.ok{color:#16a34a}.bad{color:#dc2626}
.msg{color:#1e3a8a;font-weight:bold;white-space:pre-line;background:#eff6ff;
  border:1px solid #bfdbfe;border-radius:8px;padding:12px;font-size:0.92em;
  word-break:break-word;overflow-wrap:anywhere;}
table{width:100%;border-collapse:collapse;margin-bottom:14px;font-size:0.9em;}
th{text-align:left;border-bottom:1px solid #e5e7eb;padding:4px;}
td{border-bottom:1px solid #f3f4f6;padding:4px;}
.room-btn{display:block;width:100%;text-align:left;padding:12px;margin-bottom:8px;cursor:pointer;border:1px solid #e5e7eb;border-radius:8px;background:#f9fafb;line-height:1.5;}
.room-btn:hover{background:#eef2ff;border-color:#c7d2fe;}
.side-tag{display:inline-block;padding:1px 7px;border-radius:6px;font-size:0.8em;font-weight:bold;}
.side-pro{background:#dcfce7;color:#166534;} .side-con{background:#fee2e2;color:#991b1b;}
#watch-chat{height:300px;overflow-y:auto;border:1px solid #e5e7eb;padding:10px;background:#f9fafb;border-radius:8px;margin-top:10px;font-size:0.9em;}
.hidden{display:none;}
input[type=text],input[type=password]{padding:8px;border:1px solid #d1d5db;border-radius:6px;
  font-size:16px;background:#fff;color:#111827;-webkit-appearance:none;appearance:none;
  max-width:100%;box-sizing:border-box;}
select{padding:8px 28px 8px 8px;border:1px solid #d1d5db;border-radius:6px;
  font-size:16px;background:#fff;color:#111827;max-width:100%;
  -webkit-appearance:none;appearance:none;
  background-image:url("data:image/svg+xml;utf8,<svg xmlns='http://www.w3.org/2000/svg' width='12' height='8'><path d='M1 1l5 5 5-5' stroke='%23374151' stroke-width='2' fill='none'/></svg>");
  background-repeat:no-repeat;background-position:right 9px center;}
button,input[type=submit]{-webkit-appearance:none;appearance:none;
  border:1px solid #93c5fd;border-radius:6px;background:#2563eb;color:#ffffff !important;
  cursor:pointer;font-size:15px;font-weight:bold;padding:10px 16px;margin:4px 4px 4px 0;
  font-family:inherit;-webkit-text-fill-color:#ffffff;}
button:hover{background:#1d4ed8;}
button:active{background:#1e40af;}
.hint{font-size:0.82em;color:#6b7280;}
fieldset{border:1px solid #e5e7eb;border-radius:8px;margin-bottom:12px;padding:12px;}
legend{font-weight:bold;font-size:0.92em;color:#374151;padding:0 6px;}
</style>
</head><body><h2>🛠️ 운영자 설정</h2>
{% if message %}<p class="msg">{{ message }}</p>{% endif %}

<div class="box"><h3>🤖 AI 엔진</h3>
<p>현재 활성: <b>{{ active }}</b></p>

<form method="POST" action="/admin">
{% for p in profiles %}
<div class="profile {% if p.name == active %}active{% endif %}">
<label><input type="radio" name="profile" value="{{ p.name }}" {% if p.name == active %}checked{% endif %}>
<b>{{ p.name }}</b> — {{ p.provider }} / {{ p.model }}
{% if p.key_ok %}<span class="ok">(키 등록됨 ✓ {{ p.masked }})</span>{% else %}<span class="bad">(키 미등록)</span>{% endif %}
</label></div>
{% endfor %}
<div class="profile {% if active == '테스트 모드' %}active{% endif %}">
<label><input type="radio" name="profile" value="테스트 모드" {% if active == '테스트 모드' %}checked{% endif %}>
<b>테스트 모드</b> — AI 없이 동작</label></div>
<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="switch" style="padding:8px 15px;">엔진 변경</button>
<button type="submit" name="action" value="test" style="padding:8px 15px;">🔌 연결 테스트</button>
</form>
</div>

<div class="box"><h3>🔑 AI 툴 등록 / 변경</h3>
<p class="hint">이름과 API 키를 등록하면 바로 사용할 수 있습니다. 같은 이름으로 다시 등록하면 덮어쓰기(키 변경)가 됩니다.<br>
키는 데이터베이스에 저장되므로 재배포해도 유지됩니다.</p>

<form method="POST" action="/admin">
<fieldset><legend>등록 / 수정</legend>
<p>이름 <input type="text" name="p_name" placeholder="예: Gemini" style="width:150px;">
&nbsp;제공사
<select name="p_provider">
  {% for pv in providers %}<option value="{{ pv }}">{{ pv }}</option>{% endfor %}
</select></p>
<p>모델 <input type="text" name="p_model" placeholder="비우면 기본 모델 사용" style="width:220px;"></p>
<p>API 키 <input type="text" name="p_key" placeholder="키 입력 (비우면 기존 키 유지)" style="width:340px;"></p>
<label class="hint"><input type="checkbox" name="p_activate" value="1" checked> 등록 후 바로 이 툴을 활성화</label>
</fieldset>

<fieldset><legend>삭제</legend>
<p>이름 <input type="text" name="del_name" placeholder="삭제할 툴 이름" style="width:150px;"></p>
</fieldset>

<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="save_profile" style="padding:8px 15px;">저장</button>
<button type="submit" name="action" value="delete_profile" style="padding:8px 15px;" onclick="return confirm('정말 삭제하시겠습니까?')">삭제</button>
</form>

<p class="hint">기본 모델 — gemini: {{ default_models['gemini'] }} / openai: {{ default_models['openai'] }} / anthropic: {{ default_models['anthropic'] }}</p>
</div>

<div class="box"><h3>👤 아이디 생성 및 관리</h3>
<table>
<tr><th>아이디</th><th>전적</th><th>점수</th></tr>
{% for u in users %}<tr><td>{{u.username}}</td><td>{{u.wins}}승 {{u.losses}}패</td><td>{{u.points}}</td></tr>{% endfor %}
</table>
<form method="POST" action="/admin">
<p><b>생성:</b> 아이디 <input type="text" name="new_username" placeholder="새 아이디" style="width:130px;"> PIN <input type="text" name="new_pin" placeholder="4자리 이상" style="width:110px;"></p>
<p><b>PIN 변경:</b> 아이디 <input type="text" name="edit_username" placeholder="대상 아이디" style="width:130px;"> 새 PIN <input type="text" name="edit_pin" placeholder="4자리 이상" style="width:110px;"></p>
<p><b>삭제:</b> 아이디 <input type="text" name="delete_username" placeholder="삭제할 아이디" style="width:130px;"></p>
<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="create_user" style="padding:8px 15px;">생성</button>
<button type="submit" name="action" value="edit_user" style="padding:8px 15px;">PIN 변경</button>
<button type="submit" name="action" value="delete_user" style="padding:8px 15px;" onclick="return confirm('정말 삭제하시겠습니까?')">삭제</button>
</form></div>

<div class="box"><h3>🩺 AI 호출 상태 (오늘)</h3>
{% if quota_hit %}
<p style="background:#fef2f2;border:1px solid #fca5a5;color:#991b1b;padding:10px;border-radius:8px;">
⚠️ 최근 호출에서 <b>사용량 한도 초과(429)</b>가 감지되었습니다. 무료 한도를 다 쓰면 주제 추천·요약·심사가 모두 실패하고,
심사는 <b>심사실패</b>로 기록됩니다. 하루가 지나 한도가 초기화되길 기다리거나 다른 키를 등록하세요.</p>
{% endif %}
<table>
<tr><th>종류</th><th>성공</th><th>실패</th></tr>
{% for kind, s in ai_stats.items() %}
<tr><td>{{ kind }}</td><td class="ok">{{ s.ok }}</td>
<td class="{% if s.fail %}bad{% endif %}">{{ s.fail }}</td></tr>
{% else %}<tr><td colspan="3">오늘 호출 기록이 없습니다.</td></tr>{% endfor %}
</table>
<p class="hint">토론 1회당 호출 수 = 주제 1 + 요약 8 + 심사 1 ≈ <b>10회</b>. 요약이 가장 많이 씁니다.</p>
<p style="background:#f0f9ff;border:1px solid #bae6fd;padding:10px;border-radius:8px;">
📊 최근 1분간 호출: <b>{{ rpm_used }}회</b> / 상한 {{ rpm_limit }}회
{% if rpm_used >= rpm_limit %}<span class="bad">— 대기 중</span>{% endif %}
<br><span class="hint">Gemini 무료 한도는 <b>분당 20회</b>입니다. 상한을 그보다 낮게 두면
한도 초과 자체가 일어나지 않고, 요약은 붐빌 때 자동으로 건너뜁니다.</span></p>
<form method="POST" action="/admin">
<p><label><input type="checkbox" name="summary_on" value="1" {% if summary_on %}checked{% endif %}>
발언 요약 사용 (끄면 AI 호출이 <b>10회 → 2회</b>로 줄어 한도 부족 시 유용)</label></p>
<p>분당 호출 상한 <input type="text" name="max_rpm" value="{{ rpm_limit }}" style="width:70px;">
<span class="hint">권장 15 — 무료 한도 20보다 여유 있게</span></p>
<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="toggle_summary" style="padding:8px 15px;">설정 저장</button>
</form>
{% if backup_count %}
<p class="hint">🔑 예비 키 {{ backup_count }}개 등록됨 — 활성 키가 한도에 걸리면 자동으로 넘어갑니다.</p>
{% else %}
<p class="hint">🔑 예비 키가 없습니다. 다른 구글 계정으로 키를 하나 더 만들어 등록해두면,
한도에 걸렸을 때 자동으로 그 키를 씁니다.</p>
{% endif %}
<details style="margin-top:10px;"><summary>최근 호출 15건</summary>
<table>
<tr><th>시각</th><th>종류</th><th>결과</th><th>내용</th></tr>
{% for c in ai_recent %}
<tr><td>{{ c.at[11:] }}</td><td>{{ c.kind }}</td>
<td class="{% if c.ok %}ok{% else %}bad{% endif %}">{{ '성공' if c.ok else '실패' }}</td>
<td style="font-size:0.85em;word-break:break-all;">{{ c.error[:90] if c.error else (c.ms|string + 'ms') }}</td></tr>
{% else %}<tr><td colspan="4">기록 없음</td></tr>{% endfor %}
</table></details>
</div>

<div class="box"><h3>🔴 실시간 관전</h3>
<p>운영자 코드: <input type="password" id="watch-code" placeholder="코드 입력"></p>
<button onclick="loadRooms()" style="padding:8px 15px;">진행 중인 토론 불러오기</button>
<div id="room-list" style="margin-top:10px;"></div>
<div id="watch-chat" class="hidden"></div>
</div>

<p class="nav"><a href="/">← 토론 화면</a> · <a href="/ranking">🏆 랭킹</a> · <a href="/history">📚 토론 기록</a></p>
<script>
const wsocket=io();
function loadRooms(){
  const code=document.getElementById('watch-code').value;
  fetch('/admin/rooms',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({code})})
    .then(r=>r.json()).then(data=>{
      if(data.error){alert(data.error);return;}
      const list=document.getElementById('room-list');list.innerHTML='';
      if(data.rooms.length===0){list.innerHTML='<p>진행 중인 토론이 없습니다.</p>';return;}
      data.rooms.forEach(rm=>{
        const btn=document.createElement('div');btn.className='room-btn';
        const who=rm.players.map(p=>{
          const tag="<span class='side-tag "+(p.side==='찬성'?'side-pro':'side-con')+"'>"+p.side+"</span>";
          const net=p.connected?'':" <span style='color:#dc2626;'>⚠️끊김</span>";
          const now=p.speaking?" <span style='color:#2563eb;'>◀ 발언 중</span>":"";
          return "<b>"+p.username+"</b> "+tag+net+now+
                 "<br><span style='color:#9ca3af;font-size:0.85em;'>"+p.alias+"</span>";
        }).join("<div style='color:#9ca3af;margin:4px 0;'>vs</div>");
        btn.innerHTML="<div style='font-weight:bold;margin-bottom:6px;'>📌 "+rm.topic+"</div>"+
          "<div style='font-size:0.85em;color:#6b7280;margin-bottom:6px;'>"+
          rm.stage+" ("+(rm.stage_index+1)+"/"+rm.total_stages+") · 발언 "+rm.turns+"회 · "+
          rm.elapsed_min+"분 경과</div>"+who;
        btn.onclick=()=>watchRoom(rm.room_id,code);
        list.appendChild(btn);
      });
    });
}
function watchRoom(roomId,code){
  const box=document.getElementById('watch-chat');
  box.classList.remove('hidden');box.innerHTML='';
  wsocket.emit('admin_watch',{code:code,room_id:roomId});
}
function appendWatchLine(line){
  const box=document.getElementById('watch-chat');
  const el=document.createElement('p');
  el.innerHTML='<b>['+line.stage+'] '+line.side+':</b> '+line.message;
  box.appendChild(el);box.scrollTop=box.scrollHeight;
}
wsocket.on('admin_history',d=>{
  const box=document.getElementById('watch-chat');
  box.innerHTML='<p><b>주제:</b> '+d.topic+'</p>';
  d.logs.forEach(appendWatchLine);
});
wsocket.on('admin_message',appendWatchLine);
wsocket.on('admin_summary',d=>{
  const box=document.getElementById('watch-chat');
  const el=document.createElement('p');
  el.style.cssText='color:#6b7280;font-size:0.9em;padding-left:10px;border-left:2px solid #d1d5db;';
  el.textContent='🤖 요약: '+d.summary;box.appendChild(el);box.scrollTop=box.scrollHeight;});
wsocket.on('admin_stage',d=>{
  const box=document.getElementById('watch-chat');
  const el=document.createElement('p');el.style.cssText='color:#7c3aed;font-weight:bold;';
  el.textContent='▶ 단계 전환: '+d.stage;box.appendChild(el);box.scrollTop=box.scrollHeight;
});
wsocket.on('admin_end',d=>{
  const box=document.getElementById('watch-chat');
  const el=document.createElement('p');el.style.cssText='color:#dc2626;font-weight:bold;white-space:pre-line;';
  el.textContent='🔔 토론 종료\\n'+d.result;box.appendChild(el);box.scrollTop=box.scrollHeight;
});
wsocket.on('error_msg',d=>alert(d.msg));
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
승리 +20점 · 패배 −15점 · 완주 +5점 (시작 점수 1000점)</p>
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
<p class="nav"><a href="/">← 토론 화면</a> · <a href="/history">토론 기록</a></p>
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
.winner-tag{display:inline-block;padding:2px 8px;border-radius:6px;font-weight:bold;font-size:0.9em;}
.w-fail{background:#fef3c7;color:#92400e;}</style></head><body>
<h2>📚 지난 토론 기록 (최근 {{ debates|length }}건)</h2>
{% for d in debates %}
<div class="box"><b>📌 {{ d.topic }}</b>
<p class="meta">{{ d.played_at }} | A: {{ d.player_a }} ({{ d.side_a }}) vs B: {{ d.player_b }} ({{ d.side_b }}) | 엔진: {{ d.engine }}</p>
<p><b>결과: </b>{% if d.winner == '심사실패' %}<span class="winner-tag w-fail">⚠️ 심사 실패 (전적 미반영)</span>{% else %}<b>{{ d.winner }}</b>{% endif %}</p>
<div class="reason">{{ d.reason }}</div>
<details><summary>토론 전문 보기</summary>
{% for line in d.logs %}<p class="log-line {{ 'pro' if line.side == '찬성' else 'con' }}"><b>[{{ line.stage }}] {{ line.side }}:</b> {{ line.text }}</p>{% endfor %}
</details></div>{% else %}<p>아직 토론 기록이 없습니다.</p>{% endfor %}
<p><a href="/">← 토론 화면</a> · <a href="/ranking">🏆 랭킹</a></p></body></html>"""


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, engine_name=get_active_name(),
                                  turn_seconds=TURN_SECONDS, min_length=MIN_MESSAGE_LENGTH)


@app.route('/admin', methods=['GET', 'POST'])
def admin():
    message = None
    if request.method == 'POST':
        code = request.form.get('code', '')
        action = request.form.get('action', '')
        if code != ADMIN_CODE:
            message = "❌ 운영자 코드가 틀렸습니다."

        # ── AI 엔진 전환 / 테스트 ──
        elif action == 'switch':
            chosen = request.form.get('profile', TEST_MODE)
            valid_names = [p['name'] for p in list_profiles()] + [TEST_MODE]
            if chosen in valid_names:
                set_setting("active_profile", chosen)
                message = f"✅ '{chosen}'으로 변경되었습니다."
            else:
                message = "❌ 존재하지 않는 프로필입니다."
        elif action == 'test':
            ok, detail = test_llm_connection()
            message = ("✅ 연결 성공! " if ok else "❌ 연결 실패: ") + detail
        elif action == 'toggle_summary':
            on = request.form.get('summary_on') == '1'
            set_setting("summary_enabled", "1" if on else "0")
            parts = ["발언 요약 " + ("사용" if on else "끔")]
            raw_rpm = (request.form.get('max_rpm') or '').strip()
            if raw_rpm:
                try:
                    val = max(1, min(60, int(raw_rpm)))
                    set_setting("ai_max_rpm", str(val))
                    parts.append(f"분당 상한 {val}회")
                except ValueError:
                    parts.append("분당 상한은 숫자로 입력해주세요")
            message = "✅ " + ", ".join(parts)

        # ── AI 툴 등록 / 수정 / 삭제 ──
        elif action == 'save_profile':
            name = (request.form.get('p_name') or '').strip()
            provider = (request.form.get('p_provider') or 'gemini').strip()
            model = (request.form.get('p_model') or '').strip()
            key = (request.form.get('p_key') or '').strip()
            activate = request.form.get('p_activate') == '1'
            if not name:
                message = "❌ 툴 이름을 입력해주세요."
            elif name == TEST_MODE:
                message = f"❌ '{TEST_MODE}'는 예약된 이름입니다."
            elif provider not in PROVIDERS:
                message = "❌ 지원하지 않는 제공사입니다."
            else:
                existing = db_fetchone("SELECT model, api_key FROM ai_profiles WHERE name = %s", (name,))
                if not model:
                    model = (existing[0] if existing and existing[0] else DEFAULT_MODELS[provider])
                if not key:
                    key = (existing[1] if existing else '') or ''
                if not key:
                    message = "❌ API 키를 입력해주세요. (신규 등록은 키가 필요합니다)"
                else:
                    upsert_profile(name, provider, model, key)
                    if activate:
                        set_setting("active_profile", name)
                    verb = "수정" if existing else "등록"
                    message = f"✅ '{name}' {verb} 완료 ({provider}/{model})" + (" — 활성화됨" if activate else "")

        elif action == 'delete_profile':
            target = (request.form.get('del_name') or '').strip()
            if not target:
                message = "❌ 삭제할 툴 이름을 입력해주세요."
            elif not db_fetchone("SELECT name FROM ai_profiles WHERE name = %s", (target,)):
                message = f"❌ '{target}' 툴을 찾을 수 없습니다."
            else:
                delete_profile(target)
                message = f"✅ '{target}' 툴을 삭제했습니다."

        # ── 사용자 관리 ──
        elif action == 'create_user':
            new_user = (request.form.get('new_username') or '').strip()
            new_pin = (request.form.get('new_pin') or '').strip()
            if not new_user or len(new_pin) < 4:
                message = "❌ 아이디와 4자리 이상 PIN을 입력해주세요."
            elif db_fetchone("SELECT username FROM users WHERE username = %s", (new_user,)):
                message = f"❌ '{new_user}' 아이디가 이미 존재합니다."
            else:
                db_execute("INSERT INTO users (username, pin_hash) VALUES (%s, %s)", (new_user, hash_pin(new_user, new_pin)))
                message = f"✅ '{new_user}' 아이디를 생성했습니다."
        elif action == 'edit_user':
            target = (request.form.get('edit_username') or '').strip()
            new_pin = (request.form.get('edit_pin') or '').strip()
            if not target or len(new_pin) < 4:
                message = "❌ 아이디와 4자리 이상 새 PIN을 입력해주세요."
            elif not db_fetchone("SELECT username FROM users WHERE username = %s", (target,)):
                message = f"❌ '{target}' 아이디를 찾을 수 없습니다."
            else:
                db_execute("UPDATE users SET pin_hash = %s WHERE username = %s", (hash_pin(target, new_pin), target))
                message = f"✅ '{target}'의 PIN을 변경했습니다."
        elif action == 'delete_user':
            target = (request.form.get('delete_username') or '').strip()
            if not db_fetchone("SELECT username FROM users WHERE username = %s", (target,)):
                message = f"❌ '{target}' 아이디를 찾을 수 없습니다."
            else:
                db_execute("DELETE FROM users WHERE username = %s", (target,))
                message = f"✅ '{target}' 아이디를 삭제했습니다."

    profiles_view = [{"name": p["name"], "provider": p["provider"], "model": p["model"],
                      "key_ok": bool(p["api_key"].strip()),
                      "masked": mask_key(p["api_key"])}
                     for p in list_profiles()]
    user_rows = db_fetchall("SELECT username, wins, losses, points FROM users ORDER BY username")
    users_view = [{"username": r[0], "wins": r[1], "losses": r[2], "points": r[3]} for r in user_rows]
    ai_stats, ai_recent, quota_hit = ai_call_summary()
    return render_template_string(ADMIN_TEMPLATE, profiles=profiles_view, users=users_view,
                                  active=get_active_name(), message=message,
                                  providers=PROVIDERS, default_models=DEFAULT_MODELS,
                                  ai_stats=ai_stats, ai_recent=ai_recent, quota_hit=quota_hit,
                                  summary_on=summaries_enabled(),
                                  rpm_used=current_rpm(), rpm_limit=max_rpm(),
                                  backup_count=len(backup_profiles()))


@app.route('/admin/rooms', methods=['POST'])
def admin_rooms():
    data = request.get_json(silent=True) or {}
    if data.get('code') != ADMIN_CODE:
        return jsonify({'error': '운영자 코드가 틀렸습니다.'}), 403
    room_list = []
    now = time.time()
    for rid, r in rooms.items():
        stage_idx = min(r['turn_count'], TOTAL_STAGES - 1)
        speaker = r['players'][r['current_speaker']]
        room_list.append({
            'room_id': rid, 'topic': r['topic'],
            'stage': 'AI 심사 중' if r.get('judging') else STAGES[stage_idx],
            'stage_index': stage_idx, 'total_stages': TOTAL_STAGES,
            'turns': len(r['logs']),
            'elapsed_min': int((now - r.get('started_at', now)) // 60),
            'speaker': speaker['username'],
            'players': [{
                'username': p['username'], 'alias': p['alias'], 'side': p['side'],
                'connected': p.get('connected', True),
                'speaking': (p is speaker) and not r.get('judging'),
            } for p in r['players']],
        })
    room_list.sort(key=lambda x: -x['turns'])
    return jsonify({'rooms': room_list})


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


def close_room(room_id):
    room = rooms.pop(room_id, None)
    if room:
        for p in room['players']:
            sid_to_room.pop(p['sid'], None)
            user_to_room.pop(p['username'], None)

def make_reveal_text(room):
    a, b = room['players'][0], room['players'][1]
    return f"🎭 정체 공개!\n{a['alias']} ({a['side']}) = {a['username']}\n{b['alias']} ({b['side']}) = {b['username']}"

def who(player):
    """기록에 남길 이름 표기 — 아이디(찬성/반대)"""
    return f"{player['username']}({player['side']})"

def end_by_withdrawal(room_id, loser, winner, kind):
    """퇴장 또는 항복으로 토론을 끝낸다. kind: '퇴장' 또는 '항복'"""
    room = rooms.get(room_id)
    if not room:
        return
    record_win_loss(winner['username'], loser['username'])
    reveal = make_reveal_text(room)
    headline = f"{who(loser)} {kind}으로 {who(winner)} 몰수승"
    save_debate(room['topic'], room['players'][0]['username'], room['players'][1]['username'],
                room['players'][0]['side'], room['players'][1]['side'],
                room['logs'], "몰수", f"{headline}\n\n{reveal}")
    socketio.emit('opponent_left', {'reveal': reveal, 'headline': headline}, room=winner['sid'])
    socketio.emit('surrendered', {'reveal': reveal, 'headline': headline}, room=loser['sid'])
    socketio.emit('admin_end', {'result': f"{headline}\n\n{reveal}"}, room=f"watch_{room_id}")
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
    end_by_withdrawal(room_id, me, opp, "항복")


@socketio.on('login')
def handle_login(data):
    username = (data.get('username') or '').strip()
    pin = (data.get('pin') or '').strip()
    if not username or len(pin) < 4:
        emit('error_msg', {'msg': '아이디를 입력하고 PIN은 4자리 이상으로 해주세요.'})
        return
    # 이미 접속 중이더라도, 끊긴 채로 남아 있는 유령 세션은 정리하고 통과시킨다
    stale = [s for s, u in sid_to_user.items() if u == username and s != request.sid]
    for s in stale:
        if s not in sid_to_room:
            sid_to_user.pop(s, None)
    if username in sid_to_user.values():
        emit('error_msg', {'msg': '이 아이디는 이미 다른 기기에서 접속 중입니다.'})
        return
    row = db_fetchone("SELECT pin_hash FROM users WHERE username = %s", (username,))
    if not row:
        emit('error_msg', {'msg': '존재하지 않는 아이디입니다. 운영자에게 문의하세요.'})
        return
    if row[0] != hash_pin(username, pin):
        emit('error_msg', {'msg': 'PIN이 틀렸습니다.'})
        return
    sid_to_user[request.sid] = username
    token = uuid.uuid4().hex
    resume_tokens[token] = username
    stats = get_user_stats(username)
    emit('login_ok', {'username': username, 'resume_token': token,
                      'wins': stats['wins'], 'losses': stats['losses'],
                      'points': stats['points'], 'rank': stats['rank']})


@socketio.on('join_queue')
def handle_join_queue(data):
    user_sid = request.sid
    username = sid_to_user.get(user_sid)
    if not username:
        emit('error_msg', {'msg': '먼저 로그인해주세요.'})
        return
    if any(p['sid'] == user_sid for p in waiting_pool) or user_sid in sid_to_room:
        return
    waiting_pool.append({"sid": user_sid, "username": username})
    emit('status', {'msg': f'대기열에서 상대방 매칭을 기다리는 중... (대기 {len(waiting_pool)}명)'})
    if len(waiting_pool) >= 2:
        p1 = waiting_pool.pop(random.randint(0, len(waiting_pool) - 1))
        p2 = waiting_pool.pop(random.randint(0, len(waiting_pool) - 1))
        try:
            start_debate(p1, p2)
        except Exception as e:
            # 방을 못 만들면 두 사람을 대기열로 되돌린다 (사라져서 영영 매칭 안 되는 것 방지)
            print(f"[매칭 실패] {type(e).__name__}: {e}")
            waiting_pool.extend([p1, p2])
            for p in (p1, p2):
                socketio.emit('error_msg',
                              {'msg': '매칭 중 문제가 생겨 대기열로 돌아갔습니다. 잠시 후 자동으로 다시 시도됩니다.'},
                              room=p['sid'])
                socketio.emit('rejoin_queue', {}, room=p['sid'])


def start_debate(p1, p2):
        title_a, title_b = random.sample(ALIAS_POOL, 2)
        side_a, side_b = random.sample(["찬성", "반대"], 2)
        p1['alias'] = title_a
        p2['alias'] = title_b
        p1['side'] = side_a
        p2['side'] = side_b
        room_id = f"room_{uuid.uuid4().hex[:8]}"
        topic = take_topic()          # 미리 만들어둔 주제 — 기다리지 않는다
        p1['connected'] = True
        p2['connected'] = True
        rooms[room_id] = {"players": [p1, p2], "turn_count": 0, "current_speaker": 0,
                          "topic": topic, "logs": [], "turn_started": time.time(),
                          "started_at": time.time()}
        sid_to_room[p1['sid']] = room_id
        sid_to_room[p2['sid']] = room_id
        user_to_room[p1['username']] = room_id
        user_to_room[p2['username']] = room_id
        join_room(room_id, sid=p1['sid'])
        join_room(room_id, sid=p2['sid'])
        common = {'room_id': room_id, 'topic': topic, 'stage': STAGES[0], 'stage_index': 0,
                  'total_stages': TOTAL_STAGES, 'seconds_left': TURN_SECONDS,
                  'speaker_alias': p1['alias'], 'speaker_side': p1['side'],
                  'criteria': [{'stage': s, 'desc': d} for s, d in JUDGE_CRITERIA],
                  'min_length': MIN_MESSAGE_LENGTH}
        emit('match_found', {**common, 'your_turn': True,
                             'my_alias': p1['alias'], 'opp_alias': p2['alias'],
                             'my_side': p1['side'], 'opp_side': p2['side']}, room=p1['sid'])
        emit('match_found', {**common, 'your_turn': False,
                             'my_alias': p2['alias'], 'opp_alias': p1['alias'],
                             'my_side': p2['side'], 'opp_side': p1['side']}, room=p2['sid'])


@socketio.on('admin_watch')
def handle_admin_watch(data):
    if data.get('code') != ADMIN_CODE:
        emit('error_msg', {'msg': '운영자 코드가 틀렸습니다.'})
        return
    room_id = data.get('room_id')
    room = rooms.get(room_id)
    if not room:
        emit('error_msg', {'msg': '존재하지 않거나 이미 종료된 토론입니다.'})
        return
    join_room(f"watch_{room_id}")
    history_logs = [{'stage': l['stage'], 'side': l['side'], 'message': l['text']} for l in room['logs']]
    emit('admin_history', {'topic': room['topic'], 'logs': history_logs})


def summarize_in_background(room_id, msg_id, text):
    """AI 요약을 따로 돌려 준비되는 대로 채워 넣는다."""
    summary = get_ai_summary(text)
    socketio.emit('receive_summary', {'msg_id': msg_id, 'summary': summary}, room=room_id)
    if summary:
        socketio.emit('admin_summary', {'msg_id': msg_id, 'summary': summary},
                      room=f"watch_{room_id}")


def judge_in_background(room_id):
    """AI 심사를 따로 돌린다. 그동안 참가자에게는 심사 중 안내가 떠 있다."""
    room = rooms.get(room_id)
    if not room:
        return
    winner_side, result_text = judge_debate(room['topic'], room['logs'])
    player_a, player_b = room['players'][0], room['players'][1]
    add_points(player_a['username'], 5)
    add_points(player_b['username'], 5)
    if winner_side == player_a['side']:
        record_win_loss(player_a['username'], player_b['username'])
    elif winner_side == player_b['side']:
        record_win_loss(player_b['username'], player_a['username'])
    final_text = result_text + "\n\n" + make_reveal_text(room)
    save_debate(room['topic'], player_a['username'], player_b['username'],
                player_a['side'], player_b['side'], room['logs'], winner_side, final_text)
    socketio.emit('debate_end', {'result': final_text}, room=room_id)
    socketio.emit('admin_end', {'result': final_text}, room=f"watch_{room_id}")
    close_room(room_id)


@socketio.on('send_message')
def handle_send_message(data):
    room_id = data.get('room_id')
    msg_text = (data.get('message') or '').strip()
    room = rooms.get(room_id)
    if not room or not msg_text:
        return
    speaker_idx = room['current_speaker']
    speaker = room['players'][speaker_idx]
    if request.sid != speaker['sid']:
        emit('error_msg', {'msg': '지금은 당신의 발언 차례가 아닙니다.'})
        return

    # 너무 짧은 발언은 반려한다 (시간 초과 자동 전송은 예외)
    if msg_text != TIMEOUT_MESSAGE and len(msg_text) < MIN_MESSAGE_LENGTH:
        emit('message_rejected', {
            'msg': f'발언이 너무 짧습니다. 최소 {MIN_MESSAGE_LENGTH}자 이상 작성해주세요.',
            'text': msg_text, 'length': len(msg_text), 'required': MIN_MESSAGE_LENGTH})
        return

    stage_name = STAGES[room['turn_count']]
    msg_id = uuid.uuid4().hex[:12]
    room['logs'].append({"side": speaker['side'], "stage": stage_name, "text": msg_text})

    # 발언은 즉시 전달하고, 느린 AI 요약은 백그라운드에서 채워 넣는다
    emit('receive_message', {'sender': speaker['alias'], 'side': speaker['side'],
                             'message': msg_text, 'msg_id': msg_id, 'stage': stage_name},
         room=room_id)
    emit('admin_message', {'side': speaker['side'], 'message': msg_text, 'stage': stage_name}, room=f"watch_{room_id}")
    socketio.start_background_task(summarize_in_background, room_id, msg_id, msg_text)

    stage_completed = (speaker_idx == 1)
    if stage_completed:
        room['turn_count'] += 1
    if room['turn_count'] >= TOTAL_STAGES:
        # 심사도 느리므로 안내를 먼저 띄우고 백그라운드에서 처리
        room['judging'] = True
        emit('judging', {'msg': 'AI가 토론을 심사하고 있습니다...'}, room=room_id)
        emit('admin_stage', {'stage': 'AI 심사 중'}, room=f"watch_{room_id}")
        socketio.start_background_task(judge_in_background, room_id)
    else:
        room['current_speaker'] = 1 - speaker_idx
        next_idx = room['current_speaker']
        new_stage = STAGES[room['turn_count']]
        room['turn_started'] = time.time()
        for i, p in enumerate(room['players']):
            emit('turn_change', {'your_turn': i == next_idx, 'stage': new_stage,
                                 'stage_index': room['turn_count'], 'stage_changed': stage_completed,
                                 'seconds_left': TURN_SECONDS,
                                 'speaker_alias': room['players'][next_idx]['alias'],
                                 'speaker_side': room['players'][next_idx]['side']}, room=p['sid'])
        if stage_completed:
            emit('admin_stage', {'stage': new_stage}, room=f"watch_{room_id}")


def forfeit_after_grace(room_id, username, marked_at):
    """유예 시간이 지나도 돌아오지 않으면 그때 몰수 처리한다."""
    socketio.sleep(RECONNECT_GRACE_SECONDS)
    room = rooms.get(room_id)
    if not room or room.get('judging'):
        return
    leaver = next((p for p in room['players'] if p['username'] == username), None)
    if not leaver or leaver.get('connected', True):
        return                              # 돌아왔음
    if leaver.get('disconnect_at') != marked_at:
        return                              # 그사이 다시 끊긴 건이므로 최신 타이머에 맡긴다
    stayer = next((p for p in room['players'] if p['username'] != username), None)
    if not stayer:
        return
    end_by_withdrawal(room_id, leaver, stayer, "퇴장")


@socketio.on('disconnect')
def handle_disconnect(*args):
    sid = request.sid
    username = sid_to_user.pop(sid, None)
    waiting_pool[:] = [p for p in waiting_pool if p['sid'] != sid]
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

    # 즉시 몰수시키지 않고 유예 시간을 준다 — 잠깐 끊긴 것일 수 있다
    marked_at = time.time()
    leaver['connected'] = False
    leaver['disconnect_at'] = marked_at
    socketio.emit('opponent_disconnected',
                  {'seconds': RECONNECT_GRACE_SECONDS}, room=stayer['sid'])
    socketio.start_background_task(forfeit_after_grace, room_id, leaver['username'], marked_at)


@socketio.on('resume')
def handle_resume(data):
    """끊겼다 돌아온 참가자를 원래 방에 다시 앉힌다."""
    token = (data or {}).get('token')
    username = resume_tokens.get(token)
    if not username:
        emit('resume_failed', {})
        return
    sid = request.sid
    sid_to_user[sid] = username
    room_id = user_to_room.get(username)
    room = rooms.get(room_id) if room_id else None
    if not room:
        emit('resume_failed', {})
        return

    me = next((p for p in room['players'] if p['username'] == username), None)
    opp = next((p for p in room['players'] if p['username'] != username), None)
    if not me or not opp:
        emit('resume_failed', {})
        return

    my_index = room['players'].index(me)
    me['sid'] = sid
    me['connected'] = True
    sid_to_room[sid] = room_id
    join_room(room_id)

    stage_idx = min(room['turn_count'], TOTAL_STAGES - 1)
    emit('resumed', {
        'room_id': room_id, 'topic': room['topic'],
        'my_alias': me['alias'], 'opp_alias': opp['alias'],
        'my_side': me['side'], 'opp_side': opp['side'],
        'stage': STAGES[stage_idx], 'stage_index': stage_idx, 'total_stages': TOTAL_STAGES,
        'your_turn': room['current_speaker'] == my_index,
        'seconds_left': max(0, int(TURN_SECONDS - (time.time() - room.get('turn_started', time.time())))),
        'speaker_alias': room['players'][room['current_speaker']]['alias'],
        'speaker_side': room['players'][room['current_speaker']]['side'],
        'criteria': [{'stage': s, 'desc': d} for s, d in JUDGE_CRITERIA],
        'min_length': MIN_MESSAGE_LENGTH,
        'logs': [{'sender': (me['alias'] if l['side'] == me['side'] else opp['alias']),
                  'side': l['side'], 'stage': l['stage'], 'message': l['text']} for l in room['logs']],
    })
    socketio.emit('opponent_reconnected', {}, room=opp['sid'])


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
