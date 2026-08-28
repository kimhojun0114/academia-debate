import os
import json
import random
import uuid
import hashlib
import psycopg2
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
from flask_socketio import SocketIO, emit, join_room

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('DEBATE_SECRET_KEY', 'debate_secret_key_1234')
socketio = SocketIO(app, cors_allowed_origins="*")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL 환경변수가 설정되지 않았습니다. Supabase 연결 문자열을 등록해주세요.")

waiting_pool = []
rooms = {}
sid_to_room = {}
sid_to_user = {}

STAGES = ["입론", "반론", "반박", "최종변론"]
TOTAL_STAGES = len(STAGES)
TURN_SECONDS = 300

ADMIN_CODE = os.environ.get("DEBATE_ADMIN_CODE", "1234")
TEST_MODE = "테스트 모드"

# 기본 Gemini 키 (환경변수 GEMINI_API_KEY가 있으면 그쪽이 우선)
DEFAULT_GEMINI_KEY = "AQ.Ab8RN6LuDcj_jU7-uZHXTq8uQELxQPoh6mrDkzldT3RfG5pPYA"

PROVIDERS = ["gemini", "openai", "anthropic"]
DEFAULT_MODELS = {
    "gemini": "gemini-2.5-flash",
    "openai": "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5",
}

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
def llm_call(prompt, max_tokens=600):
    profile = get_active_profile()
    if not profile:
        return None
    provider = profile["provider"]
    try:
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
            from google import genai
            c = genai.Client(api_key=profile["api_key"])
            r = c.models.generate_content(model=profile["model"], contents=prompt)
            return r.text.strip()
        else:
            return None
    except ImportError:
        print(f"[LLM 오류] {provider} SDK 미설치")
        return None
    except Exception as e:
        print(f"[LLM 호출 실패] {provider}: {e}")
        return None


def test_llm_connection():
    """관리자 페이지의 '연결 테스트' 버튼용."""
    profile = get_active_profile()
    if not profile:
        return False, "활성 프로필이 없거나 API 키가 비어 있습니다."
    try:
        result = llm_call("'연결 성공'이라고만 답해.", max_tokens=50)
        if result:
            return True, f"응답: {result[:60]}"
        return False, "응답이 비어 있습니다. 서버 로그의 [LLM 호출 실패] 메시지를 확인하세요."
    except Exception as e:
        return False, str(e)[:200]


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


# ─────────────────────────────────────────────
# AI 기능
# ─────────────────────────────────────────────
def get_ai_topic():
    recent_rows = db_fetchall("SELECT topic FROM debates ORDER BY id DESC LIMIT 30")
    recent_topics = list(dict.fromkeys(r[0] for r in recent_rows))[:15]
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
    result = llm_call(prompt)
    if not result:
        return "인공지능 발전은 인간에게 유익한가?"
    return result.strip().strip('"').strip("'").splitlines()[0]

def get_ai_summary(text):
    result = llm_call(f"다음 토론 발언을 한 문장으로 핵심만 요약해줘:\n{text}", max_tokens=200)
    return result if result else f"[요약] {text[:15]}..."

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
    raw = llm_call(prompt, max_tokens=900)
    if not raw:
        return "무승부", "무승부 (테스트 모드)"
    try:
        data = json.loads(raw.replace("```json", "").replace("```", "").strip())
        winner = data.get("winner", "무승부")
        if winner not in ("찬성", "반대", "무승부"):
            winner = "무승부"
        verdict_line = f"판정: {winner}측 승리" if winner != "무승부" else "판정: 무승부"
        result_text = (verdict_line + "\n" +
                       f"점수: 찬성 {data.get('score_pro','?')}점 vs 반대 {data.get('score_con','?')}점 (20점 만점)\n" +
                       f"근거: {data.get('reason','근거 없음')}")
        return winner, result_text
    except Exception:
        return "무승부", f"판정 오류\nAI 원문: {raw[:200]}"


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
</div>
<div id="profile-area" class="box hidden">
  <p>👤 <b id="my-name"></b>님, 환영합니다!</p>
  <p class="stats" id="my-stats"></p>
  <p style="font-size:0.9em;color:#555;">✅ 끝까지 완주하면 승패와 관계없이 <b>+5점</b>!</p>
  <button onclick="joinQueue()" id="match-btn" style="padding:10px 20px;cursor:pointer;background:#2563eb;color:white;border:none;border-radius:6px;font-weight:bold;">토론 매칭 시작</button>
  <p id="queue-text" style="color:#2563eb;font-weight:bold;margin-top:10px;"></p>
</div>
<div id="debate-area" class="box hidden">
  <div id="stage-dots"></div>
  <div id="stage-banner">단계 준비 중...</div>
  <h3 id="topic-area" style="color:#1f2937;">주제: 추천 중...</h3>
  <p id="role-area" style="font-weight:bold;color:#4b5563;"></p>
  <p id="turn-status" style="color:#dc2626;font-weight:bold;margin-bottom:15px;"></p>
  <div id="chat-box"></div>
  <textarea id="msg-input" style="width:96%;height:70px;padding:10px;border-radius:6px;border:1px solid #ccc;" placeholder="여기에 논리를 펼쳐주세요 (복사/붙여넣기 금지)"></textarea>
  <button id="send-btn" onclick="sendMessage()" style="width:100%;margin-top:8px;padding:10px;background:#2563eb;color:white;border:none;border-radius:6px;cursor:pointer;font-weight:bold;">발언 완료 (상대방에게 전송)</button>
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
  if(isCtrl&&key==='a'&&isTyping)return;
  if(isCtrl&&['c','v','x','a'].includes(key)){e.preventDefault();alert('⚠️ 단축키 금지!');}
});
const socket=io();let currentRoom="",myTurn=false,totalStages=4;
const TIMER_SECONDS={{ turn_seconds }};let timerInterval=null,remaining=TIMER_SECONDS;
function startTimer(){clearInterval(timerInterval);remaining=TIMER_SECONDS;updateTimerText();
  timerInterval=setInterval(()=>{remaining--;updateTimerText();if(remaining<=0){clearInterval(timerInterval);autoSend();}},1000);}
function stopTimer(){clearInterval(timerInterval);}
function updateTimerText(){const m=Math.floor(remaining/60),s=String(remaining%60).padStart(2,'0');
  document.getElementById('turn-status').innerText="⏰ 당신의 발언 차례입니다! (남은 시간 "+m+":"+s+")";}
function autoSend(){const i=document.getElementById('msg-input');if(!i.value.trim())i.value="(시간 초과)";sendMessage();}
function login(){const n=document.getElementById('username').value,p=document.getElementById('pin').value;
  if(!n.trim()||!p.trim())return alert('아이디와 PIN을 입력해주세요.');socket.emit('login',{username:n,pin:p});}
socket.on('login_ok',d=>{
  document.getElementById('login-area').classList.add('hidden');
  document.getElementById('profile-area').classList.remove('hidden');
  document.getElementById('my-name').textContent=d.username;
  document.getElementById('my-stats').textContent="내 전적: "+d.wins+"승 "+d.losses+"패 | 점수: "+d.points+"점 | 현재 "+d.rank+"위";});
socket.on('error_msg',d=>alert(d.msg));
function joinQueue(){socket.emit('join_queue',{});document.getElementById('match-btn').disabled=true;}
socket.on('status',d=>{document.getElementById('queue-text').innerText=d.msg;});
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
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  document.getElementById('role-area').innerHTML="🎭 내 이름: ["+d.my_alias+"] "+sideTag(d.my_side)+"  vs  상대: ["+d.opp_alias+"] "+sideTag(d.opp_side);
  showStage(d.stage,d.stage_index,d.total_stages,true);
  updateTurnUI();});
socket.on('turn_change',d=>{myTurn=d.your_turn;
  showStage(d.stage,d.stage_index,totalStages,d.stage_changed);
  updateTurnUI();});
function updateTurnUI(){const area=document.getElementById('debate-area'),
  input=document.getElementById('msg-input'),btn=document.getElementById('send-btn');
  if(myTurn){area.classList.add('turn-active');input.disabled=false;btn.disabled=false;input.focus();startTimer();}
  else{area.classList.remove('turn-active');document.getElementById('turn-status').innerText="⏳ 상대방 발언 중...";
    input.disabled=true;btn.disabled=true;stopTimer();}}
function sendMessage(){const msg=document.getElementById('msg-input').value;
  if(!msg.trim())return alert('내용을 입력해주세요.');stopTimer();
  socket.emit('send_message',{room_id:currentRoom,message:msg});document.getElementById('msg-input').value="";}
socket.on('receive_message',d=>{const cb=document.getElementById('chat-box');
  const md=document.createElement('div');md.className='msg-item';
  const ne=document.createElement('strong');ne.innerHTML="<span class='stage-label'>["+d.stage+"]</span> "+d.sender+': ';
  const te=document.createElement('span');te.textContent=d.message;
  md.appendChild(ne);md.appendChild(te);cb.appendChild(md);
  const sd=document.createElement('div');sd.className='summary-item';sd.textContent='🤖 AI 요약: '+d.summary;
  cb.appendChild(sd);cb.scrollTop=cb.scrollHeight;});
socket.on('opponent_left',d=>{stopTimer();alert("🚪 상대방이 떠나 몰수승!\\n\\n"+d.reveal);location.reload();});
socket.on('debate_end',d=>{stopTimer();alert("🔔 토론 완료! (+5점)\\n\\n[AI 판정]\\n"+d.result);location.reload();});
</script>
</body></html>"""

ADMIN_TEMPLATE = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>운영자 설정</title>
<script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script>
<style>body{font-family:'Malgun Gothic',sans-serif;max-width:700px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:20px;background:white;margin-bottom:15px;border-radius:12px;}
.profile{padding:10px;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:8px;}
.active{border:2px solid #2563eb;background:#eff6ff;}.ok{color:#16a34a}.bad{color:#dc2626}.msg{color:#2563eb;font-weight:bold;}
table{width:100%;border-collapse:collapse;margin-bottom:14px;font-size:0.9em;}
th{text-align:left;border-bottom:1px solid #e5e7eb;padding:4px;}
td{border-bottom:1px solid #f3f4f6;padding:4px;}
.room-btn{display:block;width:100%;text-align:left;padding:8px;margin-bottom:6px;cursor:pointer;border:1px solid #e5e7eb;border-radius:6px;background:#f9fafb;}
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

<div class="box"><h3>🔴 실시간 관전</h3>
<p>운영자 코드: <input type="password" id="watch-code" placeholder="코드 입력"></p>
<button onclick="loadRooms()" style="padding:8px 15px;">진행 중인 토론 불러오기</button>
<div id="room-list" style="margin-top:10px;"></div>
<div id="watch-chat" class="hidden"></div>
</div>

<p><a href="/">← 토론 화면</a> | <a href="/history">토론 기록</a></p>
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
        btn.textContent=rm.topic+' — '+rm.alias_a+' vs '+rm.alias_b+' ('+rm.stage+')';
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

HISTORY_TEMPLATE = """<!DOCTYPE html>
<html lang="ko"><head><meta charset="UTF-8"><title>토론 기록</title>
<style>body{font-family:'Malgun Gothic',sans-serif;max-width:700px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:16px;background:white;margin-bottom:12px;border-radius:12px;}
.meta{font-size:0.85em;color:#6b7280;}.reason{background:#f9fafb;padding:10px;border-radius:8px;font-size:0.9em;margin-top:8px;white-space:pre-line;}
.log-line{margin:6px 0;font-size:0.92em;}</style></head><body>
<h2>📚 지난 토론 기록 (최근 {{ debates|length }}건)</h2>
{% for d in debates %}
<div class="box"><b>📌 {{ d.topic }}</b>
<p class="meta">{{ d.played_at }} | A: {{ d.player_a }} ({{ d.side_a }}) vs B: {{ d.player_b }} ({{ d.side_b }}) | 엔진: {{ d.engine }}</p>
<p><b>결과: {{ d.winner }}</b></p><div class="reason">{{ d.reason }}</div>
<details><summary>토론 전문 보기</summary>
{% for line in d.logs %}<p class="log-line"><b>[{{ line.stage }}] {{ line.side }}:</b> {{ line.text }}</p>{% endfor %}
</details></div>{% else %}<p>아직 토론 기록이 없습니다.</p>{% endfor %}
<p><a href="/">← 토론 화면</a></p></body></html>"""


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, engine_name=get_active_name(), turn_seconds=TURN_SECONDS)


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
    return render_template_string(ADMIN_TEMPLATE, profiles=profiles_view, users=users_view,
                                  active=get_active_name(), message=message,
                                  providers=PROVIDERS, default_models=DEFAULT_MODELS)


@app.route('/admin/rooms', methods=['POST'])
def admin_rooms():
    data = request.get_json(silent=True) or {}
    if data.get('code') != ADMIN_CODE:
        return jsonify({'error': '운영자 코드가 틀렸습니다.'}), 403
    room_list = []
    for rid, r in rooms.items():
        stage_idx = min(r['turn_count'], TOTAL_STAGES - 1)
        room_list.append({
            'room_id': rid, 'topic': r['topic'],
            'alias_a': r['players'][0]['alias'], 'alias_b': r['players'][1]['alias'],
            'stage': STAGES[stage_idx]
        })
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


def close_room(room_id):
    room = rooms.pop(room_id, None)
    if room:
        for p in room['players']:
            sid_to_room.pop(p['sid'], None)

def make_reveal_text(room):
    a, b = room['players'][0], room['players'][1]
    return f"🎭 정체 공개!\n{a['alias']} ({a['side']}) = {a['username']}\n{b['alias']} ({b['side']}) = {b['username']}"


@socketio.on('login')
def handle_login(data):
    username = (data.get('username') or '').strip()
    pin = (data.get('pin') or '').strip()
    if not username or len(pin) < 4:
        emit('error_msg', {'msg': '아이디를 입력하고 PIN은 4자리 이상으로 해주세요.'})
        return
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
    stats = get_user_stats(username)
    emit('login_ok', {'username': username,
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
    emit('status', {'msg': '대기열에서 상대방 매칭을 기다리는 중...'})
    if len(waiting_pool) >= 2:
        p1 = waiting_pool.pop(random.randint(0, len(waiting_pool) - 1))
        p2 = waiting_pool.pop(random.randint(0, len(waiting_pool) - 1))
        title_a, title_b = random.sample(ALIAS_POOL, 2)
        side_a, side_b = random.sample(["찬성", "반대"], 2)
        p1['alias'] = title_a
        p2['alias'] = title_b
        p1['side'] = side_a
        p2['side'] = side_b
        room_id = f"room_{uuid.uuid4().hex[:8]}"
        topic = get_ai_topic()
        rooms[room_id] = {"players": [p1, p2], "turn_count": 0, "current_speaker": 0, "topic": topic, "logs": []}
        sid_to_room[p1['sid']] = room_id
        sid_to_room[p2['sid']] = room_id
        join_room(room_id, sid=p1['sid'])
        join_room(room_id, sid=p2['sid'])
        emit('match_found', {'room_id': room_id, 'topic': topic, 'your_turn': True,
                             'my_alias': p1['alias'], 'opp_alias': p2['alias'],
                             'my_side': p1['side'], 'opp_side': p2['side'],
                             'stage': STAGES[0], 'stage_index': 0, 'total_stages': TOTAL_STAGES}, room=p1['sid'])
        emit('match_found', {'room_id': room_id, 'topic': topic, 'your_turn': False,
                             'my_alias': p2['alias'], 'opp_alias': p1['alias'],
                             'my_side': p2['side'], 'opp_side': p1['side'],
                             'stage': STAGES[0], 'stage_index': 0, 'total_stages': TOTAL_STAGES}, room=p2['sid'])


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
    stage_name = STAGES[room['turn_count']]
    summary = get_ai_summary(msg_text)
    room['logs'].append({"side": speaker['side'], "stage": stage_name, "text": msg_text})
    emit('receive_message', {'sender': speaker['alias'], 'message': msg_text, 'summary': summary, 'stage': stage_name}, room=room_id)
    emit('admin_message', {'side': speaker['side'], 'message': msg_text, 'stage': stage_name}, room=f"watch_{room_id}")
    stage_completed = (speaker_idx == 1)
    if stage_completed:
        room['turn_count'] += 1
    if room['turn_count'] >= TOTAL_STAGES:
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
        emit('debate_end', {'result': final_text}, room=room_id)
        emit('admin_end', {'result': final_text}, room=f"watch_{room_id}")
        close_room(room_id)
    else:
        room['current_speaker'] = 1 - speaker_idx
        next_idx = room['current_speaker']
        new_stage = STAGES[room['turn_count']]
        for i, p in enumerate(room['players']):
            emit('turn_change', {'your_turn': i == next_idx, 'stage': new_stage,
                                 'stage_index': room['turn_count'], 'stage_changed': stage_completed}, room=p['sid'])
        if stage_completed:
            emit('admin_stage', {'stage': new_stage}, room=f"watch_{room_id}")


@socketio.on('disconnect')
def handle_disconnect(*args):
    sid = request.sid
    sid_to_user.pop(sid, None)
    waiting_pool[:] = [p for p in waiting_pool if p['sid'] != sid]
    room_id = sid_to_room.get(sid)
    if room_id and room_id in rooms:
        room = rooms[room_id]
        leaver = next(p for p in room['players'] if p['sid'] == sid)
        stayer = next(p for p in room['players'] if p['sid'] != sid)
        record_win_loss(stayer['username'], leaver['username'])
        reveal = make_reveal_text(room)
        save_debate(room['topic'], room['players'][0]['username'], room['players'][1]['username'],
                    room['players'][0]['side'], room['players'][1]['side'],
                    room['logs'], "몰수", f"{leaver['username']} 퇴장으로 {stayer['username']} 몰수승\n\n{reveal}")
        emit('opponent_left', {'reveal': reveal}, room=stayer['sid'])
        emit('admin_end', {'result': f"{leaver['username']} 퇴장으로 종료\n\n{reveal}"}, room=f"watch_{room_id}")
        close_room(room_id)


if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
