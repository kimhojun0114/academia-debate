import os
import json
import random
import sqlite3
import uuid
import hashlib
from datetime import datetime
from flask import Flask, render_template_string, request, jsonify
from flask_socketio import SocketIO, emit, join_room

app = Flask(__name__)
app.config['SECRET_KEY'] = 'debate_secret_key_1234'
socketio = SocketIO(app, cors_allowed_origins="*")

waiting_pool = []
rooms = {}
sid_to_room = {}
sid_to_user = {}

TOTAL_TURNS = 5
ADMIN_CODE = os.environ.get("DEBATE_ADMIN_CODE", "1234")
PROFILE_FILE = "api_profiles.json"

ALIAS_POOL = ["너구리", "고래", "부엉이", "수달", "펭귄", "사막여우", "돌고래",
              "호랑이", "판다", "카피바라", "문어", "매", "고슴도치", "알파카",
              "두더지", "수리부엉이", "해달", "스라소니", "꿀벌", "도롱뇽"]

DEFAULT_PROFILES = {
    "active": "테스트 모드",
    "profiles": [
        {"name": "김호준 Claude", "provider": "anthropic", "model": "claude-haiku-4-5",  "api_key": "여기에_키_입력"},
        {"name": "김호준 GPT",    "provider": "openai",    "model": "gpt-4o-mini",       "api_key": "여기에_키_입력"},
        {"name": "서하준 Gemini", "provider": "gemini",    "model": "gemini-2.5-flash",  "api_key": "여기에_키_입력"}
    ]
}

def load_profiles():
    if not os.path.exists(PROFILE_FILE):
        with open(PROFILE_FILE, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_PROFILES, f, ensure_ascii=False, indent=2)
        return dict(DEFAULT_PROFILES)
    try:
        with open(PROFILE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[프로필 파일 오류] {e}")
        return dict(DEFAULT_PROFILES)

def save_profiles(data):
    with open(PROFILE_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

profile_data = load_profiles()

def get_active_profile():
    active = profile_data.get("active", "테스트 모드")
    if active == "테스트 모드":
        return None
    for p in profile_data.get("profiles", []):
        if p["name"] == active:
            if not p.get("api_key") or "여기에" in p["api_key"]:
                return None
            return p
    return None

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

def db_execute(query, params=()):
    conn = sqlite3.connect('league.db')
    cursor = conn.cursor()
    cursor.execute(query, params)
    conn.commit()
    conn.close()

def db_fetchall(query, params=()):
    conn = sqlite3.connect('league.db')
    cursor = conn.cursor()
    cursor.execute(query, params)
    rows = cursor.fetchall()
    conn.close()
    return rows

def db_fetchone(query, params=()):
    rows = db_fetchall(query, params)
    return rows[0] if rows else None

def init_db():
    db_execute('''CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY, pin_hash TEXT,
        wins INTEGER DEFAULT 0, losses INTEGER DEFAULT 0, points INTEGER DEFAULT 1000)''')
    try:
        db_execute("ALTER TABLE users ADD COLUMN pin_hash TEXT")
    except Exception:
        pass
    db_execute('''CREATE TABLE IF NOT EXISTS debates (
        id INTEGER PRIMARY KEY AUTOINCREMENT, played_at TEXT, topic TEXT,
        player_a TEXT, player_b TEXT, log_json TEXT, winner TEXT, reason TEXT, engine TEXT)''')

def hash_pin(username, pin):
    return hashlib.sha256(f"{username}:{pin}:academia".encode()).hexdigest()

def get_user_stats(username):
    row = db_fetchone("SELECT wins, losses, points FROM users WHERE username = ?", (username,))
    if not row:
        return None
    rank_row = db_fetchone("SELECT COUNT(*) FROM users WHERE points > ?", (row[2],))
    return {"wins": row[0], "losses": row[1], "points": row[2], "rank": rank_row[0] + 1}

def add_points(username, n):
    db_execute("UPDATE users SET points = points + ? WHERE username = ?", (n, username))

def record_win_loss(winner, loser):
    db_execute("UPDATE users SET wins = wins + 1, points = points + 20 WHERE username = ?", (winner,))
    db_execute("UPDATE users SET losses = losses + 1, points = points - 15 WHERE username = ?", (loser,))

def save_debate(topic, player_a, player_b, logs, winner, reason):
    engine = profile_data.get("active", "테스트 모드")
    db_execute(
        "INSERT INTO debates (played_at, topic, player_a, player_b, log_json, winner, reason, engine) VALUES (?,?,?,?,?,?,?,?)",
        (datetime.now().strftime("%Y-%m-%d %H:%M"), topic, player_a, player_b,
         json.dumps(logs, ensure_ascii=False), winner, reason, engine))

init_db()

def get_ai_topic():
    result = llm_call("청소년들이 1대1로 토론하기 좋은 찬반 주제 하나만 짧게 제목만 추천해줘. 다른 말 없이 주제만.")
    return result if result else "인공지능 발전은 인간에게 유익한가?"

def get_ai_summary(text):
    result = llm_call(f"다음 토론 발언을 한 문장으로 핵심만 요약해줘:\n{text}", max_tokens=200)
    return result if result else f"[요약] {text[:15]}..."

def judge_debate(topic, logs):
    log_text = "\n".join([f"참가자 {log['role']}: {log['text']}" for log in logs])
    prompt = f"""너는 고등학교 토론 동아리의 심사위원이다. 아래 1대1 토론을 심사 기준에 따라 엄격히 평가하라.

[심사 기준 — 항목당 5점 만점, 총 20점]
1. 논거-증거 연결 2. 반박력 3. 논리 구조 4. 독창성

[주제] {topic}
[토론 로그]
{log_text}

반드시 아래 JSON 형식으로만 답하라.
{{"winner": "A" 또는 "B" 또는 "무승부", "score_a": 총점숫자, "score_b": 총점숫자, "reason": "판정 근거를 3문장 이내로"}}"""
    raw = llm_call(prompt, max_tokens=800)
    if not raw:
        return "무승부", "무승부 (테스트 모드)"
    try:
        data = json.loads(raw.replace("```json", "").replace("```", "").strip())
        winner = data.get("winner", "무승부")
        if winner not in ("A", "B", "무승부"):
            winner = "무승부"
        result_text = (f"판정: {'참가자 ' + winner + ' 승리' if winner != '무승부' else '무승부'}\n"
                       f"점수: A {data.get('score_a','?')}점 vs B {data.get('score_b','?')}점 (20점 만점)\n"
                       f"근거: {data.get('reason','근거 없음')}")
        return winner, result_text
    except Exception as e:
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
</style>
</head>
<body>
<h2>🗣️ 동아리 익명 1대1 토론 시스템</h2>
<p class="engine-tag">🤖 현재 AI 엔진: {{ engine_name }}</p>
<div id="login-area" class="box">
  <p style="font-size:0.9em;color:#555;">자기 아이디로 로그인하세요. 처음이면 자동 가입됩니다.<br>🎭 토론 중에는 익명 이름만 표시됩니다.</p>
  <input type="text" id="username" placeholder="아이디" style="padding:8px;width:40%;">
  <input type="password" id="pin" placeholder="PIN (4자리 이상)" style="padding:8px;width:35%;">
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
document.addEventListener('keydown',e=>{
  const isCtrl=e.ctrlKey||e.metaKey,key=e.key.toLowerCase(),isTyping=['INPUT','TEXTAREA'].includes(e.target.tagName);
  if(isCtrl&&key==='a'&&isTyping)return;
  if(isCtrl&&['c','v','x','a'].includes(key)){e.preventDefault();alert('⚠️ 단축키 금지!');}
});
const socket=io();let currentRoom="",myTurn=false;
const TURN_SECONDS=180;let timerInterval=null,remaining=TURN_SECONDS;
function startTimer(){clearInterval(timerInterval);remaining=TURN_SECONDS;updateTimerText();
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
  document.getElementById('my-stats').textContent="내 전적: "+d.wins+"승 "+d.losses+"패 | 점수: "+d.points+"점 | 현재 "+d.rank+"위";
  if(d.is_new)alert("🎉 신규 가입 완료! PIN을 잊지 마세요.");});
socket.on('error_msg',d=>alert(d.msg));
function joinQueue(){socket.emit('join_queue',{});document.getElementById('match-btn').disabled=true;}
socket.on('status',d=>{document.getElementById('queue-text').innerText=d.msg;});
socket.on('match_found',d=>{currentRoom=d.room_id;myTurn=d.your_turn;
  document.getElementById('profile-area').classList.add('hidden');
  document.getElementById('debate-area').classList.remove('hidden');
  document.getElementById('topic-area').innerText="📌 주제: "+d.topic;
  document.getElementById('role-area').innerText="🎭 내 이름: ["+d.my_alias+"]  vs  상대: ["+d.opp_alias+"]";
  updateTurnUI();});
socket.on('turn_change',d=>{myTurn=d.your_turn;updateTurnUI();});
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
  const ne=document.createElement('strong');ne.textContent=d.sender+': ';
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
<style>body{font-family:'Malgun Gothic',sans-serif;max-width:600px;margin:30px auto;padding:20px;background:#f3f4f6;}
.box{border:1px solid #e5e7eb;padding:20px;background:white;margin-bottom:15px;border-radius:12px;}
.profile{padding:10px;border:1px solid #e5e7eb;border-radius:8px;margin-bottom:8px;}
.active{border:2px solid #2563eb;background:#eff6ff;}.ok{color:#16a34a}.bad{color:#dc2626}.msg{color:#2563eb;font-weight:bold;}</style>
</head><body><h2>🛠️ 운영자 설정</h2>
{% if message %}<p class="msg">{{ message }}</p>{% endif %}
<div class="box"><h3>AI 엔진 선택</h3><p>현재: <b>{{ active }}</b></p>
<p style="font-size:0.85em;color:#666;">키 등록은 api_profiles.json 파일 수정 후 "파일 다시 읽기"</p>
<form method="POST" action="/admin">
{% for p in profiles %}
<div class="profile {% if p.name == active %}active{% endif %}">
<label><input type="radio" name="profile" value="{{ p.name }}" {% if p.name == active %}checked{% endif %}>
<b>{{ p.name }}</b> — {{ p.provider }}/{{ p.model }}
{% if p.key_ok %}<span class="ok">(키 등록됨 ✓)</span>{% else %}<span class="bad">(키 미등록)</span>{% endif %}
</label></div>{% endfor %}
<div class="profile {% if active == '테스트 모드' %}active{% endif %}">
<label><input type="radio" name="profile" value="테스트 모드" {% if active == '테스트 모드' %}checked{% endif %}>
<b>테스트 모드</b> — AI 없이 동작</label></div>
<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="switch" style="padding:8px 15px;">엔진 변경</button>
<button type="submit" name="action" value="reload" style="padding:8px 15px;">파일 다시 읽기</button>
</form></div>
<div class="box"><h3>부원 PIN 초기화</h3>
<form method="POST" action="/admin">
<p>아이디: <input type="text" name="reset_user" placeholder="초기화할 아이디"></p>
<p>운영자 코드: <input type="password" name="code" placeholder="코드 입력"></p>
<button type="submit" name="action" value="resetpin" style="padding:8px 15px;">PIN 초기화 (→ 0000)</button>
</form></div>
<p><a href="/">← 토론 화면</a> | <a href="/history">토론 기록</a></p>
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
<p class="meta">{{ d.played_at }} | A: {{ d.player_a }} vs B: {{ d.player_b }} | 엔진: {{ d.engine }}</p>
<p><b>결과: {{ d.winner }}</b></p><div class="reason">{{ d.reason }}</div>
<details><summary>토론 전문 보기</summary>
{% for line in d.logs %}<p class="log-line"><b>참가자 {{ line.role }}:</b> {{ line.text }}</p>{% endfor %}
</details></div>{% else %}<p>아직 토론 기록이 없습니다.</p>{% endfor %}
<p><a href="/">← 토론 화면</a></p></body></html>"""

@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE, engine_name=profile_data.get("active", "테스트 모드"))

@app.route('/admin', methods=['GET', 'POST'])
def admin():
    global profile_data
    message = None
    if request.method == 'POST':
        code = request.form.get('code', '')
        action = request.form.get('action', '')
        if code != ADMIN_CODE:
            message = "❌ 운영자 코드가 틀렸습니다."
        elif action == 'reload':
            profile_data = load_profiles()
            message = "🔄 파일을 다시 읽었습니다."
        elif action == 'switch':
            chosen = request.form.get('profile', '테스트 모드')
            valid_names = [p['name'] for p in profile_data.get('profiles', [])] + ['테스트 모드']
            if chosen in valid_names:
                profile_data['active'] = chosen
                save_profiles(profile_data)
                message = f"✅ '{chosen}'으로 변경되었습니다."
            else:
                message = "❌ 존재하지 않는 프로필입니다."
        elif action == 'resetpin':
            target = (request.form.get('reset_user') or '').strip()
            row = db_fetchone("SELECT username FROM users WHERE username = ?", (target,))
            if row:
                db_execute("UPDATE users SET pin_hash = ? WHERE username = ?", (hash_pin(target, "0000"), target))
                message = f"✅ '{target}'의 PIN이 0000으로 초기화되었습니다."
            else:
                message = f"❌ '{target}' 아이디를 찾을 수 없습니다."
    profiles_view = [{"name": p["name"], "provider": p["provider"], "model": p["model"],
                      "key_ok": bool(p.get("api_key")) and "여기에" not in p.get("api_key", "")}
                     for p in profile_data.get("profiles", [])]
    return render_template_string(ADMIN_TEMPLATE, profiles=profiles_view,
                                  active=profile_data.get("active", "테스트 모드"), message=message)

@app.route('/history')
def history():
    rows = db_fetchall("SELECT played_at, topic, player_a, player_b, log_json, winner, reason, engine FROM debates ORDER BY id DESC LIMIT 30")
    debates = []
    for r in rows:
        try:
            logs = json.loads(r[4])
        except Exception:
            logs = []
        debates.append({"played_at": r[0], "topic": r[1], "player_a": r[2], "player_b": r[3],
                         "logs": logs, "winner": r[5], "reason": r[6], "engine": r[7]})
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
    return f"🎭 정체 공개!\n{a['alias']} = {a['username']}\n{b['alias']} = {b['username']}"

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
    row = db_fetchone("SELECT pin_hash FROM users WHERE username = ?", (username,))
    is_new = False
    if not row:
        db_execute("INSERT INTO users (username, pin_hash) VALUES (?, ?)", (username, hash_pin(username, pin)))
        is_new = True
    elif row[0] is None:
        db_execute("UPDATE users SET pin_hash = ? WHERE username = ?", (hash_pin(username, pin), username))
    elif row[0] != hash_pin(username, pin):
        emit('error_msg', {'msg': 'PIN이 틀렸습니다.'})
        return
    sid_to_user[request.sid] = username
    stats = get_user_stats(username)
    emit('login_ok', {'username': username, 'is_new': is_new,
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
        animal_a, animal_b = random.sample(ALIAS_POOL, 2)
        p1['alias'] = f"익명의 {animal_a}"
        p2['alias'] = f"익명의 {animal_b}"
        room_id = f"room_{uuid.uuid4().hex[:8]}"
        topic = get_ai_topic()
        rooms[room_id] = {"players": [p1, p2], "turn_count": 0, "current_speaker": 0, "topic": topic, "logs": []}
        sid_to_room[p1['sid']] = room_id
        sid_to_room[p2['sid']] = room_id
        join_room(room_id, sid=p1['sid'])
        join_room(room_id, sid=p2['sid'])
        emit('match_found', {'room_id': room_id, 'topic': topic, 'your_turn': True,
                             'my_alias': p1['alias'], 'opp_alias': p2['alias']}, room=p1['sid'])
        emit('match_found', {'room_id': room_id, 'topic': topic, 'your_turn': False,
                             'my_alias': p2['alias'], 'opp_alias': p1['alias']}, room=p2['sid'])

@socketio.on('send_message')
def handle_send_message(data):
    room_id = data.get('room_id')
    msg_text = (data.get('message') or '').strip()
    room = rooms.get(room_id)
    if not room or not msg_text:
        return
    speaker = room['players'][room['current_speaker']]
    if request.sid != speaker['sid']:
        emit('error_msg', {'msg': '지금은 당신의 발언 차례가 아닙니다.'})
        return
    role = 'A' if room['current_speaker'] == 0 else 'B'
    summary = get_ai_summary(msg_text)
    room['logs'].append({"role": role, "text": msg_text})
    emit('receive_message', {'sender': speaker['alias'], 'message': msg_text, 'summary': summary}, room=room_id)
    if role == 'B':
        room['turn_count'] += 1
    if room['turn_count'] >= TOTAL_TURNS:
        winner, result_text = judge_debate(room['topic'], room['logs'])
        player_a, player_b = room['players'][0], room['players'][1]
        add_points(player_a['username'], 5)
        add_points(player_b['username'], 5)
        if winner == "A":
            record_win_loss(player_a['username'], player_b['username'])
        elif winner == "B":
            record_win_loss(player_b['username'], player_a['username'])
        final_text = result_text + "\n\n" + make_reveal_text(room)
        save_debate(room['topic'], player_a['username'], player_b['username'], room['logs'], winner, final_text)
        emit('debate_end', {'result': final_text}, room=room_id)
        close_room(room_id)
    else:
        room['current_speaker'] = 1 - room['current_speaker']
        p1_turn = (room['current_speaker'] == 0)
        emit('turn_change', {'your_turn': p1_turn}, room=room['players'][0]['sid'])
        emit('turn_change', {'your_turn': not p1_turn}, room=room['players'][1]['sid'])

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
                    room['logs'], "몰수", f"{leaver['username']} 퇴장으로 {stayer['username']} 몰수승\n\n{reveal}")
        emit('opponent_left', {'reveal': reveal}, room=stayer['sid'])
        close_room(room_id)

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    socketio.run(app, host='0.0.0.0', port=port, allow_unsafe_werkzeug=True)
