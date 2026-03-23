import os
import json
import time
import asyncio
import websockets
import html as html_escape
from datetime import datetime
from contextlib import asynccontextmanager
import psycopg2
from psycopg2 import pool
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

PGHOST = os.environ.get("PGHOST", "")
PGPORT = os.environ.get("PGPORT", "")
PGUSER = os.environ.get("PGUSER", "")
PGPASSWORD = os.environ.get("PGPASSWORD", "")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "")

db_pool = pool.SimpleConnectionPool(
    1, 20,
    dbname=POSTGRES_DB,
    user=PGUSER,
    password=PGPASSWORD,
    host=PGHOST,
    port=PGPORT
)

TOKEN = os.getenv("TOKEN", "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIifQ.TuitMgjF9S7JE3kkD65lG1PfJe-1oJTGstc9BCVDczs")
active_connections = set()
history = []

cache_data = None
cache_time = 0
CACHE_TTL = 1

chat_cache = {}
chat_cache_time = {}
CHAT_CACHE_TTL = 2

def get_db():
    return db_pool.getconn()

def put_db(conn):
    db_pool.putconn(conn)

def get_request():
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT data FROM request ORDER BY updated_at DESC LIMIT 1")
    row = c.fetchone()
    c.close()
    put_db(conn)
    if row:
        return row[0]
    return None

async def indodax_ws_listener():
    uri = "wss://chat-ws.indodax.com/ws/"
    while True:
        try:
            async with websockets.connect(uri) as ws:
                await ws.send(json.dumps({"params": {"token": TOKEN}, "id": 6}))
                await ws.recv()
                await ws.send(json.dumps({"method": 1, "params": {"channel": "chatroom_indodax"}, "id": 7}))
                await ws.recv()
                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                        if data.get("result", {}).get("channel") == "chatroom_indodax":
                            chat = data["result"]["data"]["data"]
                            WIB_OFFSET = 7 * 3600
                            ts = chat["timestamp"]
                            chat["timestamp_wib"] = datetime.utcfromtimestamp(ts + WIB_OFFSET).strftime('%Y-%m-%d %H:%M:%S')
                            history.append(chat)
                            history[:] = history[-1000:]
                            for client in list(active_connections):
                                try:
                                    await client.send_text(json.dumps({"new": [chat]}))
                                except Exception:
                                    active_connections.discard(client)
                    except Exception as e:
                        print("Error parsing/broadcast:", e)
        except Exception as e:
            print("WebSocket Indodax error, reconnecting in 5s:", e)
            await asyncio.sleep(5)

@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(indodax_ws_listener())
    yield
    task.cancel()

app = FastAPI(lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
def index():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Ranking Chat Indodax</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 30px; }
            table.dataTable thead th { font-weight: bold; }
            .btn-history { display:inline-block; margin-bottom:20px; padding:8px 16px; background:#00abff; color:#fff; border:none; border-radius:4px; text-decoration:none;}
            .btn-history:hover { background:#0056b3; }
            .cusername {
                font-weight: bold;
                cursor: pointer;
                text-decoration: underline;
            }
            .cusername:hover { opacity: 0.7; }
            .clastchat {
                font-weight: bold;
            }
            .level-0 { color: #5E5E5E; }
            .level-1 { color: #5B2D00; }
            .level-2 { color: #FF8200; text-shadow: 1px 1px #CCCCCC; }
            .level-3 { color: #034AC4; text-shadow: 1px 1px #CCCCCC; }
            .level-4 { color: #00E124; text-shadow: 1px 1px #00B31C; }
            .level-5 { color: #B232B2 }
            th, td { vertical-align: top; }
            th:nth-child(1), td:nth-child(1) { width: 10px; min-width: 10px; max-width: 20px; white-space: nowrap; }
            th:nth-child(2), td:nth-child(2) { width: 120px; min-width: 90px; max-width: 150px; white-space: nowrap; }
            th:nth-child(3), td:nth-child(3) { width: 10px; min-width: 10px; max-width: 35px; white-space: nowrap; }
            th:nth-child(4), td:nth-child(4) { width: auto; word-break: break-word; white-space: pre-line; }
            th:nth-child(5), td:nth-child(5) { width: 130px; min-width: 110px; max-width: 150px; white-space: nowrap; }
        </style>
    </head>
    <body>
    <h2>Top Chatroom Indodax</h2>
    <a href="/cr" class="btn-history">Chat Terkini</a>
    <table id="ranking" class="display" style="width:100%">
        <thead>
        <tr>
            <th>No</th>
            <th>Username</th>
            <th>Total</th>
            <th>Terakhir Chat</th>
            <th>Waktu Chat</th>
        </tr>
        </thead>
        <tbody></tbody>
    </table>
    <p id="periode"></p>
    <script>
        var table = $('#ranking').DataTable({
            "order": [[2, "desc"]],
            "paging": false,
            "info": false,
            "searching": true,
            "language": {"emptyTable": "Tidak ada DATA"}
        });
        $(document).on('click', '#ranking tbody .cusername', function() {
            var username = $(this).text().trim();
            window.location.href = "/user/" + encodeURIComponent(username);
        });
        function loadData() {
            $.getJSON("/data", function(data) {
                table.clear();
                if (data.ranking.length === 0) {
                    $("#periode").html("<b>Tidak ada DATA</b>");
                    table.draw();
                    return;
                }
                for (var i = 0; i < data.ranking.length; i++) {
                    var row = data.ranking[i];
                    table.row.add([
                        i+1,
                        '<span class="level-' + row.level + ' cusername">' + row.username + '</span>',
                        row.count,
                        '<span class="level-' + row.level + ' clastchat">' + row.last_content + '</span>',
                        row.last_time
                    ]);
                }
                table.draw();
                $("#periode").html("Periode: <b>" + data.t_awal + "</b> s/d <b>" + data.t_akhir + "</b>");
            });
        }
        loadData();
        setInterval(loadData, 1000);
    </script>
    </body>
    </html>
    """
    return html

@app.get("/cr", response_class=HTMLResponse)
async def websocket_page():
    html = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Chatroom Indodax </title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <style>
            body { font-family: Arial, sans-serif; margin: 8px; padding: 0; }
            table.dataTable thead th { font-weight: bold; border-bottom: 2px solid #ddd; }
            table.dataTable { border-bottom: 2px solid #ddd; }
            .cusername {
                color: #A0B6C8;
                font-weight: bold;
                cursor: pointer;
            }
            .level-0 { color: #5E5E5E; }
            .level-1 { color: #5B2D00; }
            .level-2 { color: #FF8200; text-shadow: 1px 1px #CCCCCC; }
            .level-3 { color: #034AC4; text-shadow: 1px 1px #CCCCCC; }
            .level-4 { color: #00E124; text-shadow: 1px 1px #00B31C; }
            .level-5 { color: #B232B2 }
            .chat{ color: #000; text-shadow: none;}
            th, td { vertical-align: top; }
            th:nth-child(1), td:nth-child(1) { width: 130px; min-width: 110px; max-width: 150px; white-space: nowrap; }
            th:nth-child(2), td:nth-child(2) { width: 120px; min-width: 90px; max-width: 160px; white-space: nowrap; }
            th:nth-child(3), td:nth-child(3) { width: auto; word-break: break-word; white-space: pre-line; }
            .header-chatroom { display: flex; align-items: center; justify-content: flex-start; gap: 20px; margin-top: 0; margin-left: 10px; padding-top: 0; }
            .header-chatroom a {color: red;}
        </style>
    </head>
    <body>
    <div class="header-chatroom">
        <h2>Chatroom Indodax</h2>
        <a>* Maksimal 1000 chat terakhir</a>
    </div>
    <table id="history" class="display" style="width:100%">
        <thead>
            <tr>
                <th>Waktu</th>
                <th>Username</th>
                <th>Chat</th>
                <th style="display:none;">Timestamp</th>
                <th style="display:none;">ID</th>
                <th style="display:none;">SortKey</th>
            </tr>
        </thead>
        <tbody></tbody>
    </table>
    <script>
        var table = $('#history').DataTable({
            "ordering": false,
            "paging": false,
            "info": false,
            "searching": true,
            "columnDefs": [
                { "targets": [3,4,5], "visible": false }
            ],
            "language": {
                "emptyTable": "Belum ada chat"
            }
        });

        let allChats = [];

        function updateTable(newChats) {
            newChats.forEach(function(chat) {
                if (!allChats.some(c => c.id === chat.id)) {
                    chat.sort_key = chat.timestamp * 1000000 + chat.id;
                    allChats.push(chat);
                }
            });
            allChats.sort(function(a, b) {
                return b.sort_key - a.sort_key;
            });
            table.clear();
            allChats.forEach(function(chat) {
                var level = chat.level || 0;
                var row = [
                    chat.timestamp_wib || "",
                    '<span class="level-' + level + ' cusername ">' + (chat.username || "") + '</span>',
                    '<span class="level-' + level + ' chat ">' + (chat.content || "") + '</span>',
                    chat.timestamp,
                    chat.id,
                    chat.sort_key
                ];
                table.row.add(row);
            });
            table.draw(false);
        }

        function connectWS() {
            var ws = new WebSocket((location.protocol === "https:" ? "wss://" : "ws://") + location.host + "/ws");
            ws.onmessage = function(event) {
                var data = JSON.parse(event.data);
                if (data.history) {
                    allChats = [];
                    updateTable(data.history);
                } else if (data.new) {
                    updateTable(data.new);
                }
            };
            ws.onclose = function() {
                setTimeout(connectWS, 1000);
            };
        }
        connectWS();
    </script>
    </body>
    </html>
    """
    return HTMLResponse(html)

@app.get("/user/{username}", response_class=HTMLResponse)
def user_chat_page(username: str):
    safe_username = html_escape.escape(username)
    json_username = json.dumps(username)
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Chat dari {safe_username}</title>
        <link rel="stylesheet" type="text/css" href="https://cdn.datatables.net/1.13.6/css/jquery.dataTables.min.css"/>
        <script src="https://code.jquery.com/jquery-3.7.0.min.js"></script>
        <script src="https://cdn.datatables.net/1.13.6/js/jquery.dataTables.min.js"></script>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 30px; }}
            table.dataTable thead th {{ font-weight: bold; }}
            .btn-back {{ display:inline-block; margin-bottom:20px; padding:8px 16px; background:#00abff; color:#fff; border:none; border-radius:4px; text-decoration:none;}}
            .btn-back:hover {{ background:#0056b3; }}
            .level-0 {{ color: #5E5E5E; font-weight: bold; }}
            .level-1 {{ color: #5B2D00; font-weight: bold; }}
            .level-2 {{ color: #FF8200; text-shadow: 1px 1px #CCCCCC; font-weight: bold; }}
            .level-3 {{ color: #034AC4; text-shadow: 1px 1px #CCCCCC; font-weight: bold; }}
            .level-4 {{ color: #00E124; text-shadow: 1px 1px #00B31C; font-weight: bold; }}
            .level-5 {{ color: #B232B2; font-weight: bold; }}
            th, td {{ vertical-align: top; }}
            th:nth-child(1), td:nth-child(1) {{ width: 10px; min-width: 10px; max-width: 20px; white-space: nowrap; }}
            th:nth-child(2), td:nth-child(2) {{ width: 130px; min-width: 110px; max-width: 160px; white-space: nowrap; }}
            th:nth-child(3), td:nth-child(3) {{ width: 100px; min-width: 80px; max-width: 150px; white-space: nowrap; }}
            th:nth-child(4), td:nth-child(4) {{ width: auto; word-break: break-word; white-space: pre-line; }}
        </style>
    </head>
    <body>
    <h2>Chat dari: {safe_username}</h2>
    <a href="/" class="btn-back">&#8592; Kembali ke Ranking</a>
    <p id="info">Memuat data...</p>
    <table id="chatTable" class="display" style="width:100%">
        <thead>
        <tr>
            <th>No</th>
            <th>Waktu</th>
            <th>Username</th>
            <th>Isi Chat</th>
        </tr>
        </thead>
        <tbody></tbody>
    </table>
    <script>
        var chatTable = $('#chatTable').DataTable({{
            "order": [[1, "desc"]],
            "paging": true,
            "lengthMenu": [[48, 88, 888, -1], [48, 88, 888, "Semua"]],
            "pageLength": 48,
            "info": true,
            "searching": true,
            "language": {{
                "emptyTable": "Tidak ada chat ditemukan",
                "info": "Menampilkan _START_ - _END_ dari _TOTAL_ chat",
                "lengthMenu": "Tampilkan _MENU_ data",
                "paginate": {{ "previous": "Prev", "next": "Next" }},
                "search": "Cari:"
            }}
        }});
        var targetUser = {json_username};
        $.ajax({{
            url: "/chat_detail",
            data: {{ username: targetUser }},
            dataType: "json",
            timeout: 15000,
            success: function(data) {{
                chatTable.clear();
                if (!data.chats || data.chats.length === 0) {{
                    $("#info").html("Total chat: <b>0</b>");
                    chatTable.draw();
                    return;
                }}
                $("#info").html("Total chat: <b>" + data.chats.length + "</b> &nbsp;|&nbsp; Periode: <b>" + data.t_awal + "</b> s/d <b>" + data.t_akhir + "</b>");
                for (var i = 0; i < data.chats.length; i++) {{
                    var chat = data.chats[i];
                    chatTable.row.add([
                        i + 1,
                        chat.timestamp,
                        '<span class="level-' + chat.level + '">' + chat.username + '</span>',
                        chat.content
                    ]);
                }}
                chatTable.draw();
            }},
            error: function(xhr, status, error) {{
                $("#info").html('<span style="color:red;">Gagal memuat data: ' + status + '</span>');
            }}
        }});
    </script>
    </body>
    </html>
    """
    return html

@app.get("/data")
def data():
    global cache_data, cache_time
    now = time.time()
    if cache_data and now - cache_time < CACHE_TTL:
        return cache_data

    req = get_request()
    if not req:
        result = {"ranking": [], "t_awal": "-", "t_akhir": "-"}
        cache_data = result
        cache_time = now
        return result

    t_awal = req.get("start", "-")
    t_akhir = req.get("end", "-")
    usernames = [u.lower() for u in req.get("usernames", [])]
    usernames_filter = [u.lower() for u in req.get("usernames", [])]
    mode = req.get("mode", "")
    kata = req.get("kata", None)
    level = req.get("level", None)

    conn = get_db()
    c = conn.cursor()
    query = "SELECT username, content, timestamp_wib, level FROM chat WHERE timestamp_wib >= %s AND timestamp_wib <= %s"
    params = [t_awal, t_akhir]
    if kata:
        query += " AND LOWER(content) LIKE %s"
        params.append(f"%{kata}%")
    if mode == "username" and usernames:
        query += " AND LOWER(username) = ANY(%s)"
        params.append(usernames)
    if mode == "level" and level is not None:
        query += " AND level = %s"
        params.append(level)
    c.execute(query, params)
    rows = c.fetchall()
    c.close()
    put_db(conn)

    user_info = {}
    for row in rows:
        uname = row[0]
        uname_lower = uname.lower()
        content = row[1]
        t_chat = row[2]
        level = row[3]
        if uname_lower not in user_info:
            user_info[uname_lower] = {
                "username": uname,
                "count": 1,
                "last_content": content,
                "last_time": t_chat,
                "level": level
            }
        else:
            user_info[uname_lower]["count"] += 1
            if t_chat > user_info[uname_lower]["last_time"]:
                user_info[uname_lower]["last_content"] = content
                user_info[uname_lower]["last_time"] = t_chat
                user_info[uname_lower]["level"] = level

    if mode == "username" and usernames:
        ranking = []
        for u in usernames_filter:
            if u in user_info:
                ranking.append((user_info[u]["username"], user_info[u]))
            else:
                ranking.append((u, {"username": u, "count": 0, "last_content": "-", "last_time": "-", "level": 0}))
        ranking = sorted(ranking, key=lambda x: (-x[1]["count"], x[1]["last_time"]))
    else:
        ranking = sorted(
            [(info["username"], info) for info in user_info.values()],
            key=lambda x: (-x[1]["count"], x[1]["last_time"])
        )

    data_result = []
    for user, info in ranking:
        data_result.append({
            "username": user,
            "count": info["count"],
            "last_content": info["last_content"],
            "last_time": info["last_time"],
            "level": info.get("level", 0)
        })
    result = {"ranking": data_result, "t_awal": t_awal, "t_akhir": t_akhir}
    cache_data = result
    cache_time = now
    return result

@app.get("/chat_detail")
def chat_detail(username: str):
    global chat_cache, chat_cache_time
    now = time.time()
    cache_key = username.lower()
    if cache_key in chat_cache and now - chat_cache_time.get(cache_key, 0) < CHAT_CACHE_TTL:
        return chat_cache[cache_key]

    req = get_request()
    if not req:
        return {"chats": [], "t_awal": "-", "t_akhir": "-"}

    t_awal = req.get("start", "-")
    t_akhir = req.get("end", "-")
    kata = req.get("kata", None)

    conn = get_db()
    c = conn.cursor()
    query = "SELECT username, content, timestamp_wib, level FROM chat WHERE timestamp_wib >= %s AND timestamp_wib <= %s AND LOWER(username) = LOWER(%s)"
    params = [t_awal, t_akhir, username]
    if kata:
        query += " AND LOWER(content) LIKE %s"
        params.append(f"%{kata}%")
    query += " ORDER BY timestamp_wib DESC"
    c.execute(query, params)
    rows = c.fetchall()
    c.close()
    put_db(conn)

    chats = []
    for row in rows:
        chats.append({
            "username": row[0],
            "content": row[1],
            "timestamp": row[2],
            "level": row[3]
        })

    result = {"chats": chats, "t_awal": t_awal, "t_akhir": t_akhir}
    chat_cache[cache_key] = result
    chat_cache_time[cache_key] = now
    return result

@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    active_connections.add(websocket)
    try:
        await websocket.send_text(json.dumps({"history": history[-1000:]}))
        while True:
            await asyncio.sleep(30)
            await websocket.send_text(json.dumps({"ping": True}))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print("WebSocket error:", e)
    finally:
        active_connections.discard(websocket)
