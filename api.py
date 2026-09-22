#!/usr/bin/env python3
"""
TempMail API + Webhook Receiver
==========================

FOR RAILWAY HOBBY PLAN (SMTP blocked):
  Use webhook endpoints to receive emails from mail relay services like:
  - Mailgun (free tier: 5,000 emails/mo)
  - Postmark (free tier: 100 emails/mo)
  - SendGrid Inbound Parse (free tier)
  - AWS SES + SNS
  - Cloudflare Email Routing

FOR RAILWAY PRO/ENTERPRISE OR VPS:
  Use smtp_server.py directly - ports are unblocked.

This API serves BOTH the REST API and webhook endpoints.
"""

import os
import uuid
import json
import re
import hmac
import hashlib
import base64
from datetime import datetime, timezone
from typing import Optional, List
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel
import asyncpg
import asyncio

# ==================== CONFIG ====================
DB_URL = os.getenv("DATABASE_URL")
EMAIL_EXPIRY = int(os.getenv("EMAIL_EXPIRY_SECONDS", "300"))
DOMAIN = os.getenv("MAIL_DOMAIN", "yourdomain.com")

# Webhook secrets (set these in your mail service dashboard)
MAILGUN_API_KEY = os.getenv("MAILGUN_API_KEY", "")
POSTMARK_SECRET = os.getenv("POSTMARK_SECRET", "")
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

if not DB_URL:
    raise ValueError("DATABASE_URL environment variable is required!")

# ==================== DB SETUP ====================
db_pool = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    db_pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS emails (
                id UUID PRIMARY KEY,
                recipient TEXT NOT NULL,
                sender TEXT,
                subject TEXT,
                body_text TEXT,
                body_html TEXT,
                raw_content TEXT,
                attachments JSONB DEFAULT '[]',
                received_at DOUBLE PRECISION NOT NULL,
                expires_at DOUBLE PRECISION NOT NULL
            )
        """)
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_recipient ON emails(recipient)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON emails(expires_at)")
    yield
    await db_pool.close()

# ==================== APP ====================
app = FastAPI(title="TempMail API", version="2.0.0", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

# ==================== WEBSOCKET MANAGER ====================
class WSManager:
    def __init__(self):
        self.connections = {}

    async def connect(self, ws: WebSocket, email: str):
        await ws.accept()
        self.connections.setdefault(email, []).append(ws)

    def disconnect(self, ws: WebSocket, email: str):
        if email in self.connections:
            self.connections[email] = [c for c in self.connections[email] if c != ws]

    async def notify(self, email: str, data: dict):
        if email not in self.connections:
            return
        dead = []
        for ws in self.connections[email]:
            try:
                await ws.send_json(data)
            except:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws, email)

ws_manager = WSManager()

# ==================== MODELS ====================
class EmailAddr(BaseModel):
    email: str
    expires_in: int
    created_at: float

class EmailMsg(BaseModel):
    id: str
    sender: str
    subject: str
    body_text: Optional[str]
    body_html: Optional[str]
    attachments: List[dict]
    received_at: float
    expires_in: int

# ==================== CORE FUNCTIONS ====================
async def store_email(recipient: str, sender: str, subject: str, body_text: str, body_html: str, raw: str, attachments: list):
    """Store email and notify WebSocket clients"""
    global db_pool
    if not db_pool:
        db_pool = await asyncpg.create_pool(DB_URL, min_size=5, max_size=20)
    
    eid = str(uuid.uuid4())
    now = datetime.now(timezone.utc).timestamp()
    expires = now + EMAIL_EXPIRY

    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO emails (id, recipient, sender, subject, body_text, body_html, raw_content, attachments, received_at, expires_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
        """, eid, recipient.lower(), sender, subject, body_text, body_html, raw, json.dumps(attachments), now, expires)

    # Notify WebSocket clients with the complete message so the inbox can
    # update immediately without requiring a manual refresh.
    await ws_manager.notify(recipient.lower(), {
        "type": "new_email",
        "id": eid,
        "sender": sender,
        "subject": subject,
        "body_text": body_text,
        "body_html": body_html,
        "attachments": attachments,
        "body_preview": (body_text or body_html or "")[:200],
        "received_at": now,
        "expires_in": EMAIL_EXPIRY
    })

    return eid

async def cleanup_expired():
    while True:
        await asyncio.sleep(30)
        try:
            global db_pool
            if db_pool:
                now = datetime.now(timezone.utc).timestamp()
                async with db_pool.acquire() as conn:
                    await conn.execute("DELETE FROM emails WHERE expires_at < $1", now)
        except:
            pass

# Start cleanup on startup
@app.on_event("startup")
async def start_cleanup():
    asyncio.create_task(cleanup_expired())

# ==================== API ENDPOINTS ====================

@app.get("/", response_class=HTMLResponse)
async def root():
    return await web_ui()

@app.get("/api/generate")
async def generate():
    username = str(uuid.uuid4()).replace("-", "")[:12]
    email = f"{username}@{DOMAIN}"
    return EmailAddr(email=email, expires_in=EMAIL_EXPIRY, created_at=datetime.now(timezone.utc).timestamp())

@app.get("/api/generate/{custom}")
async def generate_custom(custom: str):
    if not re.match(r'^[a-zA-Z0-9._-]+$', custom) or len(custom) > 30:
        raise HTTPException(400, "Invalid username")
    return EmailAddr(email=f"{custom}@{DOMAIN}", expires_in=EMAIL_EXPIRY, created_at=datetime.now(timezone.utc).timestamp())

@app.get("/api/inbox/{email}")
async def inbox(email: str):
    now = datetime.now(timezone.utc).timestamp()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, sender, subject, body_text, body_html, attachments, received_at, expires_at
            FROM emails WHERE recipient = $1 AND expires_at > $2 ORDER BY received_at DESC
        """, email.lower(), now)

    messages = []
    for r in rows:
        messages.append({
            "id": str(r["id"]), "sender": r["sender"] or "Unknown", "subject": r["subject"] or "No Subject",
            "body_text": r["body_text"], "body_html": r["body_html"],
            "attachments": json.loads(r["attachments"]) if r["attachments"] else [],
            "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
        })

    return {"email": email, "messages": messages, "count": len(messages)}

@app.get("/api/message/{msg_id}")
async def get_msg(msg_id: str):
    now = datetime.now(timezone.utc).timestamp()
    async with db_pool.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM emails WHERE id = $1 AND expires_at > $2", msg_id, now)
    if not r:
        raise HTTPException(404, "Not found or expired")
    return {
        "id": str(r["id"]), "recipient": r["recipient"], "sender": r["sender"], "subject": r["subject"],
        "body_text": r["body_text"], "body_html": r["body_html"], "raw_content": r["raw_content"],
        "attachments": json.loads(r["attachments"]) if r["attachments"] else [],
        "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
    }

@app.delete("/api/message/{msg_id}")
async def del_msg(msg_id: str):
    async with db_pool.acquire() as conn:
        result = await conn.execute("DELETE FROM emails WHERE id = $1", msg_id)
    if result == "DELETE 0":
        raise HTTPException(404, "Not found")
    return {"deleted": msg_id}

@app.get("/api/stats")
async def stats():
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM emails")
        active = await conn.fetchval("SELECT COUNT(*) FROM emails WHERE expires_at > $1", datetime.now(timezone.utc).timestamp())
    return {"total": total, "active": active, "expired": total - active, "domain": DOMAIN}

# ==================== WEBSOCKET ====================
@app.websocket("/ws/{email}")
async def ws_endpoint(ws: WebSocket, email: str):
    await ws_manager.connect(ws, email.lower())
    try:
        # Send existing messages
        now = datetime.now(timezone.utc).timestamp()
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, sender, subject, body_text, body_html, attachments, received_at, expires_at
                FROM emails WHERE recipient = $1 AND expires_at > $2 ORDER BY received_at DESC
            """, email.lower(), now)
        for r in rows:
            await ws.send_json({
                "type": "existing", "id": str(r["id"]), "sender": r["sender"],
                "subject": r["subject"], "body_text": r["body_text"],
                "body_html": r["body_html"],
                "attachments": json.loads(r["attachments"]) if r["attachments"] else [],
                "body_preview": (r["body_text"] or r["body_html"] or "")[:200],
                "received_at": r["received_at"], "expires_in": int(r["expires_at"] - now)
            })
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        ws_manager.disconnect(ws, email.lower())
    except:
        ws_manager.disconnect(ws, email.lower())

# ==================== WEBHOOK ENDPOINTS ====================
MAX_ATTACHMENT_BYTES = 12 * 1024 * 1024

@app.post("/webhook/mailgun")
async def webhook_mailgun(request: Request):
    form = await request.form()
    # Verify signature if key exists
    if MAILGUN_API_KEY:
        timestamp = form.get("timestamp")
        token = form.get("token")
        signature = form.get("signature")
        hmac_digest = hmac.new(
            key=MAILGUN_API_KEY.encode(),
            msg=f"{timestamp}{token}".encode(),
            digestmod=hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(signature, hmac_digest):
            raise HTTPException(401, "Invalid signature")

    await store_email(
        recipient=form.get("recipient"),
        sender=form.get("from"),
        subject=form.get("subject"),
        body_text=form.get("body-plain", ""),
        body_html=form.get("body-html", ""),
        raw=json.dumps(dict(form)),
        attachments=[] # Mailgun attachments need separate handling
    )
    return {"status": "ok"}

@app.post("/webhook/postmark")
async def webhook_postmark(request: Request):
    data = await request.json()
    await store_email(
        recipient=data.get("To"),
        sender=data.get("From"),
        subject=data.get("Subject"),
        body_text=data.get("TextBody", ""),
        body_html=data.get("HtmlBody", ""),
        raw=json.dumps(data),
        attachments=data.get("Attachments", [])
    )
    return {"status": "ok"}

@app.post("/webhook/generic")
async def webhook_generic(request: Request, secret: Optional[str] = None):
    if secret and secret != WEBHOOK_SECRET:
        raise HTTPException(401, "Invalid secret")
    data = await request.json()
    await store_email(
        recipient=data.get("to") or data.get("recipient"),
        sender=data.get("from") or data.get("sender"),
        subject=data.get("subject"),
        body_text=data.get("body_text") or data.get("text", ""),
        body_html=data.get("body_html") or data.get("html", ""),
        raw=json.dumps(data),
        attachments=data.get("attachments", [])
    )
    return {"status": "ok"}

@app.post("/webhook/raw")
async def webhook_raw(request: Request, secret: Optional[str] = None):
    """Handle raw email content (ideal for Cloudflare Workers)"""
    # Cloudflare sends the shared secret in X-Secret. Do not allow an
    # unauthenticated request when WEBHOOK_SECRET is missing or malformed.
    header_secret = request.headers.get("X-Secret", "")
    provided_secret = secret or header_secret
    if not WEBHOOK_SECRET or not hmac.compare_digest(provided_secret, WEBHOOK_SECRET):
        raise HTTPException(401, "Invalid secret")
            
    import email
    from email import policy
    
    data = await request.json()
    raw_content = data.get("raw", "")
    if not raw_content:
        raise HTTPException(400, "No raw content provided")
        
    msg = email.message_from_string(raw_content, policy=policy.default)
    
    subject = msg.get("Subject", "No Subject")
    sender = msg.get("From", data.get("from", "Unknown"))
    recipient = data.get("to") or msg.get("To") or "unknown@domain.com"
    
    body_text = ""
    body_html = ""
    attachments = []
    inline_images = {}
    
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            cd = str(part.get("Content-Disposition", ""))
            payload = part.get_payload(decode=True) or b""
            filename = part.get_filename()
            content_id = (part.get("Content-ID") or "").strip("<>")
            is_attachment = bool(filename or "attachment" in cd.lower() or content_type.startswith("image/"))
            if is_attachment and payload:
                item = {
                    "filename": filename or content_id or "attachment",
                    "content_type": content_type,
                    "size": len(payload),
                    "content_id": content_id or None,
                }
                if len(payload) <= MAX_ATTACHMENT_BYTES:
                    data_url = f"data:{content_type};base64,{base64.b64encode(payload).decode('ascii')}"
                    item["data_url"] = data_url
                    if content_id:
                        inline_images[content_id] = data_url
                else:
                    item["too_large"] = True
                attachments.append(item)
            elif content_type == "text/plain":
                body_text = part.get_content()
            elif content_type == "text/html":
                body_html = part.get_content()
    else:
        content_type = msg.get_content_type()
        if content_type == "text/html":
            body_html = msg.get_content()
        else:
            body_text = msg.get_content()

    if body_html and inline_images:
        def replace_cid(match):
            content_id = match.group(1).strip("<>").lower()
            return inline_images.get(content_id, match.group(0))

        normalized_images = {key.lower(): value for key, value in inline_images.items()}
        inline_images = normalized_images
        body_html = re.sub(r"cid:\s*<?([^\s>'\"]+)>?", replace_cid, body_html, flags=re.IGNORECASE)
            
    await store_email(
        recipient=recipient,
        sender=sender,
        subject=subject,
        body_text=body_text,
        body_html=body_html,
        raw=raw_content,
        attachments=attachments
    )
    return {"status": "ok"}

# ==================== WEB UI ====================
@app.get("/web", response_class=HTMLResponse)
async def web_ui():
    return """
    <!DOCTYPE html>
    <html>
    <head>
        <title>TempMail</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
            * { margin: 0; padding: 0; box-sizing: border-box; }
            body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: #0a0a1a; color: #e0e0e0; padding: 20px; min-height: 100vh; }
            .container { max-width: 900px; margin: 0 auto; }
            h1 { text-align: center; margin-bottom: 10px; background: linear-gradient(90deg, #00ff88, #00ccff); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.5em; }
            .subtitle { text-align: center; color: #888; margin-bottom: 30px; }
            .top-links { text-align: center; margin: -15px 0 25px; }
            .top-links a { color: #00ccff; text-decoration: none; margin: 0 10px; font-size: 0.95em; }
            .top-links a:hover { color: #00ff88; text-decoration: underline; }
            .card { background: #121228; border: 1px solid #1e1e3f; border-radius: 16px; padding: 25px; margin-bottom: 25px; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
            .email-display { background: #0a0a1a; border: 2px dashed #1e1e3f; border-radius: 12px; padding: 20px; font-size: 1.5em; color: #00ff88; text-align: center; font-weight: 700; word-break: break-all; margin: 15px 0; position: relative; }
            .timer { text-align: center; color: #ff6b6b; font-size: 0.95em; margin: 10px 0; }
            .btn-group { display: flex; gap: 10px; justify-content: center; flex-wrap: wrap; margin-top: 15px; }
            button { background: linear-gradient(135deg, #00ff88, #00cc6a); color: #000; border: none; padding: 12px 24px; border-radius: 10px; cursor: pointer; font-weight: 700; font-size: 0.95em; transition: transform 0.2s, box-shadow 0.2s; }
            button:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(0,255,136,0.3); }
            button.secondary { background: #1e1e3f; color: #00ff88; border: 1px solid #00ff88; }
            button.secondary:hover { background: #00ff88; color: #000; }
            input { padding: 12px; border-radius: 10px; border: 1px solid #333; background: #0a0a1a; color: #fff; width: 200px; font-size: 0.95em; }
            input:focus { outline: none; border-color: #00ff88; }
            .status { text-align: center; color: #00ff88; margin: 15px 0; min-height: 24px; }
            .messages { margin-top: 10px; }
            .message { background: #121228; border: 1px solid #1e1e3f; border-radius: 12px; padding: 18px; margin-bottom: 12px; border-left: 4px solid #00ff88; transition: all 0.2s; }
            .message:hover { border-color: #00ff88; transform: translateX(4px); }
            .msg-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px; flex-wrap: wrap; gap: 8px; }
            .sender { color: #888; font-size: 0.85em; }
            .subject { font-weight: 700; font-size: 1.1em; color: #fff; margin-bottom: 8px; }
            .preview { color: #aaa; font-size: 0.9em; line-height: 1.5; }
            .empty { text-align: center; color: #555; padding: 50px; font-size: 1.1em; }
            .badge { background: #1e1e3f; color: #00ff88; padding: 4px 10px; border-radius: 20px; font-size: 0.75em; }
            .count-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px; }
            .count { color: #888; }
            @keyframes pulse { 0%,100% { opacity: 1; } 50% { opacity: 0.5; } }
            .live { animation: pulse 2s infinite; color: #00ff88; }
            .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.8); z-index: 1000; justify-content: center; align-items: center; }
            .modal-content { background: #121228; border: 1px solid #1e1e3f; border-radius: 16px; padding: 30px; max-width: 700px; width: 90%; max-height: 80vh; overflow-y: auto; }
            .modal-header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
            .close-btn { background: none; border: none; color: #888; font-size: 1.5em; cursor: pointer; padding: 0; }
            .close-btn:hover { color: #fff; }
            .modal-body { color: #ccc; line-height: 1.7; }
            .modal-body pre { background: #0a0a1a; padding: 15px; border-radius: 8px; overflow-x: auto; font-size: 0.85em; }
            .html-email { width: 100%; min-height: 260px; border: 1px solid #2b2b52; border-radius: 10px; background: #fff; margin-top: 15px; }
            .text-email { white-space: pre-wrap; word-break: break-word; background: #0a0a1a; border-radius: 10px; padding: 15px; margin-top: 15px; }
            .attachment-list { display: flex; flex-wrap: wrap; gap: 12px; margin-top: 15px; }
            .attachment { background: #0a0a1a; border: 1px solid #2b2b52; border-radius: 10px; padding: 8px; max-width: 180px; }
            .attachment img { display: block; max-width: 160px; max-height: 120px; border-radius: 6px; object-fit: contain; background: #fff; }
            .file-download { display: block; color: #00ccff; text-decoration: none; font-weight: 600; padding: 12px 6px; }
            .file-download:hover { color: #00ff88; text-decoration: underline; }
            .attachment-name { display: block; color: #aaa; font-size: 0.78em; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; margin-top: 6px; }
            #refreshBtn { background: #1e1e3f; color: #00ff88; padding: 6px 12px; border-radius: 8px; font-size: 0.85em; border: 1px solid #1e1e3f; }
            #refreshBtn:hover { border-color: #00ff88; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>🔒 TempMail</h1>
            <p class="subtitle">Temporary email addresses that auto-expire</p>
            <div class="top-links">
                <a href="/docs">API Documentation (Swagger UI)</a>
                <a href="/redoc">ReDoc</a>
            </div>
            <div class="card">
                <p style="text-align:center;color:#888;margin-bottom:10px;">Your temporary email address:</p>
                <div class="email-display" id="email">Loading...</div>
                <div class="timer" id="timer"></div>
                <div class="status" id="status"></div>
                <div class="btn-group">
                    <button onclick="generateEmail(true)">🎲 New Random</button>
                    <input type="text" id="customInput" placeholder="custom-name" maxlength="30">
                    <button onclick="generateCustom()">✏️ Custom</button>
                    <button class="secondary" onclick="copyEmail()">📋 Copy</button>
                </div>
            </div>

            <div class="card">
                <div class="count-header">
                    <div class="count">
                        <span class="badge live">● LIVE</span>
                        <span id="msgCount">0 messages</span>
                    </div>
                    <button id="refreshBtn" onclick="loadMessages()">🔄 Refresh</button>
                </div>
                <div class="messages" id="messages">
                    <div class="empty">No messages yet. Send an email to your address!</div>
                </div>
            </div>
        </div>

        <div class="modal" id="modal">
            <div class="modal-content">
                <div class="modal-header">
                    <h3 id="modalSubject">Email Details</h3>
                    <button class="close-btn" onclick="closeModal()">&times;</button>
                </div>
                <div class="modal-body" id="modalBody"></div>
            </div>
        </div>

        <script>
            let currentEmail = null, ws = null, expiryTime = null, timerInterval = null, messages = [];

            async function init() {
                try {
                    const saved = localStorage.getItem('tempmail_data');
                    if (saved) {
                        const data = JSON.parse(saved);
                        if (data.email && Date.now() < data.saved_at + (data.expires_in * 1000)) {
                            setEmail(data, false);
                            return;
                        }
                        localStorage.removeItem('tempmail_data');
                    }
                    await generateEmail(false);
                } catch (e) {
                    console.error('Mailbox initialization failed', e);
                    showGenerationError();
                }
            }

            async function generateEmail(force = true) {
                document.getElementById('status').textContent = 'Generating...';
                try {
                    const res = await fetch('/api/generate');
                    if (!res.ok) throw new Error(`Generator returned ${res.status}`);
                    const data = await res.json();
                    setEmail(data, true);
                } catch (e) {
                    console.error('Address generation failed', e);
                    showGenerationError();
                }
            }

            async function generateCustom() {
                const custom = document.getElementById('customInput').value.trim();
                if (!custom) return;
                document.getElementById('status').textContent = 'Generating...';
                try {
                    const res = await fetch('/api/generate/' + encodeURIComponent(custom));
                    if (!res.ok) throw new Error(`Generator returned ${res.status}`);
                    const data = await res.json();
                    setEmail(data, true);
                } catch (e) {
                    console.error('Custom address generation failed', e);
                    showGenerationError();
                }
            }

            function showGenerationError() {
                document.getElementById('email').textContent = 'Unavailable';
                document.getElementById('status').textContent = '⚠️ Could not create an address. Tap New Random to retry.';
            }

            function setEmail(data, save = true) {
                currentEmail = data.email;
                if (save) {
                    data.saved_at = Date.now();
                    localStorage.setItem('tempmail_data', JSON.stringify(data));
                    expiryTime = Date.now() + (data.expires_in * 1000);
                } else {
                    const remaining = Math.floor((data.saved_at + (data.expires_in * 1000) - Date.now()) / 1000);
                    expiryTime = Date.now() + (remaining * 1000);
                }
                
                document.getElementById('email').textContent = data.email;
                document.getElementById('status').textContent = '✅ Active! Send emails to this address.';
                startTimer();
                connectWS();
                loadMessages();
            }

            function startTimer() {
                if (timerInterval) clearInterval(timerInterval);
                timerInterval = setInterval(() => {
                    const remaining = Math.max(0, Math.floor((expiryTime - Date.now()) / 1000));
                    const m = Math.floor(remaining / 60), s = remaining % 60;
                    document.getElementById('timer').textContent = remaining > 0 
                        ? `⏱️ Messages expire in: ${m}m ${s}s` 
                        : '⏱️ Expired - generate new email';
                    if (remaining <= 0) {
                        localStorage.removeItem('tempmail_data');
                    }
                }, 1000);
            }

            function copyEmail() {
                if (!currentEmail) return;
                navigator.clipboard.writeText(currentEmail);
                document.getElementById('status').textContent = '📋 Copied!';
                setTimeout(() => document.getElementById('status').textContent = '✅ Active!', 2000);
            }

            function connectWS() {
                if (ws) ws.close();
                const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
                ws = new WebSocket(proto + '//' + window.location.host + '/ws/' + currentEmail);

                ws.onmessage = (e) => {
                    const msg = JSON.parse(e.data);
                    if (msg.type === 'new_email') {
                        if (!messages.some(existing => existing.id === msg.id)) {
                            messages.unshift(msg);
                            renderMessages();
                        }
                        showNotification(msg);
                    } else if (msg.type === 'existing' && !messages.some(existing => existing.id === msg.id)) {
                        messages.push(msg);
                        renderMessages();
                    }
                };
                ws.onclose = () => {
                    if (currentEmail) setTimeout(connectWS, 3000);
                };
            }

            function showNotification(msg) {
                if ('Notification' in window && Notification.permission === 'granted') {
                    new Notification('New Email!', { body: msg.subject });
                }
            }
            if ('Notification' in window) Notification.requestPermission();

            async function loadMessages() {
                if (!currentEmail) return;
                const btn = document.getElementById('refreshBtn');
                btn.textContent = '⌛ Loading...';
                try {
                    const res = await fetch('/api/inbox/' + encodeURIComponent(currentEmail));
                    const data = await res.json();
                    messages = data.messages;
                    renderMessages();
                } catch (e) {
                    console.error(e);
                }
                btn.textContent = '🔄 Refresh';
            }

            function renderMessages() {
                document.getElementById('msgCount').textContent = `${messages.length} message${messages.length !== 1 ? 's' : ''}`;
                const container = document.getElementById('messages');
                if (!messages.length) {
                    container.innerHTML = '<div class="empty">No messages yet. Send an email to your address!</div>';
                    return;
                }
                container.innerHTML = messages.map((m, i) => {
                    const previewSource = m.body_text || stripHtml(m.body_html || '') || 'No preview available';
                    const imageCount = (m.attachments || []).filter(a => a.content_type && a.content_type.startsWith('image/')).length;
                    return `
                        <div class="message" onclick="openModal(${i})" style="cursor:pointer;">
                            <div class="msg-header">
                                <span class="sender">${esc(m.sender || 'Unknown')}</span>
                                <span style="color:#ff6b6b;font-size:0.8em;">⏱️ ${Math.floor(m.expires_in/60)}m ${m.expires_in%60}s</span>
                            </div>
                            <div class="subject">${esc(m.subject || 'No Subject')}</div>
                            <div class="preview">${esc(previewSource).substring(0, 180)}${previewSource.length > 180 ? '...' : ''}${imageCount ? ` · 🖼️ ${imageCount} image${imageCount > 1 ? 's' : ''}` : ''}</div>
                        </div>
                    `;
                }).join('');
            }

            function openModal(idx) {
                const m = messages[idx];
                document.getElementById('modalSubject').textContent = m.subject;
                const htmlPart = m.body_html
                    ? '<iframe class="html-email" sandbox="allow-same-origin" referrerpolicy="no-referrer"></iframe>'
                    : `<div class="text-email">${esc(m.body_text || 'No content')}</div>`;
                const attachments = (m.attachments || []).map(a => a.data_url && a.content_type && a.content_type.startsWith('image/')
                    ? `<div class="attachment"><a href="${esc(a.data_url)}" download="${esc(a.filename || 'image')}"><img src="${esc(a.data_url)}" alt="${esc(a.filename || 'Image')}" referrerpolicy="no-referrer"></a><span class="attachment-name">${esc(a.filename || 'Image')}</span></div>`
                    : a.data_url
                        ? `<div class="attachment"><a class="file-download" href="${esc(a.data_url)}" download="${esc(a.filename || 'attachment')}">📎 Download ${esc(a.filename || 'Attachment')}</a><span class="attachment-name">${esc(a.content_type || 'file')} · ${formatBytes(a.size)}</span></div>`
                        : `<div class="attachment"><span class="attachment-name">📎 ${esc(a.filename || 'Attachment')} (${esc(a.content_type || 'file')})${a.too_large ? ' · too large to preview' : ''}</span></div>`
                ).join('');
                document.getElementById('modalBody').innerHTML = `
                    <p><strong>From:</strong> ${esc(m.sender)}</p>
                    <p><strong>Received:</strong> ${new Date(m.received_at * 1000).toLocaleString()}</p>
                    <p><strong>Expires in:</strong> ${Math.floor(m.expires_in/60)}m ${m.expires_in%60}s</p>
                    <hr style="border-color:#333;margin:15px 0;">
                    ${htmlPart}
                    ${attachments ? `<h4 style="margin-top:18px;">Attachments</h4><div class="attachment-list">${attachments}</div>` : ''}
                `;
                const frame = document.querySelector('#modalBody iframe');
                if (frame) {
                    frame.srcdoc = emailDocument(m.body_html);
                    frame.onload = () => {
                        frame.style.height = Math.min(900, Math.max(320, frame.contentDocument.body.scrollHeight + 24)) + 'px';
                    };
                }
                document.getElementById('modal').style.display = 'flex';
            }

            function closeModal() {
                document.getElementById('modal').style.display = 'none';
            }

            function esc(t) {
                const d = document.createElement('div');
                d.textContent = t || '';
                return d.innerHTML;
            }

            function stripHtml(html) {
                const d = document.createElement('div');
                d.innerHTML = html || '';
                return d.textContent || d.innerText || '';
            }

            function formatBytes(bytes) {
                if (!bytes) return '0 B';
                const units = ['B', 'KB', 'MB', 'GB'];
                const i = Math.min(Math.floor(Math.log(bytes) / Math.log(1024)), units.length - 1);
                return `${(bytes / Math.pow(1024, i)).toFixed(i ? 1 : 0)} ${units[i]}`;
            }

            function emailDocument(html) {
                const parsed = new DOMParser().parseFromString(html || '', 'text/html');
                parsed.querySelectorAll('script, iframe, object, embed, form, input, button, textarea, select, base, link').forEach(node => node.remove());
                parsed.querySelectorAll('*').forEach(node => {
                    [...node.attributes].forEach(attr => {
                        const name = attr.name.toLowerCase();
                        const value = attr.value.trim();
                        if (name.startsWith('on') || name === 'srcdoc' || /javascript\\s*:/i.test(value)) {
                            node.removeAttribute(attr.name);
                        } else if ((name === 'src' || name === 'href') && value && !/^(https?:|data:image\\/|mailto:|#)/i.test(value)) {
                            node.removeAttribute(attr.name);
                        }
                    });
                    if (node.tagName === 'A' && node.getAttribute('href')) {
                        node.setAttribute('target', '_blank');
                        node.setAttribute('rel', 'noopener noreferrer');
                    }
                });
                const styles = [...parsed.querySelectorAll('style')].map(style => style.textContent || '').join('\\n');
                const content = parsed.body ? parsed.body.innerHTML : html;
                return `<!doctype html><html><head><meta charset="utf-8"><style>
                    ${styles}
                    html,body { margin: 0; padding: 0; background: #fff; color: #111; }
                    body { font-family: Arial, Helvetica, sans-serif; line-height: 1.45; overflow-wrap: anywhere; }
                    img { max-width: 100%; height: auto; }
                    table { max-width: 100%; }
                    pre { white-space: pre-wrap; }
                </style></head><body>${content}</body></html>`;
            }

            document.getElementById('modal').onclick = (e) => {
                if (e.target.id === 'modal') closeModal();
            };

            init();
        </script>
    </body>
    </html>
    """

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
