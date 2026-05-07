# 🔒 TempMail Service

A fully functional temporary email service with real email reception, auto-expiring messages, WebSocket real-time delivery, and REST API support.

## Features

- ✅ **Real email reception** - Receive emails from Gmail, Outlook, anywhere
- ⏱️ **Auto-expiring emails** - Messages delete after 1-5 minutes (configurable)
- 🎲 **Random & custom addresses** - Generate random or custom usernames
- 🔔 **Real-time WebSocket** - Instant email notifications in browser
- 📡 **REST API** - Full programmatic access
- 🌐 **Web UI** - Built-in responsive interface
- 📎 **Attachment support** - Handles file attachments
- 🗑️ **Auto-cleanup** - Background task removes expired emails

---

## ⚠️ CRITICAL: Railway SMTP Port Blocking

**Railway blocks SMTP ports (25, 465, 587, 2525) on Hobby plans.** [^2^]

| Plan | SMTP Ports | Solution |
|------|-----------|----------|
| **Hobby** ($5/mo) | ❌ Blocked | Use webhook approach (Mailgun/Postmark) |
| **Pro** ($20+/mo) | ✅ Unblocked | Use direct SMTP server |
| **VPS** (DO, Hetzner) | ✅ Unblocked | Use direct SMTP server |

### Recommended Architecture by Platform

#### Option A: Railway Hobby (Webhook Approach) ⭐ RECOMMENDED
Use a mail relay service that forwards emails via HTTP webhook to your Railway app.

**Free mail relay options:**
- **Mailgun** - 5,000 emails/month free
- **Postmark** - 100 emails/month free
- **Cloudflare Email Routing** - Free, unlimited
- **AWS SES + SNS** - 62,000 emails/month free (first year)

#### Option B: Railway Pro / VPS (Direct SMTP)
Run the SMTP server directly. Requires port 25 or 2525 open.

---

## 🚀 Quick Deploy to Railway

### Step 1: Create Project

```bash
# Clone this repo
git clone <your-repo>
cd tempmail-service

# Deploy to Railway
railway login
railway init
railway up
```

### Step 2: Add PostgreSQL Database

In Railway dashboard:
1. Click **"New"** → **"Database"** → **"Add PostgreSQL"**
2. Railway automatically sets `DATABASE_URL`

### Step 3: Configure Environment Variables

In Railway dashboard → Variables:

```
DATABASE_URL=postgresql://... (auto-set by Railway)
MAIL_DOMAIN=yourdomain.com
EMAIL_EXPIRY_SECONDS=300
WEBHOOK_SECRET=your-random-secret-key
```

### Step 4: Set Up Domain & Email Routing

#### Using Cloudflare Email Routing (FREE)

1. **Add domain to Railway:**
   - Railway Dashboard → Settings → Custom Domains
   - Add `yourdomain.com`
   - Add the CNAME and TXT records to Cloudflare DNS

2. **Enable Email Routing in Cloudflare:**
   - Cloudflare Dashboard → Email → Email Routing → Routes
   - Add catch-all rule: `*@yourdomain.com` → forward to webhook
   - **OR** use Cloudflare Workers to POST to your webhook endpoint

3. **Cloudflare Worker for webhook forwarding:**
    Use the `worker.js` file provided in this repo. It uses the `/webhook/raw` endpoint which is more reliable for Cloudflare Workers.

    ```javascript
    export default {
      async email(message, env, ctx) {
        const url = env.WEBHOOK_URL || 'https://your-app.up.railway.app/webhook/raw';
        const secret = env.WEBHOOK_SECRET || 'your-random-secret-key';

        const rawEmail = await new Response(message.raw).text();

        await fetch(url, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Secret': secret
          },
          body: JSON.stringify({
            to: message.to,
            from: message.from,
            raw: rawEmail
          })
        });
      }
    };
    ```

#### Using Mailgun (FREE - 5K emails/mo)

1. Sign up at [mailgun.com](https://mailgun.com)
2. Add your domain and verify DNS records
3. Create a **Route**: `*@yourdomain.com` → Forward to `https://your-app.up.railway.app/webhook/mailgun`
4. Set `MAILGUN_API_KEY` in Railway variables

#### Using Postmark (FREE - 100 emails/mo)

1. Sign up at [postmarkapp.com](https://postmarkapp.com)
2. Add your domain
3. Go to **Inbound** → Add inbound webhook URL: `https://your-app.up.railway.app/webhook/postmark`
4. Set `POSTMARK_SECRET` in Railway variables

---

## 🏗️ Architecture

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│   Gmail/User    │────▶│  Mail Relay/    │────▶│  Your Railway   │
│   Sends Email   │     │  MX Server      │     │  Webhook API    │
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │   PostgreSQL    │
                                               │   (emails table)│
                                               └─────────────────┘
                                                         │
                                                         ▼
                                               ┌─────────────────┐
                                               │  WebSocket      │
                                               │  (real-time)    │
                                               └─────────────────┘
```

---

## 📡 API Documentation

### Generate Random Email
```bash
GET /api/generate

Response:
{
  "email": "a1b2c3d4e5f6@yourdomain.com",
  "expires_in": 300,
  "created_at": 1715000000.0
}
```

### Generate Custom Email
```bash
GET /api/generate/mycustomname

Response:
{
  "email": "mycustomname@yourdomain.com",
  "expires_in": 300,
  "created_at": 1715000000.0
}
```

### Get Inbox
```bash
GET /api/inbox/a1b2c3d4e5f6@yourdomain.com

Response:
{
  "email": "a1b2c3d4e5f6@yourdomain.com",
  "messages": [
    {
      "id": "uuid",
      "sender": "sender@gmail.com",
      "subject": "Hello",
      "body_text": "Email body...",
      "body_html": "<p>Email body...</p>",
      "attachments": [],
      "received_at": 1715000000.0,
      "expires_in": 245
    }
  ],
  "count": 1
}
```

### Get Single Message
```bash
GET /api/message/{message_id}
```

### Delete Message
```bash
DELETE /api/message/{message_id}
```

### Get Stats
```bash
GET /api/stats
```

### WebSocket (Real-time)
```javascript
const ws = new WebSocket('wss://your-app.up.railway.app/ws/a1b2c3d4e5f6@yourdomain.com');
ws.onmessage = (event) => {
  const email = JSON.parse(event.data);
  console.log('New email:', email);
};
```

### Webhook Endpoints

| Service | Endpoint | Method |
|---------|----------|--------|
| Mailgun | `/webhook/mailgun` | POST |
| Postmark | `/webhook/postmark` | POST |
| Generic | `/webhook/generic` | POST (requires `X-Secret` header) |

---

## 🖥️ Web Interface

Visit `/web` after deployment for a built-in UI:
- Generate emails
- Real-time inbox with WebSocket
- View message details
- Copy email address

---

## 🐳 Local Development

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up PostgreSQL locally
# Using Docker:
docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=tempmail postgres:15

# 3. Set environment variables
export DATABASE_URL=postgresql://postgres:postgres@localhost:5432/tempmail
export MAIL_DOMAIN=localhost
export EMAIL_EXPIRY_SECONDS=300
export PORT=8000

# 4. Run API
python api.py

# 5. (Optional) Run SMTP server for local testing
export SMTP_PORT=2525
python smtp_server.py
```

---

## 🔧 Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DATABASE_URL` | ✅ | - | PostgreSQL connection string |
| `MAIL_DOMAIN` | ✅ | `yourdomain.com` | Your email domain |
| `EMAIL_EXPIRY_SECONDS` | ❌ | `300` | Email lifetime in seconds |
| `PORT` | ❌ | `8000` | API server port |
| `SMTP_PORT` | ❌ | `2525` | SMTP server port (if using direct SMTP) |
| `WEBHOOK_SECRET` | ❌ | `change-me` | Secret for generic webhook auth |
| `MAILGUN_API_KEY` | ❌ | - | For Mailgun webhook verification |
| `POSTMARK_SECRET` | ❌ | - | For Postmark webhook verification |

---

## 📁 Project Structure

```
tempmail-service/
├── api.py              # FastAPI app + webhooks + WebSocket
├── smtp_server.py      # Direct SMTP server (for VPS/Pro plans)
├── requirements.txt    # Python dependencies
├── Procfile           # Railway process definition
├── railway.toml       # Railway deployment config
├── nixpacks.toml      # Build configuration
├── .env.example       # Environment template
└── .gitignore
```

---

## ⚡ Performance Tips

1. **Database**: Use Railway's managed PostgreSQL (auto-scaling)
2. **Connection pooling**: Already configured (5-20 connections)
3. **Cleanup interval**: 30 seconds (adjustable in code)
4. **WebSocket**: Automatic reconnection with 3s backoff

---

## 🛡️ Security Considerations

- Emails auto-expire (no persistent storage)
- Webhook endpoints require secrets
- CORS enabled for all origins (adjust for production)
- No authentication on inbox endpoints (by design for temp emails)
- Consider rate limiting for `/api/generate`

---

## 📝 License

MIT
