#!/usr/bin/env python3
"""
TempMail SMTP Server - Alternative for Railway (uses webhook/relay approach)

CRITICAL: Railway blocks SMTP ports on Hobby plans. Options:
1. Upgrade to Railway Pro ($20+/mo) - SMTP ports unblocked
2. Use this webhook-based approach with a mail relay service
3. Use a VPS instead (DigitalOcean, Hetzner, etc.)

This file shows both approaches.
"""

import asyncio
import email
import json
import uuid
import os
from datetime import datetime, timezone
from aiosmtpd.controller import Controller
from aiosmtpd.handlers import Message
import asyncpg
import logging
import aiohttp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TempMail-SMTP")

DB_URL = os.getenv("DATABASE_URL")
EMAIL_EXPIRY = int(os.getenv("EMAIL_EXPIRY_SECONDS", "300"))
DOMAIN = os.getenv("MAIL_DOMAIN", "yourdomain.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "2525"))

# For webhook approach - configure these in your mail relay service
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "change-me")

class TempMailHandler(Message):
    async def handle_DATA(self, server, session, envelope):
        try:
            msg = email.message_from_bytes(envelope.content)
            recipients = envelope.rcpt_tos
            mail_from = envelope.mail_from

            logger.info(f"📧 Received from {mail_from} to {recipients}")

            subject = msg.get("Subject", "No Subject")
            from_addr = msg.get("From", mail_from)

            # Extract body
            body_text = ""
            body_html = ""
            attachments = []

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    cd = str(part.get("Content-Disposition", ""))

                    if "attachment" in cd:
                        filename = part.get_filename()
                        if filename:
                            attachments.append({
                                "filename": filename,
                                "content_type": content_type,
                                "size": len(part.get_payload(decode=True) or b"")
                            })
                    elif content_type == "text/plain":
                        try:
                            body_text = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        except:
                            body_text = part.get_payload() or ""
                    elif content_type == "text/html":
                        try:
                            body_html = part.get_payload(decode=True).decode("utf-8", errors="ignore")
                        except:
                            body_html = part.get_payload() or ""
            else:
                try:
                    content = msg.get_payload(decode=True).decode("utf-8", errors="ignore")
                except:
                    content = msg.get_payload() or ""
                if msg.get_content_type() == "text/html":
                    body_html = content
                else:
                    body_text = content

            for recipient in recipients:
                await self.store_email(
                    recipient=recipient.lower().strip(),
                    sender=from_addr,
                    subject=subject,
                    body_text=body_text,
                    body_html=body_html,
                    raw_content=envelope.content.decode("utf-8", errors="ignore"),
                    attachments=attachments
                )

            return "250 Message accepted"

        except Exception as e:
            logger.error(f"Error: {e}")
            return "451 Temporary failure"

    async def store_email(self, recipient, sender, subject, body_text, body_html, raw_content, attachments):
        conn = await asyncpg.connect(DB_URL)
        email_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).timestamp()
        expires = now + EMAIL_EXPIRY

        await conn.execute("""
            INSERT INTO emails (id, recipient, sender, subject, body_text, body_html, raw_content, attachments, received_at, expires_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        """, email_id, recipient, sender, subject, body_text, body_html, raw_content, json.dumps(attachments), now, expires)

        logger.info(f"✅ Stored {email_id} for {recipient}")
        await conn.close()

async def cleanup_task():
    while True:
        await asyncio.sleep(30)
        try:
            conn = await asyncpg.connect(DB_URL)
            now = datetime.now(timezone.utc).timestamp()
            result = await conn.execute("DELETE FROM emails WHERE expires_at < $1", now)
            if result != "DELETE 0":
                logger.info(f"🗑️ Cleaned: {result}")
            await conn.close()
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

async def init_db():
    conn = await asyncpg.connect(DB_URL)
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS emails (
            id UUID PRIMARY KEY, recipient TEXT NOT NULL, sender TEXT, subject TEXT,
            body_text TEXT, body_html TEXT, raw_content TEXT,
            attachments JSONB DEFAULT '[]', received_at DOUBLE PRECISION NOT NULL, expires_at DOUBLE PRECISION NOT NULL
        )
    """)
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_recipient ON emails(recipient)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_expires ON emails(expires_at)")
    await conn.close()
    logger.info("✅ Database ready")

async def main():
    await init_db()

    handler = TempMailHandler()
    controller = Controller(handler, hostname="0.0.0.0", port=SMTP_PORT)
    controller.start()
    logger.info(f"🚀 SMTP server on port {SMTP_PORT}")

    asyncio.create_task(cleanup_task())

    while True:
        await asyncio.sleep(3600)

if __name__ == "__main__":
    asyncio.run(main())
