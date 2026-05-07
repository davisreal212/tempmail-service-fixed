export default {
  async email(message, env, ctx) {
    const url = env.WEBHOOK_URL || 'https://your-app.up.railway.app/webhook/raw';
    const secret = env.WEBHOOK_SECRET || 'change-me';

    const rawEmail = await new Response(message.raw).text();

    const payload = {
      to: message.to,
      from: message.from,
      raw: rawEmail
    };

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Secret': secret
      },
      body: JSON.stringify(payload)
    });

    if (!response.ok) {
      console.error('Failed to forward email:', await response.text());
    }
  }
};
