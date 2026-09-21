import { buildDiscordMessage } from "../_shared/discord.mjs";

const JSON_HEADERS = { "content-type": "application/json; charset=utf-8" };

function json(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: JSON_HEADERS });
}

Deno.serve(async (request) => {
  if (request.method !== "POST") {
    return json({ error: "Method not allowed" }, 405);
  }

  const webhookUrl = Deno.env.get("DISCORD_WEBHOOK_URL");
  const expectedSecret = Deno.env.get("WEDDING_WEBHOOK_SECRET");
  const providedSecret = request.headers.get("x-webhook-secret");

  if (!webhookUrl || !expectedSecret) {
    console.error("필수 Secret이 설정되지 않았습니다.");
    return json({ error: "Server is not configured" }, 500);
  }

  if (!providedSecret || providedSecret !== expectedSecret) {
    return json({ error: "Unauthorized" }, 401);
  }

  try {
    const payload = await request.json();
    const discordPayload = buildDiscordMessage(payload);
    const discordResponse = await fetch(webhookUrl, {
      method: "POST",
      headers: JSON_HEADERS,
      body: JSON.stringify(discordPayload),
    });

    if (!discordResponse.ok) {
      const responseText = await discordResponse.text();
      console.error("Discord 전송 실패", discordResponse.status, responseText);
      return json({ error: "Discord delivery failed" }, 502);
    }

    return json({ ok: true });
  } catch (error) {
    console.error("Webhook 처리 실패", error);
    return json({ error: "Invalid webhook payload" }, 400);
  }
});
