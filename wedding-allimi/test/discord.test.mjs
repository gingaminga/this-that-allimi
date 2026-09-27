import assert from "node:assert/strict";
import test from "node:test";

import {
  buildDiscordMessage,
  buildDiscordWebhookUrl,
  maskPhone,
} from "../supabase/functions/_shared/discord.mjs";

const now = new Date("2026-09-21T05:00:00.000Z");

test("참석 응답을 한국어 Discord embed로 변환한다", () => {
  const result = buildDiscordMessage({
    type: "INSERT",
    table: "attendance_survey_responses",
    record: { id: 12, side: "groom", attendance: "attending", name: "홍길동", guest_count: 2 },
  }, { now });

  assert.equal(result.username, "웨딩 알리미");
  assert.equal(result.embeds[0].title, "💌 참석 응답이 도착했어요");
  assert.deepEqual(result.embeds[0].fields.map(({ name, value }) => [name, value]), [
    ["구분", "신랑 측"],
    ["참석 여부", "참석"],
    ["성함", "홍길동"],
    ["참석 인원", "2명"],
  ]);
});

test("전세버스 응답은 연락처를 가리고 전달 사항을 표시한다", () => {
  const result = buildDiscordMessage({
    type: "UPDATE",
    table: "bus_survey_responses",
    record: { id: 3, name: "김하객", phone: "01012345678", outbound_count: 4, note: "유아 동반" },
  }, { now });

  assert.equal(result.embeds[0].title, "🚌 전세버스 응답이 수정됐어요");
  assert.equal(result.embeds[0].fields[1].value, "010-****-5678");
  assert.equal(result.embeds[0].fields[3].inline, false);
});

test("방명록 payload에 password_hash를 노출하지 않는다", () => {
  const result = buildDiscordMessage({
    type: "INSERT",
    table: "guestbook_entries",
    record: { id: 5, name: "친구", message: "결혼 축하해!", password_hash: "do-not-leak" },
  }, { now });

  const serialized = JSON.stringify(result);
  assert.match(serialized, /결혼 축하해!/);
  assert.doesNotMatch(serialized, /do-not-leak|password_hash/);
});

test("DELETE 이벤트는 old_record를 사용한다", () => {
  const result = buildDiscordMessage({
    type: "DELETE",
    table: "guestbook_entries",
    old_record: { id: 8, name: "친구", message: "삭제된 메시지" },
  }, { now });

  assert.match(result.embeds[0].footer.text, /^삭제/);
});

test("지원하지 않는 테이블은 거부한다", () => {
  assert.throws(() => buildDiscordMessage({ table: "private_table", record: {} }), /지원하지 않는 테이블/);
});

test("전화번호 마스킹", () => {
  assert.equal(maskPhone("010-9876-5432"), "010-****-5432");
  assert.equal(maskPhone("123"), "-");
});

test("Discord 웹훅 URL에 대상 스레드 ID를 추가한다", () => {
  const result = buildDiscordWebhookUrl(
    "https://discord.com/api/webhooks/123/token?existing=value",
    "123456789012345678",
  );

  const url = new URL(result);
  assert.equal(url.searchParams.get("existing"), "value");
  assert.equal(url.searchParams.get("thread_id"), "123456789012345678");
  assert.equal(url.searchParams.get("wait"), "true");
});

test("Discord 스레드 ID 형식을 검증한다", () => {
  assert.throws(
    () => buildDiscordWebhookUrl("https://discord.com/api/webhooks/123/token", "not-a-thread"),
    /스레드 ID 형식/,
  );
});
