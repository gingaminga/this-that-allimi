const TABLE_FORMATTERS = {
  attendance_survey_responses: formatAttendance,
  bus_survey_responses: formatBus,
  guestbook_entries: formatGuestbook,
};

const COLORS = {
  attendance: 0x789985,
  absence: 0xb98282,
  bus: 0x6d8fa7,
  guestbook: 0xb4838c,
};

function text(value, fallback = "-") {
  if (value === null || value === undefined || value === "") return fallback;
  return String(value).slice(0, 1024);
}

function field(name, value, inline = true) {
  return { name, value: text(value), inline };
}

function actionLabel(type) {
  if (type === "UPDATE") return "수정";
  if (type === "DELETE") return "삭제";
  return "신규";
}

function sideLabel(side) {
  return { groom: "신랑 측", bride: "신부 측" }[side] ?? text(side);
}

function attendanceLabel(attendance) {
  return {
    attending: "참석",
    "not-attending": "불참",
  }[attendance] ?? text(attendance);
}

function maskPhone(phone) {
  const digits = String(phone ?? "").replace(/\D/g, "");
  if (digits.length < 7) return "-";
  return `${digits.slice(0, 3)}-****-${digits.slice(-4)}`;
}

function eventTimestamp(record, now) {
  const candidate = record.updated_at ?? record.created_at;
  const parsed = candidate ? new Date(candidate) : now;
  return Number.isNaN(parsed.getTime()) ? now.toISOString() : parsed.toISOString();
}

function baseEmbed(payload, record, now) {
  return {
    footer: { text: `${actionLabel(payload.type)} · ${payload.table} · #${text(record.id)}` },
    timestamp: eventTimestamp(record, now),
  };
}

function formatAttendance(payload, record, now) {
  const attending = record.attendance === "attending";
  return {
    ...baseEmbed(payload, record, now),
    title: attending ? "💌 참석 응답이 도착했어요" : "💌 불참 응답이 도착했어요",
    color: attending ? COLORS.attendance : COLORS.absence,
    fields: [
      field("구분", sideLabel(record.side)),
      field("참석 여부", attendanceLabel(record.attendance)),
      field("성함", record.name),
      field("참석 인원", `${text(record.guest_count, "0")}명`),
    ],
  };
}

function formatBus(payload, record, now) {
  const fields = [
    field("대표자", record.name),
    field("연락처", maskPhone(record.phone)),
    field("탑승 인원", `${text(record.outbound_count, "0")}명`),
  ];

  if (record.note) fields.push(field("전달 사항", record.note, false));

  return {
    ...baseEmbed(payload, record, now),
    title: payload.type === "UPDATE" ? "🚌 전세버스 응답이 수정됐어요" : "🚌 전세버스 응답이 도착했어요",
    color: COLORS.bus,
    fields,
  };
}

function formatGuestbook(payload, record, now) {
  return {
    ...baseEmbed(payload, record, now),
    title: "🌷 새 축하 메시지가 도착했어요",
    color: COLORS.guestbook,
    fields: [
      field("작성자", record.name),
      field("메시지", record.message, false),
    ],
  };
}

export function buildDiscordMessage(payload, { now = new Date() } = {}) {
  if (!payload || typeof payload !== "object") {
    throw new TypeError("Webhook payload가 필요합니다.");
  }

  const formatter = TABLE_FORMATTERS[payload.table];
  if (!formatter) {
    throw new RangeError(`지원하지 않는 테이블입니다: ${text(payload.table)}`);
  }

  const record = payload.record ?? payload.old_record;
  if (!record || typeof record !== "object") {
    throw new TypeError("Webhook record가 필요합니다.");
  }

  return {
    username: "웨딩 알리미",
    allowed_mentions: { parse: [] },
    embeds: [formatter(payload, record, now)],
  };
}

export { maskPhone };
