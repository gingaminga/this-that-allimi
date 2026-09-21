# wedding-allimi

청첩장 Supabase에 저장되는 응답을 실시간으로 받아 Discord embed로 전송하는 Supabase Edge Function입니다.

## 알림 대상

- `attendance_survey_responses`: 신랑·신부 측, 참석 여부, 성함, 참석 인원
- `bus_survey_responses`: 대표자, 마스킹된 연락처, 탑승 인원, 전달 사항
- `guestbook_entries`: 작성자와 축하 메시지

비밀번호 해시는 절대 Discord payload에 포함하지 않으며, 휴대전화 번호는 기본적으로 가운데 자리를 가립니다.

## 로컬 검증

저장소 루트에서 다음 명령을 실행합니다.

```bash
pnpm install
pnpm test:wedding
```

## 배포

1. [Supabase CLI](https://supabase.com/docs/guides/local-development/cli/getting-started)를 설치하고 대상 프로젝트에 연결합니다.
2. 이 디렉터리에서 Secret과 함수를 배포합니다.

```bash
cd wedding-allimi
supabase link --project-ref YOUR_PROJECT_REF
supabase secrets set DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..."
supabase secrets set WEDDING_WEBHOOK_SECRET="충분히-긴-임의의-문자열"
supabase functions deploy wedding-allimi
```

3. Supabase Dashboard의 **Database → Webhooks**에서 아래 Webhook을 생성합니다.

| 테이블 | 이벤트 |
| --- | --- |
| `attendance_survey_responses` | `INSERT` |
| `bus_survey_responses` | `INSERT`, `UPDATE` |
| `guestbook_entries` | `INSERT` |

- URL: `https://YOUR_PROJECT_REF.supabase.co/functions/v1/wedding-allimi`
- HTTP method: `POST`
- Header: `x-webhook-secret: WEDDING_WEBHOOK_SECRET에 설정한 값`

Database Webhook이므로 청첩장 API의 저장 성공 여부와 Discord 전송 상태가 분리됩니다. Discord 장애 시 함수가 오류를 반환해 Supabase 측에서 실패를 확인할 수 있습니다.
