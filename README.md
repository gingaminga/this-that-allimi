# this-that-allimi

생활에 필요한 작은 자동화와 Discord 알림을 한 저장소에서 관리하는 폴리글랏 모노레포입니다.

## 서비스

| 서비스 | 런타임 | 역할 | 실행 방식 |
| --- | --- | --- | --- |
| [`quiz-allimi`](./quiz-allimi) | Node.js | 토스 퀴즈 정답 수집 | GitHub Actions 스케줄 |
| [`stock-allimi`](./stock-allimi) | Python | 국내 주식 조건 검색 | GitHub Actions 스케줄 |
| [`wedding-allimi`](./wedding-allimi) | Supabase Edge Functions | 웨딩 응답 Discord 알림 | Supabase Database Webhook |

각 서비스는 자체 README와 환경 변수 계약을 갖습니다. 루트에서는 설치, 검사, 테스트를 한 번에 실행합니다.

```bash
pnpm install
pnpm check
pnpm test
```

개별 서비스 실행 예시:

```bash
pnpm quiz
pnpm test:wedding
```

## 저장소 규칙

- 비밀값은 코드나 `.env` 파일로 커밋하지 않습니다.
- Discord Webhook URL은 GitHub Actions Secret 또는 Supabase Secret으로 관리합니다.
- 새 알리미는 독립된 디렉터리, README, 검사 또는 테스트 명령을 함께 추가합니다.
- 루트 `pnpm check`와 `pnpm test`가 통과해야 합니다.

## CI

`.github/workflows/ci.yml`이 푸시와 Pull Request마다 Node.js, Python, 웨딩 알림 포맷터를 함께 검증합니다. 기존 스케줄 워크플로의 경로와 Secret 이름은 유지합니다.
