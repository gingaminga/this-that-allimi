# stock-allimi

국내 주식 데이터를 조회해 거래량, 이동평균선, 일목균형표 조건을 만족하는 종목을 Discord로 전송합니다.

## 실행

```bash
python3 -m pip install -r stock-allimi/requirements.txt
DISCORD_WEBHOOK_URL="https://discord.com/api/webhooks/..." python3 stock-allimi/stock-filter-1.py
```

운영 환경에서는 `.github/workflows/stock-allimi.yml`이 매일 실행하며 `STOCK_DISCORD_WEBHOOK_URL` GitHub Actions Secret을 사용합니다.

실험용 스크립트는 검증이 끝나기 전까지 운영 워크플로에서 호출하지 않습니다.
