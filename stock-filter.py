import os
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import FinanceDataReader as fdr
import numpy as np


def calculate_rsi(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """RSI 계산 함수"""
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def send_discord_webhook(matched_stocks: list, webhook_url: str = None):
    """디스코드 웹훅으로 결과 전송"""
    if not webhook_url:
        webhook_url = os.getenv('DISCORD_WEBHOOK_URL')
    
    if not webhook_url:
        print("⚠️ 디스코드 웹훅 URL이 설정되지 않았습니다.")
        return
    
    # 필터링 조건 요약
    filter_desc = (
        "📊 [필터링 조건]\n"
        "- 3개월 평균 거래량 < 100만, 3개월 내 100만 이상 1회\n"
        "- RSI(14) ≤ 40\n"
        "- 최근 7일 내 5일선이 20일선 돌파\n"
    )
    
    if not matched_stocks:
        message = f"{filter_desc}\n❌ 조건에 맞는 종목이 없습니다."
    else:
        stocks_text = "\n".join([
            f"• {name} ({code}) - {close:,.0f}원" for name, code, close in matched_stocks
        ])
        message = f"{filter_desc}\n✅ **조건 만족 종목 ({len(matched_stocks)}개)**\n\n{stocks_text}"
    
    # 현재 시간 추가
    KST = timezone(timedelta(hours=9))
    current_time = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    message += f"\n\n⏰ **실행 시간**: {current_time} (KST)"
    
    try:
        payload = {"content": message}
        response = requests.post(webhook_url, json=payload, timeout=10)
        if response.status_code == 204:
            print("✅ 디스코드로 결과가 전송되었습니다.")
        else:
            print(f"❌ 디스코드 전송 실패: {response.status_code}")
    except Exception as e:
        print(f"❌ 디스코드 전송 오류: {e}")


def check_conditions(df: pd.DataFrame) -> bool:
    if len(df) < 60:
        return False

    # 1) 거래량 조건: 3개월 평균 거래량 < 100만이면서 3개월 내 100만 이상 한 번 이상
    three_month_avg = df['Volume'].tail(90).mean()  # 3개월 평균
    three_month_max = df['Volume'].tail(90).max()   # 3개월 최대
    
    if three_month_avg >= 1_000_000:  # 3개월 평균이 100만 이상이면 제외
        return False
    if three_month_max < 1_000_000:   # 3개월 내 100만 이상이 없으면 제외
        return False

    # 2) RSI 조건: RSI(14)가 40 이하
    rsi = calculate_rsi(df, period=14)
    if rsi.iloc[-1] > 40:  # RSI가 40 초과면 제외
        return False

    # 3) 5일선이 20일선 돌파 (골든크로스) - 최근 7일 내 돌파
    ma5 = df['Close'].rolling(5).mean()
    ma20 = df['Close'].rolling(20).mean()

    # 최근 7일 내에 골든크로스가 발생했는지 확인
    golden_cross_found = False
    for i in range(len(df) - 7, len(df) - 1):  # 최근 7일 확인
        if ma5.iloc[i] <= ma20.iloc[i] and ma5.iloc[i+1] > ma20.iloc[i+1]:  # 골든크로스 발생
            golden_cross_found = True
            break
    
    if not golden_cross_found:
        return False

    return True


def analyze_stock(stock_info):
    """개별 종목 분석 함수"""
    code = stock_info['Code']
    name = stock_info['Name']
    
    try:
        # 어제 날짜를 기준으로 데이터 가져오기 (자정 이후 안정성을 위해)
        yesterday = datetime.today() - timedelta(days=1)
        df = fdr.DataReader(code, start=(yesterday - timedelta(days=90)).strftime('%Y-%m-%d'))
        
        # 거래량 조건 먼저 체크
        three_month_avg = df['Volume'].tail(90).mean()
        three_month_max = df['Volume'].tail(90).max()
        
        if three_month_avg >= 1_000_000:
            return None
        if three_month_max < 1_000_000:
            return None
            
        if check_conditions(df):
            close = df['Close'].iloc[-1]
            print(f"✅ 조건 만족: {name} ({code})")
            return (name, code, close)
        else:
            return None
    except Exception as e:
        return None


def get_stock_list() -> pd.DataFrame:
    kospi = fdr.StockListing('KOSPI')
    kosdaq = fdr.StockListing('KOSDAQ')
    return pd.concat([kospi, kosdaq], ignore_index=True)


def run_filter() -> list:
    stock_list = get_stock_list()
    matched_stocks = []

    # 10개 스레드로 병렬 처리
    with ThreadPoolExecutor(max_workers=10) as executor:
        # 종목 정보를 딕셔너리로 변환
        stock_infos = [{'Code': row['Code'], 'Name': row['Name']} for _, row in stock_list.iterrows()]
        
        # 병렬로 분석 실행
        future_to_stock = {executor.submit(analyze_stock, stock_info): stock_info for stock_info in stock_infos}
        
        # 결과 수집
        for future in as_completed(future_to_stock):
            result = future.result()
            if result:
                matched_stocks.append(result)

    return matched_stocks


if __name__ == "__main__":
    matched = run_filter()
    if matched:
        print("조건에 맞는 종목 목록:")
        for name, code, close in matched:
            print(f"{name} ({code}) - {close:,.0f}원")
        send_discord_webhook(matched)
    else:
        print("조건에 맞는 종목이 없습니다.")
        send_discord_webhook([])
