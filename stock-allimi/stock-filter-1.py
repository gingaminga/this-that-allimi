import os
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import FinanceDataReader as fdr

def calculate_ichimoku(df: pd.DataFrame):
    """일목균형표 지표 계산 (정확한 공식)"""
    # 기준 설정
    conversion_period = 9    # 전환기간
    base_period = 26        # 기준기간
    leading_span2_period = 52  # 선행2기간
    displacement = 26       # 선행 이동값
    
    high = df['High']
    low = df['Low']
    close = df['Close']
    
    # 전환선 (9일 최고가 + 최저가) / 2
    conversion_line = (high.rolling(conversion_period).max() + low.rolling(conversion_period).min()) / 2
    
    # 기준선 (26일 최고가 + 최저가) / 2
    base_line = (high.rolling(base_period).max() + low.rolling(base_period).min()) / 2
    
    # 선행스팬1 = (전환선 + 기준선) / 2
    leading_span1 = (conversion_line + base_line) / 2
    
    # 선행스팬2 = (52일 최고가 + 최저가) / 2
    leading_span2 = (high.rolling(leading_span2_period).max() + 
                     low.rolling(leading_span2_period).min()) / 2
    
    return {
        'conversion_line': conversion_line,
        'base_line': base_line,
        'leading_span1': leading_span1,
        'leading_span2': leading_span2
    }

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
        "- 최근 7일 내 5일선이 20일선 돌파\n"
        "- 현재 주가가 일목균형표 음구름(파랑) 아래\n"
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
    if len(df) < 78:  # 일목균형표 계산을 위해 충분한 데이터 필요 (52 + 26)
        return False

    # 1) 거래량 조건: 3개월 평균 거래량 < 100만이면서 3개월 내 100만 이상 한 번 이상
    three_month_avg = df['Volume'].tail(90).mean()  # 3개월 평균
    three_month_max = df['Volume'].tail(90).max()   # 3개월 최대
    
    if three_month_avg >= 1_000_000:  # 3개월 평균이 100만 이상이면 제외
        return False
    if three_month_max < 1_000_000:   # 3개월 내 100만 이상이 없으면 제외
        return False

    # 2) 5일선이 20일선 돌파 (골든크로스) - 최근 7일 내 돌파
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

    # 3) 일목균형표 조건: 현재 주가가 음구름(파란색구름대) 아래에 있는지
    ichimoku = calculate_ichimoku(df)
    current_price = df['Close'].iloc[-1]
    
    # 현재 시점의 구름대: 26일 전에 계산된 선행스팬 값들
    span1_current = ichimoku['leading_span1'].iloc[-26]
    span2_current = ichimoku['leading_span2'].iloc[-26]
    
    # NaN 값 체크
    if pd.isna(span1_current) or pd.isna(span2_current):
        return False
    
    # 구름대 판단: 선행스팬1 < 선행스팬2이면 음구름(파란색)
    is_negative_cloud = span1_current < span2_current
    
    if not is_negative_cloud:
        return False
    
    # 현재 주가가 구름대 아래에 있는지 확인
    cloud_bottom = min(span1_current, span2_current)
    
    if current_price >= cloud_bottom:
        return False

    return True


def analyze_stock(stock_info):
    """개별 종목 분석 함수"""
    code = stock_info['Code']
    name = stock_info['Name']
    
    try:
        # 어제 날짜를 기준으로 데이터 가져오기 (일목균형표 계산을 위해 120일)
        yesterday = datetime.today() - timedelta(days=1)
        df = fdr.DataReader(code, start=(yesterday - timedelta(days=120)).strftime('%Y-%m-%d'))
        
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
