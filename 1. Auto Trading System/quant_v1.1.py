import os
import sqlite3
import time, datetime, random
import pandas as pd
from selenium import webdriver  # 설치필요
from selenium.webdriver.common.by import By  # selenium 버전이 4 이상일 경우 추가
import yfinance as yf  # 설치필요
import telepot  # 설치필요
import warnings
warnings.filterwarnings('ignore')

# 전역변수 선언
DBPath = 'quantDB.db'  # DB 파일위치
nowDateTime = datetime.datetime.now().strftime('%Y%m%d%H%M')

# 각종 함수 정의
def resetDB():
   print("데이터베이스 초기화")
   connect = sqlite3.connect(DBPath, isolation_level=None)
   sqlite3.Connection
   cursor = connect.cursor()
   cursor.execute("DELETE FROM StockRank;")
   cursor.execute("DELETE FROM StockHaving;")
   cursor.execute("DELETE FROM QuantList;")
   connect.close()

def getCodeList():
    print("종목 리스트 파일 다운로드 시작")
    dataFolder = "C:\\Users\\lpure\\PycharmProjects\\QuantInvest\\down"
    filelist = os.listdir(dataFolder)  # 파일 다운로드 전에 모든 파일 삭제
    for filename in filelist:
        filePath = dataFolder + "\\" + filename
        if os.path.isfile(filePath):
            os.remove(filePath)

    options = webdriver.ChromeOptions()
    # options.add_argument('headless')  # 화면 숨김 필요시 사용
    options.add_experimental_option('excludeSwitches', ['enable-logging'])
    options.add_experimental_option("prefs", {"download.default_directory": dataFolder})
    path = "chromedriver.exe"
    driver = webdriver.Chrome(path, options=options)

    driver.get("http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101")
    time.sleep(20)  #  창이 모두 열릴 때 까지 5초 기다립니다.
    driver.find_element(By.XPATH, '//*[@id="MDCSTAT015_FORM"]/div[2]/div/p[2]/button[2]/img').click()     # selenium 버전이 4 이상일 경우수정
    time.sleep(10)  #  다운로드가 될때 까지 5초 기다립니다.
    driver.find_element(By.XPATH, '//*[@id="ui-id-1"]/div/div[2]').click()       # selenium 버전이 4 이상일 경우 수정
    time.sleep(5)  #  다운로드가 될때 까지 5초 기다립니다.

    filelist = os.listdir(dataFolder)
    while len(filelist) == 0: # 5초 이후에도 다운로드 안되면 5 더 기다림
        time.sleep(5)
        filelist = os.listdir(dataFolder)

    filePath = dataFolder + '\\' + filelist[0]
    driver.quit()

    print("파일 업로드")
    def changeCode(code):
        code = str(code)
        code = '0' * (6 - len(code)) + code
        return code

    temp_df = pd.read_csv(filePath, encoding='CP949')
    info_df = temp_df[['종목코드', '종목명', '종가', '시가총액', '상장주식수', '거래량', '시장구분']]
    info_df['종목코드'] = info_df['종목코드'].apply(changeCode)

    # 조건를 충족하지 않는 데이터를 필터링하여 새로운 변수에 저장합니다.
    noPrice = info_df['거래량'] == 0
    info_df = info_df[~noPrice]  # ~ : 틸데라고 하며 반대조건 증 지금은 거래량에 데이터가 0인 경우 제외됨

    konex = info_df['시장구분'] == "KONEX"
    info_df = info_df[~konex]  # ~ : 틸데라고 하며 반대조건. KONEX제외(개인은 거래가 제한, 3천만원 예수금 필요함)

    info_df = info_df.sort_values(by=['시가총액'])
    info_df.reset_index(drop=True, inplace=True) # 정렬로 인덱스 변경에 따른 인덱스 재설정
    cnt = len(info_df) * 0.2  # 시가총액 하위 20%만 선택
    info_df = info_df.loc[:cnt]

    print("StockList 테이블에 주식목록 업로드")

    connect = sqlite3.connect(DBPath, isolation_level=None)
    sqlite3.Connection
    cursor = connect.cursor()

    info_df.to_sql('TempStockList', connect, if_exists='replace') # 임시 테이블에 기존자료 삭제 후 업로드

    sql = "INSERT INTO StockList (Code, Name, Price, MarketCap, StockIndex, NumStock, Date) SELECT 종목코드, 종목명, 종가, 시가총액, 시장구분, 상장주식수, '%s' FROM TempStockList;" % (nowDateTime)
    cursor.execute(sql)
    connect.close()

def getCodeInfo():
    print("주식 재무제표 획득 시작")

    connect = sqlite3.connect(DBPath, isolation_level=None)
    sqlite3.Connection
    cursor = connect.cursor()

    sql = "SELECT ID, Name, Code, NumStock, StockIndex FROM StockList WHERE Date = '%s' ORDER BY MarketCap;" % (nowDateTime)
    cursor.execute(sql)
    rows = cursor.fetchall()

    cnt = 1
    for row in rows:
        print("(%s / %s) %s(%s) 주식 정보 가져오기" % (cnt, len(rows), row[1], row[2]))
        eps, sps, bps, cfps = 0, 0, 0, 0
        if row[4] == 'KOSPI':
            StockIndex = '.KS'
        else:
            StockIndex = '.KQ'

        code = row[2] + StockIndex
        stock = yf.Ticker(code)
        stockData = stock.financials.fillna(0)  # Net Income(순이익), Gross Profit(매출총이익)  조회안되는 종목'056730'
        if "Net Income" in stockData.index:
            eps = stockData.loc["Net Income"][0] / row[3]

            if "Gross Profit" in stockData.index:
                sps = stockData.loc["Gross Profit"][0] / row[3]

                stockData = stock.balancesheet.fillna(0)  # 자본 = Total Assets(총자산) - Total Liab(총부채)
                if "Total Assets" in stockData.index and "Total Liab" in stockData.index:
                    bps = (stockData.loc["Total Assets"][0] - stockData.loc["Total Liab"][0]) / row[3]

                    stockData = stock.cashflow.fillna(0)  # Total Cash From Operating Activities
                    if "Total Cash From Operating Activities" in stockData.index:
                        cfps = stockData.loc["Total Cash From Operating Activities"][0] / row[3]

        sql_update_value = "UPDATE StockList SET EPS = %s, BPS = %s, CFPS = %s, SPS = %s, Date = '%s' WHERE ID = %s;"
        sql_update_value = sql_update_value % (eps, bps, cfps, sps, nowDateTime, row[0])
        cursor.execute(sql_update_value)

        cnt += 1
        time.sleep(random.uniform(0.1, 5))

    connect.close()

    print("주식 재무제표 획득 완료")
    send_msg("주식 재무제표 획득 완료")

def send_msg(msg):
    with open('telepot.txt', 'r') as file:
        keys = file.readlines()
        httpAPI = keys[0].strip()
        chatId = keys[1].strip()
    bot = telepot.Bot(httpAPI)
    bot.sendMessage(chatId, msg)

# 프로그램 시작
sendText = "주식 정보를 수집합니다."
print(sendText)

resetDB() # 데이터베이스 초기화
getCodeList()  # 주식 목록 획득
getCodeInfo()  # 주식 재무제표 정보 수집
