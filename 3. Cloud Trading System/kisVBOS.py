import os
import datetime
import json
import sqlite3
import time
import requests   # 설치 필요
import holidays   # 설치 필요
import telegram   # 설치 필요 (pip install python-telegram-bot==13.15)
from pandas import DataFrame   # 설치 필요

class KisVBOS():
    def __init__(self):
        # 매매시간 아니면 종료
        nowTime = int(datetime.datetime.now().strftime('%H%M'))
        if nowTime in range(910, 1520):
            # Windows와 Linux 확인
            if os.getcwd() == '/home/ubuntu':
                self.path = '/home/ubuntu/py/'
            else:
                self.path = ''

            # {현재가, 목표가, 5일이동평균, 보유수, 매수가}
            self.stocks = {"005930": {"now_price": 0, "target_price": 0, "5ma": 0, "buy_count": 0, "buy_price": 0}}

            self.modeCheck(False)  # 실제 : True, 모의 : False
            self.kisConnect()      # kis access_token 취득
            self.getStocksList()   # 보유 주식 및 예수금 확인
            self.targetPrice()     # 목표가 설정
            self.startDeal()       # 현재가 확인 및 매매

    def startDeal(self):
        nowTime = int(datetime.datetime.now().strftime('%H%M'))
        if nowTime >= 1515:
            for code in self.stocks:
                nowprice = self.stocks[code]["now_price"]  # 현재가
                if self.stocks[code]["buy_count"] > 0:
                    if nowprice < self.stocks[code]["5ma"]:
                        self.dealStock(code, self.stocks[code]["buy_count"], self.sell_trid)

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - 종가가 5일이평선 이하로 매도 %s\n" % (now, code)
                        self.log_print(sendText)
                        self.send_msg(sendText)

                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sendText = "%s - 매매종료(현재시간 : %s)\n" % (now, nowTime)
                print(sendText)
                self.log_print(sendText)
                self.send_msg(sendText)
        else :
            for code in self.stocks:
                nowprice = self.stocks[code]["now_price"]  # 현재가
                if self.stocks[code]["buy_count"] == 0:
                    if nowprice >= self.stocks[code]["target_price"] and nowprice >= self.stocks[code]["5ma"]:
                        cnt = self.eachAssets // nowprice
                        self.dealStock(code, cnt, self.buy_trid)

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - 매수 %s\n" % (now, code)
                        print(sendText)
                        self.log_print(sendText)
                        self.send_msg(sendText)

                else:
                    if nowprice < self.stocks[code]["buy_price"] * 0.985:
                        self.dealStock(code, self.stocks[code]["buy_count"], self.sell_trid)

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = '%s - 매도 %s\n' % (now, code)
                        print(sendText)
                        self.log_print(sendText)
                        self.send_msg(sendText)

    def dealStock(self, code, cnt, tr_id):
        PATH = "uapi/domestic-stock/v1/trading/order-cash"
        URL = f"{self.URL_BASE}/{PATH}"
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "PDNO": code,  # 주식 코드
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(cnt),  # 수량
            "ORD_UNPR": "0",  # 주문 단가
        }
        headers = {
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": tr_id
        }

        res = requests.post(URL, headers=headers, data=json.dumps(body))
        time.sleep(0.1)

    def send_msg(self, msg):
        apiToken = "863154404:AAF6hb_eeScAqIfqfalm14ZR2pGOwRD5HkE"
        chatId = "451041516"
        bot = telegram.Bot(apiToken)
        bot.sendMessage(chatId, msg)

    def targetPrice(self):
        for code in self.stocks:
            # 일별 현재가 가져오기
            PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
            URL = f"{self.URL_BASE}/{PATH}"
            headers = {
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "appKey": self.APP_KEY,
                "appSecret": self.APP_SECRET,
                "tr_id": "FHKST01010400"
            }
            qparams = {
                "fid_cond_mrkt_div_code": "J",  # 종목 구분
                "fid_input_iscd": code,  # 주식 코드
                "fid_org_adj_prc": "1",  # 수정주가
                "fid_period_div_code": "D"  # 일자별 데이터
            }
            res = requests.get(URL, headers=headers, params=qparams)
            time.sleep(0.1)

            result = res.json()['output']
            df = DataFrame(result)

            # 현재가 설정
            self.stocks[code]["now_price"] = int(df['stck_clpr'][0])

            # 목표가 설정(변동성 돌파 전략)
            openprice = int(df.iloc[0]['stck_oprc'])
            interval = int(df.iloc[1]['stck_hgpr']) - int(df.iloc[1]['stck_lwpr'])
            k_range = interval * 0.5
            targetPrice = openprice + k_range
            self.stocks[code]["target_price"] = int(targetPrice)

            # 5일 이평선 설정
            df_close = df['stck_clpr']
            self.stocks[code]["5ma"] = df_close.rolling(window=5).mean()[4]

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = '%s - 목표가 설정결과 : %s\n' % (now, self.stocks)
            print(sendText)
            self.log_print(sendText)

    def getStocksList(self):
        PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": self.balance_trid,  # [실전투자] TTTC8434R, [모의투자] VTTC8434R
        }
        qparams = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "02",  # 조회구분
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",  # 펀드결제분포함여부
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "00",  # 처리구분
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }

        res = requests.get(URL, headers=headers, params=qparams)
        time.sleep(0.1)

        myStocks = res.json()['output1']
        for myStock in myStocks:
            self.stocks[myStock['pdno']]["buy_count"] = int(myStock['hldg_qty'])
            self.stocks[myStock['pdno']]["buy_price"] = round(float(myStock['pchs_avg_pric']))

        self.myTotalAssets = int(res.json()['output2'][0]['tot_evlu_amt'])
        self.eachAssets = round(self.myTotalAssets / len(self.stocks))  # 주식 한종목당 배정 금액

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = '%s - 총평가금액 : %s원(주식당 %s원)\n' % (now, self.myTotalAssets, self.eachAssets)
        print(sendText)
        self.log_print(sendText)

    def log_print(self, msg):
        date = datetime.datetime.now().strftime('%Y%m%d')
        file_name = self.path + 'log/kistrade_' + date + '.log'
        log_file = open(file_name, 'a')
        log_file.write(msg)

    def kisConnect(self):  # ACCESS_TOKEN 구하기
        nowDate = int(datetime.datetime.now().strftime('%Y%m%d'))  # 거래일

        DBPath = self.path + 'kis_access.db'  # DB 파일위치
        DBconnect = sqlite3.connect(DBPath, isolation_level=None)
        sqlite3.Connection
        cursor = DBconnect.cursor()

        # ACCESS_TOKEN 발급 여부 확인하여 없으면 생성
        sql = "SELECT ACCESS_TOKEN FROM KISTrade WHERE date = %s;" % (nowDate)
        cursor.execute(sql)
        value = cursor.fetchall()
        if len(value) == 0:
            body = {"grant_type": "client_credentials",
                    "appkey": self.APP_KEY,
                    "appsecret": self.APP_SECRET}
            PATH = "oauth2/tokenP"
            URL = f"{self.URL_BASE}/{PATH}"

            res = requests.post(URL, headers="", data=json.dumps(body))
            time.sleep(0.1)
            self.ACCESS_TOKEN = res.json()["access_token"]

            sql = "INSERT INTO KISTrade (date, ACCESS_TOKEN) VALUES (%s, '%s');" % (nowDate, self.ACCESS_TOKEN)
            cursor.execute(sql)

        else:
            self.ACCESS_TOKEN = value[0][0]

    def modeCheck(self, mode):
        # 모드 선택
        with open(self.path + 'keysKIS.txt', 'r') as file:  # kis 접속을 위한 키
            keys = file.readlines()

        if mode:
            self.CANO = keys[3].strip()       # 계좌번호
            self.APP_KEY = keys[4].strip()    # 앱키
            self.APP_SECRET = keys[5].strip() # 앱비밀키

            self.URL_BASE = "https://openapi.koreainvestment.com:9443"  # 도메인
            self.balance_trid = 'TTTC8434R'   # 잔고 조회
            self.buy_trid = 'TTTC0802U'       # 매수 주문
            self.sell_trid = 'TTTC0801U'      # 매도 주문
        else:
            self.CANO = keys[0].strip()       # 계좌번호
            self.APP_KEY = keys[1].strip()    # 앱키
            self.APP_SECRET = keys[2].strip() # 앱비밀키

            self.URL_BASE = "https://openapivts.koreainvestment.com:29443"  # 도메인
            self.balance_trid = 'VTTC8434R'  # 잔고 조회
            self.buy_trid = 'VTTC0802U'       # 매수 주문
            self.sell_trid = "VTTC0801U"      # 매도 주문

if __name__ == "__main__":
    kr_holidays = holidays.KR()
    date = datetime.date.today().strftime('%Y-%m-%d')

    if date not in kr_holidays:
        KisVbos = KisVBOS()
    else:
        sendText = '오늘은 휴일입니다.'
        print(sendText)
        exit()
