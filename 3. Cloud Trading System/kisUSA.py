import os
import datetime
import json
import sqlite3
import time
import requests   # 설치 필요
import holidays   # 설치 필요
import telegram   # 설치 필요 (pip install python-telegram-bot==13.15)
from pandas import DataFrame   # 설치 필요
import pytz       # 설치 필요

class KisVBOS():
    def __init__(self):
        # 매매시간 아니면 종료
        nowTime = int(datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%H%M'))
        if nowTime in range(940, 1550):
            # Windows와 Linux 확인
            if os.getcwd() == '/home/centos':
                self.path = '/home/centos/py/'
            else:
                self.path = ''

            # {현재가, 목표가, 5일이동평균, 보유수, 매수가}
            self.stocks = {"TQQQ": {"now_price": 0, "target_price": 0, "5ma": 0, "buy_count": 0, "buy_price": 0}}

            self.modeCheck()       # 변수설정(실제투자만 사용)
            self.kisConnect()      # kis access_token 취득
            self.notsignedOrder()  # 매매 등록 확인(추가)
            self.getStocksList()   # 보유 주식 및 예수금 확인
            self.targetPrice()     # 목표가 설정
            self.startDeal()       # 현재가 확인 및 매매

    def notsignedOrder(self):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-nccs"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": self.notsigned_trid,
        }
        qparams = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",  # 해외거래소코드
            "SORT_SQN": "DS",  # 해외거래소코드
            "CTX_AREA_FK200": "",  # 연속조회검색조건200
            "CTX_AREA_NK200": "",  # 연속조회키200
        }

        res = requests.get(URL, headers=headers, params=qparams)
        time.sleep(0.1)

        output = res.json()['output']
        if len(output) > 0:  # 미체결이 있으면 취소
            code = output[0]["pdno"]
            orderNO = output[0]["odno"]
            orderCNT = output[0]["ft_ord_qty"]
            PATH = "/uapi/overseas-stock/v1/trading/order-rvsecncl"
            URL = f"{self.URL_BASE}/{PATH}"
            headers = {
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "appKey": self.APP_KEY,
                "appSecret": self.APP_SECRET,
                "tr_id": self.cancel_trid,
            }
            qparams = {
                "CANO": self.CANO,
                "ACNT_PRDT_CD": "01",
                "OVRS_EXCG_CD": "NASD",  # 해외거래소코드
                "PDNO": code,  # 주식 코드
                "ORGN_ODNO": orderNO,  # 원 주문 번호
                "RVSE_CNCL_DVSN_CD": "02",  # 01 정정, 02 취소
                "ORD_QTY": orderCNT,  # 주문수량
                "OVRS_ORD_UNPR": "0",  # 해외주문단가
            }

            res = requests.get(URL, headers=headers, params=qparams)
            time.sleep(0.1)

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = "%s - %s 매수등록 취소\n" % (now, code)
            self.log_print(sendText)
            self.send_msg(sendText)

    def startDeal(self):
        nowTime = int(datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%H%M'))
        if nowTime >= 1545:
            for code in self.stocks:
                nowprice = self.stocks[code]["now_price"]  # 현재가
                if self.stocks[code]["buy_count"] > 0:
                    if nowprice < self.stocks[code]["5ma"]:
                        self.dealStock(code, self.stocks[code]["buy_count"], nowprice, self.sell_trid, "33")

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - 종가가 5일이평선 이하로 매도 %s\n" % (now, code)
                        self.log_print(sendText)
                        self.send_msg(sendText)

                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sendText = "%s - 매매종료(현지시간 : %s)\n" % (now, nowTime)
                print(sendText)
                self.log_print(sendText)
                self.send_msg(sendText)
        else :
            for code in self.stocks:
                nowprice = self.stocks[code]["now_price"]  # 현재가
                if self.stocks[code]["buy_count"] == 0:
                    if nowprice >= self.stocks[code]["target_price"] and nowprice >= self.stocks[code]["5ma"]:
                        cnt = self.eachAssets // nowprice
                        self.dealStock(code, cnt, nowprice, self.buy_trid, "00")

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - 매수 등록 %s\n" % (now, code)
                        print(sendText)
                        self.log_print(sendText)
                        self.send_msg(sendText)

                else:
                    if nowprice < self.stocks[code]["buy_price"] * 0.985:
                        self.dealStock(code, self.stocks[code]["buy_count"], nowprice, self.sell_trid, "00")

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = '%s - 매도 등록 %s\n' % (now, code)
                        print(sendText)
                        self.log_print(sendText)
                        self.send_msg(sendText)

    def dealStock(self, code, cnt, price, tr_id, ord):
        PATH = "/uapi/overseas-stock/v1/trading/order"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_cont": "",
            "custtype": "P",
            "tr_id": tr_id
        }
        body = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",  # 해외거래소코드
            "PDNO": code,  # 주식 코드
            "ORD_QTY": str(cnt),  # 수량
            "OVRS_ORD_UNPR": str(price),  # 해외주문단가
            "ORD_SVR_DVSN_CD": "0",  # 주문서버구분코드
            "ORD_DVSN": ord,  # 시장가
        }
        if tr_id == "TTTT1006U":
            body["SLL_TYPE"] = "00"

        res = requests.post(URL, headers=headers, data=json.dumps(body))
        time.sleep(0.1)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = '%s - 매도 등록 %s : %s\n' % (now, code, res.json())
        print(sendText)
        self.log_print(sendText)

    def send_msg(self, msg):
        apiToken = "텔레그램 API 토큰"
        chatId = "쳇ID"
        bot = telegram.Bot(apiToken)
        bot.sendMessage(chatId, msg)

    def targetPrice(self):
        for code in self.stocks:
            # 일별 현재가 가져오기
            PATH = "/uapi/overseas-price/v1/quotations/dailyprice"
            URL = f"{self.URL_BASE}/{PATH}"
            headers = {
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "appKey": self.APP_KEY,
                "appSecret": self.APP_SECRET,
                "tr_id": "HHDFS76240000",
            }
            qparams = {
                "AUTH": "",     # 사용자권한정보 기본 null
                "EXCD": "NAS",  # 거래소코드
                "SYMB": code,   # 종목코드
                "GUBN": "0",    # 일/주/월구분
                "BYMD": "",     # 조회기준일자(YYYYMMDD) 공란 설정 시, 기준일 오늘 날짜로 설정
                "MODP": "0",    # 수정주가반영여부
            }
            res = requests.get(URL, headers=headers, params=qparams)
            time.sleep(0.1)

            result = res.json()['output2']
            df = DataFrame(result)

            # 현재가 설정
            self.stocks[code]["now_price"] = float(df['clos'][0])

            # 목표가 설정(변동성 돌파 전략)
            openprice = round(float(df.iloc[0]['open']), 4)
            interval = round(float(df.iloc[1]['high']), 4) - round(float(df.iloc[1]['low']), 4)
            k_range = interval * 0.5
            targetPrice = openprice + k_range
            self.stocks[code]["target_price"] = round(float(targetPrice), 4)

            # 5일 이평선 설정
            df_close = df['clos']
            self.stocks[code]["5ma"] = df_close.rolling(window=5).mean()[4]

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = '%s - 목표가 설정결과 : %s\n' % (now, self.stocks)
            print(sendText)
            self.log_print(sendText)

    def getStocksList(self):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-present-balance"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": self.balance_trid,
        }
        qparams = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "WCRC_FRCR_DVSN_CD": "02", # 원화외화구분코드 01 : 원화, 02 : 외화
            "NATN_CD": "840",          # 국가코드 : 미국
            "TR_MKET_CD": "00",        # 거래시장 코드 : 전체
            "INQR_DVSN_CD": "00",      # 조회구분 : 전체
        }

        res = requests.get(URL, headers=headers, params=qparams)
        time.sleep(0.1)

        output1 = res.json()['output1']
        if len(output1) > 0:
            code_no = output1[0]['pdno']
            self.stocks[code_no]['buy_count'] = round(float(output1[0]['ccld_qty_smtl1']))
            self.stocks[code_no]['buy_price'] = float(output1[0]['avg_unpr3'])

        output2 = res.json()['output2']
        Withholdings = float(output2[0]['frcr_dncl_amt_2'])  # 외화 예수금
        self.myTotalAssets = Withholdings
        self.eachAssets = round(self.myTotalAssets / len(self.stocks), 4)  # 주식 한종목당 배정 금액

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = '%s - 총평가금액 : %s원(주식당 %s원)\n' % (now, self.myTotalAssets, self.eachAssets)
        print(sendText)
        self.log_print(sendText)

    def log_print(self, msg):
        date = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d')
        file_name = self.path + 'log/kisUSA_' + date + '.log'
        log_file = open(file_name, 'a')
        log_file.write(msg)

    def kisConnect(self):  # ACCESS_TOKEN 구하기
        nowDate = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y%m%d')  # 거래일

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

    def modeCheck(self):
        with open(self.path + 'keysKIS.txt', 'r') as file:  # kis 접속을 위한 키
            keys = file.readlines()

        self.CANO = keys[3].strip()       # 계좌번호
        self.APP_KEY = keys[4].strip()    # 앱키
        self.APP_SECRET = keys[5].strip() # 앱비밀키

        self.URL_BASE = "https://openapi.koreainvestment.com:9443"  # 도메인
        self.balance_trid = "CTRP6504R"
        self.buy_trid = 'TTTT1002U'  # 매수 주문
        self.sell_trid = 'TTTT1006U'  # 매도 주문
        self.notsigned_trid = 'TTTS3018R'  # 미체결 확인
        self.cancel_trid = 'TTTT1004U'  # 주문 취소

if __name__ == "__main__":
    USA_holidays = holidays.USA()
    date = datetime.datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')

    if date not in USA_holidays:
        KisVbos = KisVBOS()
    else:
        sendText = '오늘은 휴일입니다.'
        print(sendText)
        exit()
