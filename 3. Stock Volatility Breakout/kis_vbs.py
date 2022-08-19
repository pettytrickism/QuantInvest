import requests   # 설치 필요
import datetime, time
from pandas import DataFrame   # 설치 필요
import json
import holidays

class KisTradePy():
    def __init__(self):
        with open('kiskeys.txt', 'r') as file:  # kis 접속을 위한 키
            keys = file.readlines()
            self.APP_KEY = keys[0].strip()  # 앱키
            self.APP_SECRET = keys[1].strip()  # 앱비밀키
            self.CANO = keys[2].strip()  # 계좌번호

        # 매매시간 대기
        nowTime = int(datetime.datetime.now().strftime('%H%M'))
        while nowTime not in range(900, 1515):
            print('거래시간 아님 (%s)' % nowTime)
            time.sleep(60)
            nowTime = int(datetime.datetime.now().strftime('%H%M'))

        sendText = "매매시작(현재시간 : %s)" % nowTime
        print(sendText)

        # 로그파일 저장
        now = datetime.datetime.now()
        self.check_fail = 'log//kis_' + now.strftime('%Y-%m-%d_%H%M%S') + '.txt'
        file = open(self.check_fail, 'w', encoding="UTF8")
        file.close()

        # 변수 설정
        # "005930" 삼성전자, "373220" LG에너지솔루션, "000660" SK하이닉스, "035420" NAVER, "035720" 카카오
        self.stocks = {"005930": [0, 0, 0], "373220": [0, 0, 0], "000660": [0, 0, 0], "035420": [0, 0, 0], "035720": [0, 0, 0]}
        self.myStocks = 0  # 보유 주식갯수
        self.myTotalAssets = 0  # 총 자산
        self.eachAssets = 0  # 주식 한종목당 배정 금액
        self.numStock = len(self.stocks)  # 포트폴리오 갯수

        # domain info
        # 실전투자 : self.URL_BASE = "https://openapi.koreainvestment.com:9443"
        # 모의투자서비스 : self.URL_BASE = "https://openapivts.koreainvestment.com:29443"
        self.URL_BASE = "https://openapivts.koreainvestment.com:29443"  # 모의투자서비스

        self.ACCESS_TOKEN = self.kisConnect()  # kis access_token 취득
        self.getStocksList()  # 보유 주식 및 예수금 확인
        self.targetPrice()  # 목표가 설정
        print(self.stocks)

        self.startDeal()  # 거래 시작

    def kisConnect(self):      # ACCESS_TOKEN 구하기
        headers = {"content-type": "application/json"}
        body = {"grant_type": "client_credentials",
                "appkey": self.APP_KEY,
                "appsecret": self.APP_SECRET}
        PATH = "oauth2/tokenP"
        URL = f"{self.URL_BASE}/{PATH}"

        res = requests.post(URL, headers=headers, data=json.dumps(body))

        ACCESS_TOKEN = res.json()["access_token"]
        return ACCESS_TOKEN

    def hashkey(self, datas):   # 해쉬키 구하기
        PATH = "uapi/hashkey"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            'content-Type' : 'application/json',
            'appKey' : self.APP_KEY,
            'appSecret' : self.APP_SECRET,
        }
        res = requests.post(URL, headers=headers, data=json.dumps(datas))
        rescode = res.status_code
        if rescode == 200:
            hashkey = res.json()["HASH"]
        else:
            sendText = '해쉬키 오류로 재실행 필요'
            print(sendText)
            file = open(self.check_fail, 'a')
            file.write(sendText)
            file.close()
            hashkey = 0  # hashkey 가 0일때 재실행 필요
        return hashkey

    def getStocksList(self):
        PATH = "/uapi/domestic-stock/v1/trading/inquire-balance"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
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
        headers = {
            "content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": "VTTC8434R", # [실전투자] TTTC8434R, [모의투자] VTTC8434R
            "custtype": "P", # 개인
            "hashkey": self.hashkey(data)
        }

        res = requests.get(URL, headers=headers, params=data)
        rescode = res.status_code

        if rescode == 200:
            myStocks = res.json()['output1']
            if len(myStocks) > 0:
                for myStock in myStocks:
                    self.stocks[myStock['pdno']][2] = round(float(myStock['pchs_avg_pric']))
                    self.stocks[myStock['pdno']][3] = int(myStock['hldg_qty'])

            self.myTotalAssets = int(res.json()['output2'][0]['tot_evlu_amt'])
            print('총평가금액 : %s원' % self.myTotalAssets)

            self.eachAssets = round(self.myTotalAssets / self.numStock)  # 주식 한종목당 배정 금액
            print('주식 한종목당 배정 금액 : %s원' % self.eachAssets)

        else:
            print("Error Code : " + str(rescode) + " | " + res.text)

    def targetPrice(self):
        print('목표가 및 이평선 계산')
        for stock in self.stocks.keys():
            # 일별 현재가 가져오기
            PATH = "uapi/domestic-stock/v1/quotations/inquire-daily-price"
            URL = f"{self.URL_BASE}/{PATH}"
            headers = {
                "Content-Type": "application/json",
                "authorization": f"Bearer {self.ACCESS_TOKEN}",
                "appKey": self.APP_KEY,
                "appSecret": self.APP_SECRET,
                "tr_id": "FHKST01010400"
            }
            params = {
                "fid_cond_mrkt_div_code": "J",  # 종목 구분
                "fid_input_iscd": stock,  # 주식 코드
                "fid_org_adj_prc": "1",  # 수정주가
                "fid_period_div_code": "D"  # 일자별 데이터
            }
            res = requests.get(URL, headers=headers, params=params)
            result = res.json()['output']
            df = DataFrame(result)

            # 목표가 설정(변동성 돌파 전략)
            openprice = int(df.iloc[0]['stck_oprc'])
            interval = int(df.iloc[1]['stck_hgpr']) - int(df.iloc[1]['stck_lwpr'])
            k_range = interval * 0.5
            targetPrice = openprice + k_range
            if targetPrice > int(df.iloc[0]['stck_clpr']):
                self.stocks[stock][0] = int(targetPrice)
            else:
                self.stocks[stock][0] = int(df.iloc[0]['stck_clpr']) * 1.03

            # 5일 이평선 설정
            df_close = df['stck_clpr']
            self.stocks[stock][1] = df_close.rolling(window=5).mean()[4]

    def dealStock(self, code, cnt, tr_id):
        PATH = "uapi/domestic-stock/v1/trading/order-cash"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "PDNO": code,  # 주식 코드
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(cnt),  # 수량
            "ORD_UNPR": "0",  # 주문 단가
        }
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "custtype": "P",
            "hashkey": self.hashkey(data),
            "tr_id": tr_id
        }

        res = requests.post(URL, headers=headers, data=json.dumps(data))
        print(res.json())

    def startDeal(self):
        while True:
            for code in self.stocks:
                nowprice = int(self.getNowPrice(code))  # int(stockInfo[1])  # 현재가
                if self.stocks[code][2] == 0:
                    if nowprice >= self.stocks[code][0] and nowprice >= self.stocks[code][1]:
                        cnt = self.eachAssets // nowprice
                        dealResult =  self.dealStock(code, cnt, "VTTC0802U")
                        # [실투자] TTTC0802U: 주식 현금 매수 주문, [모의투자] VTTC0802U: 주식 현금 매수 주문

                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - 매수 %s\n" % (now, code)
                        print(sendText)
                        self.bot.sendMessage(self.chatId, sendText)
                        self.stocks[code][2] = cnt

                else:
                    if nowprice > self.stocks[code][0]:
                        self.stocks[code][0] = nowprice
                    elif nowprice < self.stocks[code][0] * 0.97:
                        self.dealStock(code, self.stocks[code][2], "VTTC0801U")
                        # [실투자] TTTC0801U: 주식 현금 매도 주문, [모의투자] VTTC0801U: 주식 현금 매도 주문

                        sendText = '%s 매도\n' % code
                        print(sendText)
                        file = open(self.check_fail, 'a')
                        file.write(sendText)
                        file.close()

                        self.stocks[code][2] = 0

                time.sleep(0.3)

            nowMinute = int(datetime.datetime.now().strftime('%M')) % 30
            if nowMinute > self.chkTime:
                if nowMinute == 0:

                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    sendText = "%s - 매매 대기 중 - %s\n" % (now, datetime.datetime.now().strftime('%H:%M'))
                    print(sendText)

                if nowMinute == 29:
                    self.chkTime = -1
                else:
                    self.chkTime = nowMinute

            nowTime = int(datetime.datetime.now().strftime('%H%M'))
            if nowTime >= 1519:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sendText = "%s - 매매종료(현재시간 : %s)\n" % (now, nowTime)
                print(sendText)
                self.bot.sendMessage(self.chatId, sendText)

                exit()

if __name__ == "__main__":
    kr_holidays = holidays.KR()
    date = datetime.date.today().strftime('%Y-%m-%d')

    if date not in kr_holidays:
        kisTrade = KisTradePy()
    else:
        print('오늘은 휴일입니다.')
        exit()
