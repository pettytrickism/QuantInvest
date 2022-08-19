import json
import telepot  # 설치필요
import requests   # 설치 필요

class kisAPI():
    def __init__(self):
        with open('telepot.txt', 'r') as file:  # 텔레그램 등록
            keys = file.readlines()
            apiToken = keys[0].strip()
            myId = keys[1].strip()
        self.bot = telepot.Bot(apiToken)

        with open('kis.txt', 'r') as file:  # kis 접속을 위한 키
            keys = file.readlines()
            self.APP_KEY = keys[0].strip()  # 앱키
            self.APP_SECRET = keys[1].strip()  # 앱비밀키
            self.CANO = keys[2].strip()  # 계좌번호

        # domain info
        # 실전투자 : URL_BASE = "https://openapi.koreainvestment.com:9443"
        # 모의투자서비스 : URL_BASE = "https://openapivts.koreainvestment.com:29443"
        self.URL_BASE = "https://openapivts.koreainvestment.com:29443"  # 모의투자서비스
        self.ACCESS_TOKEN = self.kisConnect()  # kis access_token 취득

    def kisConnect(self):  # ACCESS_TOKEN 구하기
        headers = {"content-type": "application/json"}
        body = {"grant_type": "client_credentials",
                "appkey": self.APP_KEY,
                "appsecret": self.APP_SECRET}
        PATH = "oauth2/tokenP"
        URL = f"{self.URL_BASE}/{PATH}"
        res = requests.post(URL, headers=headers, data=json.dumps(body))
        ACCESS_TOKEN = res.json()["access_token"]
        return ACCESS_TOKEN

    def getHashkey(self, datas):  # 해쉬키 구하기
        PATH = "uapi/hashkey"
        URL = f"{self.URL_BASE}/{PATH}"
        headers = {
            'content-Type': 'application/json',
            'appKey': self.APP_KEY,
            'appSecret': self.APP_SECRET,
        }
        res = requests.post(URL, headers=headers, data=json.dumps(datas))
        rescode = res.status_code
        if rescode == 200:
            hashkey = res.json()["HASH"]
        else:
            print('해쉬키 오류로 재실행 필요')
            hashkey = 0  # hashkey 가 0일때 재실행 필요
        return hashkey

    def getStocksList(self):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-balance"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",  # 해외거래소코드
            "TR_CRCY_CD": "USD", # 거래통화코드
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        headers = {
            "content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": "VTTT3012R",  # [실전투자] JTTT3012R(야간), TTTS3012R(주간) [모의투자] VTTT3012R(야간), VTTS3012R(주간)
            "custtype": "P",  # 개인
            "hashkey": self.getHashkey(data),
        }

        res = requests.get(URL, headers=headers, params=data)
        rescode = res.status_code

        if rescode == 200:
            # print(res.headers)
            # print(str(rescode) + " | " + res.text)
            havingStocks = res.json()['output1']
            print('보유 주식 : %s' % havingStocks)

        else:
            print("Error Code : " + str(rescode) + " | " + res.text)
            havingStocks = []
        return havingStocks

    def getTotalAssets(self):
        # 해외주식 체결기준현재잔고
        PATH = "/uapi/overseas-stock/v1/trading/inquire-present-balance"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "WCRC_FRCR_DVSN_CD": "02", # 원화외화구분코드 01 : 원화, 02 : 외화
            "NATN_CD": "840",   # 국가코드
            "TR_MKET_CD": "00",
            "INQR_DVSN_CD": "00",
        }
        headers = {
            "content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": "VTRP6504R",  # [실전투자] CTRP6504R
            "tr_cont": "",   # 최초 공백
            "custtype": "P",  # 개인
            "hashkey": self.getHashkey(data),
        }

        res = requests.get(URL, headers=headers, params=data)
        rescode = res.status_code

        if rescode == 200:
            print(res.headers)
            print(str(rescode) + " | " + res.text)
            output2 = res.json()['output2']
            exRate = float(output2[3]['frst_bltn_exrt'])  #
            output3 = res.json()['output3']
            amount = float(output3['tot_asst_amt'])  # 총평가금액
            myTotalAssets = amount / exRate  # 5% 정도 예비
            print('외화 총금액 : %s $' % myTotalAssets)

        else:
            print("Error Code : " + str(rescode) + " | " + res.text)
            myTotalAssets = -1

        return myTotalAssets

    def sellStock(self, code, qty, tr_id):
        PATH = "/uapi/overseas-stock/v1/trading/order"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",
            "PDNO": code,  # 주식 코드
            "ORD_QTY": qty,
            "OVRS_ORD_UNPR": "",
            "SLL_TYPE": "00",
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "31",  # 장개시 시장가 거래
        }
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": tr_id,
            "tr_cont": "",
            "custtype": "P",
            "hashkey": self.getHashkey(data),
        }
        # [실투자] JTTT1002U : 미국 매수 주문, JTTT1006U : 미국 매도 주문
        # [모의투자] VTTT1002U : 미국 매수 주문, VTTT1001U : 미국 매도 주문

        res = requests.post(URL, headers=headers, data=json.dumps(data))
        return  res.json()

    def buyStock(self, code, qty, price, tr_id):
        PATH = "/uapi/overseas-stock/v1/trading/order"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",
            "PDNO": code,  # 주식 코드
            "ORD_QTY": qty,
            "OVRS_ORD_UNPR": price,
            "ORD_SVR_DVSN_CD": "0",
            "ORD_DVSN": "00",  # 시장가 거래 없음
        }
        headers = {
            "Content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": tr_id,
            "tr_cont": "",
            "custtype": "P",
            "hashkey": self.getHashkey(data),
        }
        # [실투자] JTTT1002U : 미국 매수 주문, JTTT1006U : 미국 매도 주문
        # [모의투자] VTTT1002U : 미국 매수 주문, VTTT1001U : 미국 매도 주문

        res = requests.post(URL, headers=headers, data=json.dumps(data))
        return  res.json()

    def checkStockDeal(self):
        PATH = "/uapi/overseas-stock/v1/trading/inquire-nccs"
        URL = f"{self.URL_BASE}/{PATH}"
        data = {
            "CANO": self.CANO,
            "ACNT_PRDT_CD": "01",
            "OVRS_EXCG_CD": "NASD",  # 해외거래소코드
            "TR_CRCY_CD": "USD", # 거래통화코드
            "SORT_SQN": "DS",  # 정렬순서
            "CTX_AREA_FK200": "",
            "CTX_AREA_NK200": "",
        }
        headers = {
            "content-Type": "application/json",
            "authorization": f"Bearer {self.ACCESS_TOKEN}",
            "appKey": self.APP_KEY,
            "appSecret": self.APP_SECRET,
            "tr_id": "VTTS3018R ",  # [실전투자] JTTT3018R(야간), TTTS3018R(주간) [모의투자] VTTT3018R(야간), VTTS3018R(주간)
            "custtype": "P",  # 개인
            "hashkey": self.getHashkey(data),
        }

        res = requests.get(URL, headers=headers, params=data)
        rescode = res.status_code

        if rescode == 200:
            # print(res.headers)
            # print(str(rescode) + " | " + res.text)
            dealStocks = len(res.json()['output1'])   ######################
            print('거래중인 주식 : %s' % dealStocks)

        else:
            print("Error Code : " + str(rescode) + " | " + res.text)
            dealStocks = []
        return dealStocks
