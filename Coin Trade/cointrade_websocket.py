# WebSocket 을 이용한 코인 거래

import pyupbit  # 설치
from pyupbit import WebSocketManager
import time
import datetime  # 설치
import math
import telepot  # 설치

class UpbitPy():
    def __init__(self):
        while True:
            # super().__init__()

            # 로그파일 생성
            now = datetime.datetime.now()
            self.check_fail = 'log//' + now.strftime('%Y-%m-%d_%H%M%S') + '.txt'
            file = open(self.check_fail, 'w', encoding="UTF8")
            file.close()

            # 변수 생성
            self.tickers = {"KRW-BTC": [0, 0, 0, 0], "KRW-ETH": [0, 0, 0, 0]}  # ['매수목표가','매도목표가' '매수가','보유수']
            self.buyCount = len(self.tickers)  # 코인 갯수
            self.chkTime = int(datetime.datetime.now().strftime('%M'))
            self.KRWbalance = {}  # 코인별 매수 금액
            self.MAline = {}  # 코인별 이평선

            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = "%s - 변동성 돌파 전략을 시작합니다.\n" % now
            print(sendText)
            file = open(self.check_fail, 'a')
            file.write(sendText)
            file.close()
            self.send_msg(sendText)

            try:
                # API 접속
                file = open("keys.txt", "r")
                keys = file.readlines()
                access_key = keys[0].strip()
                secret_key = keys[1].strip()
                self.upbit = pyupbit.Upbit(access_key, secret_key)
                file.close()

                self.set_CoinsPrice()  # 목표가 계산
                self.checkNowMytickers()  # 보유현황 체크
                self.get_MAline()  # 이평선

                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sendText = "%s - 현재가 구독 시작\n" % now
                print(sendText)
                file = open(self.check_fail, 'a')
                file.write(sendText)
                file.close()
                self.send_msg(sendText)

                self.wm = WebSocketManager("ticker", list(self.tickers.keys()))
                while True:
                    data = self.wm.get()
                    print(data['code'], data['trade_price'])

                    if len(data) > 0:
                        ticker = data['code']
                        nowPrice = data['trade_price']

                        MAmax = max(self.MAline[ticker].values())

                        if self.tickers[ticker][2] == 0:
                            if nowPrice > int(self.tickers[ticker][0]) and nowPrice > MAmax:
                                ret = self.upbit.buy_market_order(ticker, self.KRWbalance[ticker])  # 매수 (티커, 금액)

                                if list(ret.keys())[0] == 'error':
                                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    sendText = "%s - %s 매수간 오류 발생\n" % (now, ticker)
                                    print(sendText)
                                    file = open(self.check_fail, 'a')
                                    file.write(sendText)
                                    file.close()
                                    self.send_msg(sendText)

                                else:
                                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    sendText = "%s - %s 매수(현재가(%s)가 매수 목표가(%s), 최대 이평선(%s) 이상)\n" % (
                                        now, ticker, nowPrice, self.tickers[ticker], MAmax)
                                    print(sendText)
                                    file = open(self.check_fail, 'a')
                                    file.write(sendText)
                                    file.close()
                                    self.send_msg(sendText)

                                    self.tickers[ticker][2] = nowPrice
                                    time.sleep(1)
                                    self.checkNowMytickers()  # 보유현황 체크

                        else:
                            goalPrice = float(self.tickers[ticker][0])  # 목표가 설정

                            if nowPrice > goalPrice:
                                self.tickers[ticker][0] = nowPrice
                            elif nowPrice < float(self.tickers[ticker][1]):
                                ret = self.upbit.sell_market_order(ticker, self.tickers[ticker][3])  # 시장가 매도 (티커, 수량)

                                if list(ret.keys())[0] == 'error':
                                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    sendText = "%s - %s 매수간 오류 발생\n" % (now, ticker)
                                    print(sendText)
                                    file = open(self.check_fail, 'a')
                                    file.write(sendText)
                                    file.close()
                                    self.send_msg(sendText)

                                else:
                                    self.tickers[ticker][2] = 0
                                    self.tickers[ticker][3] = 0
                                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                    sendText = "%s - %s 매도(현재가(%s)가 하한가(%s) 아래)\n" % (now, ticker, nowPrice, self.tickers[ticker][3])
                                    print(sendText)
                                    file = open(self.check_fail, 'a')
                                    file.write(sendText)
                                    file.close()
                                    self.send_msg(sendText)

                        nowMinute = int(datetime.datetime.now().strftime('%M'))
                        if nowMinute > self.chkTime:
                            self.wm.terminate()
                            if nowMinute == 1:
                                sendText = "매매 대기 중 - %s\n" % int(datetime.datetime.now().strftime('%H'))
                                print(sendText)
                                file = open(self.check_fail, 'a')
                                file.write(sendText)
                                file.close()

                                self.checkNowMytickers()

                            if nowMinute == 59:
                                self.chkTime = -1
                            else:
                                self.chkTime = nowMinute

                            self.wm = WebSocketManager("ticker", list(self.tickers.keys()))

                        if int(datetime.datetime.now().strftime('%H%M')) == 859:
                            sendText = "매수종료 및 전량매도\n"
                            print(sendText)

                            self.sellCoin()  # 전체 매도

                            file = open(self.check_fail, 'a')
                            file.write(sendText)
                            file.close()
                            self.send_msg(sendText)

                            quit()

                    time.sleep(0.1)

            except Exception as e:
                now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sendText = '%s - 예외 발생 : %s\n' % (now, e)
                print(sendText)
                file = open(self.check_fail, 'a')
                file.write(sendText)
                file.close()
                self.send_msg(sendText)
                time.sleep(1)

    def checkNowMytickers(self):
        balances = self.upbit.get_balances()  # 전체 잔고 조회
        buyKRWbalance = 0
        for balance in balances:
            buyKRWbalance += float(balance['balance']) * float(balance['avg_buy_price'])
            if balance['currency'] != 'KRW' and balance['avg_buy_price'] != '0':
                ticker = balance['unit_currency'] + "-" + balance['currency']
                price = float(balance['avg_buy_price'])
                coinBalance = float(balance['balance'])
                self.tickers[ticker][2] = price
                self.tickers[ticker][3] = coinBalance

        sendText = "보유코인 : %s\n" % self.tickers
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()
        # self.send_msg(sendText)

        KRWbalances = self.upbit.get_balance("KRW")  # 잔고 조회
        KRWbalances += buyKRWbalance

        sendText = "보유 자산 : %s원\n" % KRWbalances
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

        for ticker in self.tickers:
            balance = float(KRWbalances) / self.buyCount  # 코인 갯수별 균등 매매
            self.KRWbalance[ticker] = (math.trunc(balance / 1000) * 1000) - 5000

    def sellCoin(self):
        sendText = "매도 시작"
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

        balances = self.upbit.get_balances()  # 잔고 조회

        for balance in balances:
            if balance['currency'] != 'KRW' and balance['avg_buy_price'] != '0':
                ticker = balance['unit_currency'] + "-" + balance['currency']
                count = balance['balance']

                ret = self.upbit.sell_market_order(ticker, count)  # 시장가 매도 (티커, 수량)
                time.sleep(1)

                if list(ret.keys())[0] == 'error':
                    sendText = "%s 매도간 오류 발생" % ticker
                    print(sendText)
                    file = open(self.check_fail, 'a')
                    file.write(sendText)
                    file.close()
                    self.send_msg(sendText)
                else:
                    now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    print("%s - 보유중인 %s 매도" % (now, ticker))

                self.tickers[ticker][2] = 0
                self.tickers[ticker][3] = 0

        sendText = "전량매도 완료"
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()
        self.send_msg(sendText)

    def set_CoinsPrice(self):
        for ticker in self.tickers:
            # 코인별 매수 목표가 계산
            df = pyupbit.get_ohlcv(ticker, count=10, interval="minute240")

            interval = df.iloc[-2]['high'] - df.iloc[-2]['low']
            k_range = interval * 0.5
            targetPrice = df.iloc[-1]['open'] + k_range

            # ['목표가','매도가' '매수가','보유수']
            if targetPrice > df.iloc[-1]['close']:  # 장 도중 재실행시 목표가격 설정
                self.tickers[ticker][0] = targetPrice
            else:
                self.tickers[ticker][0] = df.iloc[-1]['close'] + 10000

            self.tickers[ticker][1] = df['low'].min()

            time.sleep(0.1)

        sendText = '목표가 계산 : %s\n' % self.tickers
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

    def get_MAline(self):  # 코인별 이평선 계산
        for ticker in self.tickers:
            df = pyupbit.get_ohlcv(ticker, count=11, interval="minute240")

            ma = {}
            close = df['close']
            ma[0] = close.rolling(window=5).mean()[-2]
            ma[1] = close.rolling(window=10).mean()[-2]

            self.MAline[ticker] = ma
            time.sleep(0.1)

    def send_msg(self, msg):
        file = open("telepot.txt", "r")
        codes = file.readlines()
        apiToken = codes[0].strip()
        chatId = codes[1].strip()
        self.bot = telepot.Bot(apiToken)
        self.bot.sendMessage(chatId, msg)

if __name__ == "__main__":
    upbitpy = UpbitPy()
