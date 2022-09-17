import pyupbit  # 설치
import time
import math
import datetime  # 설치
import telepot  # 설치
from pyupbit import WebSocketManager

class UpbitPy():
    def __init__(self):
        now = datetime.datetime.now()
        self.check_fail = 'log/' + now.strftime('%Y-%m-%d_%H%M%S') + '.txt'
        file = open(self.check_fail, 'w', encoding="UTF8")
        file.close()

        # 변수 생성
        self.tickers = {"KRW-BTC": [0, 0, 0, 0], "KRW-ETH": [0, 0, 0, 0]}  # ['매수목표가','손절가','매수가','보유수']
        self.buyCount = len(self.tickers)  # 코인 갯수
        self.chkTime = int(datetime.datetime.now().strftime('%M'))
        self.KRWbalances = {}  # 코인별 매수 금액
        self.MAline = {}  # 코인별 이평선

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = "%s - 변동성 돌파 전략을 시작합니다.\n" % now
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()
        self.send_msg(sendText)

        file = open("keys.txt", "r")
        keys = file.readlines()
        access_key = keys[0].strip()
        secret_key = keys[1].strip()
        self.upbit = pyupbit.Upbit(access_key, secret_key)
        file.close()

        self.set_CoinsPrice()  # 목표가 계산
        self.get_MAline()  # 이동평균 계산
        self.checkNowMytickers()  #보유현황 확인

        self.checkMA()  # 이동평균 이상확인

        self.wm = WebSocketManager("ticker", list(self.tickers.keys()))
        while True:
            try:
                data = self.wm.get()
                # print(data['code'], data['trade_price'])

                ticker = data['code']
                nowPrice = data['trade_price']

                if self.tickers[ticker][2] == 0:
                    if nowPrice > int(self.tickers[ticker][0]):
                        ret = self.upbit.buy_market_order(ticker, self.KRWbalance[ticker])  # 매수 (티커, 금액)
                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - %s 매수(현재가(%s)가 매수 목표가(%s) 이상)\n" % (now, ticker, nowPrice, self.tickers[ticker])
                        print(sendText)
                        file = open(self.check_fail, 'a')
                        file.write(sendText)
                        file.close()

                        time.sleep(1)
                        self.checkNowMytickers()  # 보유현황 체크

                else:
                    if nowPrice < float(self.tickers[ticker][1]):
                        ret = self.upbit.sell_market_order(ticker, self.tickers[ticker][3])  # 시장가 매도 (티커, 수량)
                        self.tickers[ticker][2] = 0
                        self.tickers[ticker][3] = 0
                        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        sendText = "%s - %s 매도(현재가(%s)가 하한가(%s) 아래)\n" % (now, ticker, nowPrice, self.tickers[ticker][3])
                        print(sendText)
                        file = open(self.check_fail, 'a')
                        file.write(sendText)
                        file.close()

                nowMinute = int(datetime.datetime.now().strftime('%M'))
                if nowMinute > self.chkTime:
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

                self.wm = WebSocketManager("ticker", list(self.tickers.keys()))  # 웹소켓 차단일 가능성이 있어 재 등록

    def sellCoin(self):
        self.checkNowMytickers()

        for ticker in self.tickers:
            if self.tickers[ticker][3] > 0:
                ret = self.upbit.sell_market_order(ticker, self.tickers[ticker][3])  # 시장가 매도 (티커, 수량)
            time.sleep(1)

        sendText = "전량매도 완료"
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()
        self.send_msg(sendText)

    def checkMA(self):
        for ticker in self.MAline:
            nowPrice = pyupbit.get_current_price(ticker)
            if nowPrice < max(self.MAline[ticker]):
                if self.tickers[ticker][3] > 0:
                    ret = self.upbit.sell_market_order(ticker, self.tickers[ticker][3])
                del self.tickers[ticker]

            time.sleep(0.1)

        if len(self.tickers) != 0:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = "%s - 현재가 구독 시작 : %s\n" % (now, self.tickers)
            print(sendText)
            file = open(self.check_fail, 'a')
            file.write(sendText)
            file.close()

        else:
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            sendText = "%s - 모든 코인이 현재가 이하로 종료\n" % now
            print(sendText)
            file = open(self.check_fail, 'a')
            file.write(sendText)
            file.close()
            self.send_msg(sendText)

            quit()

    def checkNowMytickers(self):
        balances = self.upbit.get_balances()  # 전체 잔고 조회
        KRWbalance = 0
        for balance in balances:
            if balance['currency'] == 'KRW':
                KRWbalance += float(balance['balance'])
            elif balance['avg_buy_price'] != '0':
                KRWbalance += float(balance['balance']) * float(balance['avg_buy_price'])
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

        for ticker in self.tickers:
            balance = float(KRWbalance) / self.buyCount  # 코인 갯수별 균등 매매
            self.KRWbalances[ticker] = (math.trunc(balance / 1000) * 1000) - 5000

        sendText = "보유 자산 : %s원(코인별 매수금액 : %s)\n" % (KRWbalance, self.KRWbalances)
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

    def get_MAline(self):  # 코인별 이평선 계산
        for ticker in self.tickers:
            df = pyupbit.get_ohlcv(ticker, count=30)

            ma = []
            close = df['close']
            ma.append(close.rolling(window=5).mean()[-2])
            ma.append(close.rolling(window=10).mean()[-2])

            self.MAline[ticker] = ma

            self.tickers[ticker][1] = df['low'].min()

            time.sleep(0.1)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = '%s - 이동평균 계산 : %s\n' % (now, self.tickers)
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

    def set_CoinsPrice(self):
        for ticker in self.tickers:
            # 코인별 매수 목표가 계산
            df = pyupbit.get_ohlcv(ticker, count=2)

            interval = df.iloc[0]['high'] - df.iloc[0]['low']
            k_range = interval * 0.5
            targetPrice = df.iloc[1]['open'] + k_range

            self.tickers[ticker][0] = targetPrice

            time.sleep(0.1)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sendText = '%s - 목표가 계산 : %s\n' % (now, self.tickers)
        print(sendText)
        file = open(self.check_fail, 'a')
        file.write(sendText)
        file.close()

    def send_msg(self, msg):
        file = open("telepot.txt", "r")
        codes = file.readlines()
        apiToken = codes[0].strip()
        chatId = codes[1].strip()
        self.bot = telepot.Bot(apiToken)
        self.bot.sendMessage(chatId, msg)

if __name__ == "__main__":
    upbitpy = UpbitPy()
