import datetime
import time

from pandas_datareader import data as pdr   # 설치필요
import FinanceDataReader as fdr  # pip install finance-datareader
import warnings
warnings.filterwarnings('ignore')

class etfStrategy():
    def getVAA(self):
        aggressive = {"SPY" : 0,"EFA" : 0, "EEM" : 0, "AGG" : 0}
        msCount = 0
        print("공격형 자산의 모멘텀 스코어를 계산합니다.")
        for ticker in aggressive:
            aggressive[ticker] = self.getMomentumScore(ticker)
            if aggressive[ticker] >= 0: msCount += 1

        if msCount == 4:
            print("공격형 자산의 모멘텀 스코어가 모두 0이상 으로 최대 모멘텀 스코어인 자산 선택")
            return max(aggressive, key=aggressive.get)
        else:
            print("공격형 자신의 모멘텀 스코어중 하나이상이 0이하로 안전 자산의 모멘텀 스코어 계산")
            safety = {"LQD": 0, "IEF": 0, "SHY": 0}

            for ticker in safety:
                safety[ticker] = self.getMomentumScore(ticker)
            print("안전자신중 최대 모멘텀 스코어 자산 선택")
            return max(safety, key=safety.get)

    def getMomentumScore(self, ticker):
        pr1 = self.profitRate(ticker, interval='mo', method='rate', period=1)
        pr3 = self.profitRate(ticker, interval='mo', method='rate', period=3)
        pr6 = self.profitRate(ticker, interval='mo', method='rate', period=6)
        pr12 = self.profitRate(ticker, interval='mo', method='rate', period=12)
        ms = 12*pr1 + 4*pr3 + 3*pr6 + pr12
        print("%s의 모멘텀 스코어 : %s(1개월 : %s, 3개월 : %s, 6개월 : %s, 12개월 : %s)" % (ticker, ms, pr1, pr3, pr6, pr12))

        return ms

    def getLAA(self):
        unRate = self.getUnRate()
        snpRate = self.getSnpRate()

        if unRate and snpRate:
            return "SHY"
        else :
            return "QQQ"

    def getSnpRate(self):
        snpMA200 = self.profitRate("^GSPC", interval='d', method='ma', period=200)
        snpNow = self.profitRate("^GSPC", interval='d', method='price')
        print("S&P 지수 200일 이동평균 : %s" % snpMA200)
        print("현재 S&P 지수 : %s" % snpNow)

        return snpNow < snpMA200

    def getUnRate(self):
        print("미국 실업률 계산")

        df = fdr.DataReader(['UNRATE'], start='2021-06', data_source='fred')
        unRateMA12 = df['UNRATE'].mean()
        unRateNow = df['UNRATE'][-1]

        print("미국 실업률 12개월 이동평균 : %s" % unRateMA12)
        print("현재 미국 실업률 : %s" % unRateNow)

        return unRateNow > unRateMA12

    def getDualMomentum(self):
        spy_rate12 = self.profitRate("SPY", interval='mo', method='rate', period=12)
        efa_rate12 = self.profitRate("EFA", interval='mo', method='rate', period=12)
        bil_rate12 = self.profitRate("BIL", interval='mo', method='rate', period=12)
        print("SPY 12개월 수익률 : %s" % spy_rate12)
        print("EFA 12개월 수익률 : %s" % efa_rate12)
        print("BIL 12개월 수익률 : %s" % bil_rate12)

        if spy_rate12 > bil_rate12:
            if spy_rate12 >= efa_rate12:
                buy_etf = "SPY"
            else:
                buy_etf = "EFA"
        else:
            buy_etf = "AGG"

        return buy_etf

    def profitRate(self, ticker='0', interval='d', method='price', period=0):
        year = int(datetime.datetime.now().strftime('%Y'))
        month = int(datetime.datetime.now().strftime('%m'))
        startDate = "%s-%s-01" % (year-1, month-1)
        endDate = datetime.datetime.now().strftime('%Y-%m-%d')
        data = pdr.get_data_yahoo(ticker, start=startDate, end=endDate, interval=interval)
        time.sleep(0.2)

        if method == "price":
            result = data["Close"][-1]
        elif method == "rate":
            result = data["Close"][-1] / data["Close"][(len(data)-1)-period] - 1
        elif method == "sma":
            result = data["Close"][(len(data)) - period:].mean()
        elif method == "ma":
            result = data["Close"].rolling(window=period).mean()[-1]
        else:
            result = 0

        return result
