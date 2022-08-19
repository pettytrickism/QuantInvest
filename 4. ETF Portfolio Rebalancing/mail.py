import holidays           # 설치필요
import sqlite3
from pandas import DataFrame
from kis.kisAPI import *
from kis.etfStrategy import *

class investPy():
    def __init__(self):
        # DB 연결
        DBPath = 'ETFStrategy.db'  # DB 파일위치
        connect = sqlite3.connect(DBPath, isolation_level=None)

        self.etf = etfStrategy()
        strategyCount = 3 # 전략을 3가지 사용

        # 전략에 따른 포트폴리오 선택

        # 듀얼 모멘텀 전략
        # SPY, EFA, BIL 12개월 수익률
        # SPY > BIL -> (SPY, EFA 중 수익률 큰쪽) else AGG
        print("듀얼 모멘텀 전략을 계산합니다.")
        dualMomentum = self.etf.getDualMomentum()
        print("듀얼 모멘텀 : %s\n" % dualMomentum)

        # LAA 전략
        # IWD, GLD, IEF 25%씩 1년 리벨런싱
        # 25% - S&P500(^GSPC) < 200일 이평 and 미국 실업율 > 12개월 이평 -> SHY 아니면 QQQ
        print("LAA 전략을 계산합니다.")
        LAA = ["IWD", "GLD", "IEF"]
        LAA.append(self.etf.getLAA())
        print("LAA : %s\n" % LAA)

        # VAA 공격형 전략
        # 공격형 : SPY, EFA, EEM, AGG, 안전 : LQD, IEF, SHY
        # 모멘텀 스코어(12*1개월 수익률 + 4*3개월 + 2*6개월 + 12개월), 공격형 4개 모두 0이상 -> 최고 공격형 자산
        # 하나라도 0이하 -> 최고 안전자산
        print("VAA 공격형 전략을 계산합니다.")
        VAA = self.etf.getVAA()
        print("VAA Aggressive : %s\n" % VAA)

        rateETF = 1 / strategyCount
        portfolio = {}
        portfolio[dualMomentum] = [rateETF]
        for ticker in LAA:
            portfolio[ticker] = [rateETF * (1/len(LAA))]
        portfolio[VAA] = [rateETF]
        print("이번달 포트폴리오 : %s" % portfolio)
        self.dfPortfolio = DataFrame(portfolio, index=['Rate'])

        # 현재가, 매수량 계산
        price = []
        for ticker in self.dfPortfolio.columns:
            nowPrice = round(self.etf.profitRate(ticker, interval='d', method='price'), 2) - 0.01
            price.append(nowPrice)
        self.dfPortfolio.loc['Price'] = price

        # API 초기화
        self.kis = kisAPI()

        # 보유 주식, 예수금 조회
        havingStocks = self.kis.getStocksList()
        myTotalAssets = self.kis.getTotalAssets()

        print("외화 총금액 : %s" % myTotalAssets)
        if myTotalAssets == -1:
            print('외화 총금액 조회 오류 재실행 필요')
            exit()

        # ETF별 보유해야하는 수량(원화로 계산 나중에 달러로 수정 필요)
        buyingCount = []
        havingCount = []
        for ticker in self.dfPortfolio.columns:
            count = (myTotalAssets * self.dfPortfolio[ticker]["Rate"]) // self.dfPortfolio[ticker]["Price"]
            buyingCount.append(count)
            havingCount.append(0)
        self.dfPortfolio.loc['BuyingCount'] = buyingCount
        self.dfPortfolio.loc['HavingStocks'] = havingCount

        # 보유현황 확인
        if len(havingStocks) > 0:
            for myStock in havingStocks:
                if myStock['ovrs_pdno'] in self.dfPortfolio.columns:
                    self.dfPortfolio[ticker]['HavingStocks'] = int(myStock['ovrs_cblc_qty'])
                else:
                    self.dfPortfolio[ticker] = [0, 0, 0, int(myStock['ovrs_cblc_qty'])]

        # 수량 비교
        buy = []
        sell = []
        nowDate = []
        for ticker in self.dfPortfolio.columns:
            deal = self.dfPortfolio[ticker]['BuyingCount'] - self.dfPortfolio[ticker]['HavingStocks']
            if deal > 0:
                buy.append(deal)
                sell.append(0)
                nowDate.append(datetime.datetime.now().strftime('%Y%m%d%H%M'))
            else :
                buy.append(0)
                sell.append(abs(deal))
                nowDate.append(datetime.datetime.now().strftime('%Y%m%d%H%M'))

        self.dfPortfolio.loc['Buy'] = buy
        self.dfPortfolio.loc['Sell'] = sell
        self.dfPortfolio.loc['Date'] = nowDate

        print(self.dfPortfolio)
        self.dfPortfolio.T.to_sql('ETFStrategy', connect, if_exists='append')

        # 리벨런싱
        for ticker in self.dfPortfolio.columns:
            if self.dfPortfolio[ticker]['Sell'] > 0:
                print("%s %s 매도(시장가)" % (ticker, str(int(self.dfPortfolio[ticker]['Sell']))))
                # [실전투자] JTTT1006U: 미국 매도 주문, [모의투자] VTTT1001U : 미국 매도 주문
                deal = self.kis.sellStock(ticker, str(int(self.dfPortfolio[ticker]['Sell'])), "VTTT1001U")
                print(deal)  # code, qty, tr_id, price=0):
                time.sleep(0.2)

        # 매도 완료 확인 후 매수 시작
        dealStocks = self.kis.checkStockDeal()
        while dealStocks > 0:
            dealStocks = self.kis.checkStockDeal()
            time.sleep(10)

        for ticker in self.dfPortfolio.columns:
            if self.dfPortfolio[ticker]['Buy'] > 0:
                buyPrice = '%.4f' % self.dfPortfolio[ticker]['Price']
                print("%s %s 매수($ %s)" % (ticker, str(int(self.dfPortfolio[ticker]['Buy'])), buyPrice))
                # [실전투자] JTTT1002U: 미국 매수 주문, [모의투자] VTTT1002U : 미국 매수 주문
                deal = self.kis.buyStock(ticker, str(int(self.dfPortfolio[ticker]['Buy'])), buyPrice, "VTTT1002U")
                print(deal)
                time.sleep(0.2)

        print("리벨런싱 완료")

if __name__ == "__main__":
    holidays = holidays.US()
    date = datetime.date.today().strftime('%Y-%m-%d')

    investPy()

    if date not in holidays:
        investPy()
    else:
        print('오늘은 휴일입니다.')
        exit()
