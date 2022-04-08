from datetime import date, time

import backtrader as bt
from nsepy import get_history

from tradingschedule import lastclosingtime

nsepyclose = get_history(symbol="NIFTY 50",
                         start=date(2022, 4, 6),
                         end=date(2022, 4, 8),
                         index=True)["Close"].values[-1]

adjustedClose = False
fromdate = todate = lastclosingtime.date()
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)


class TestSt2(bt.Strategy):

    def next(self):
        self.cerebro.runstop()

    def stop(self):
        global adjustedClose, nsepyclose
        if self.datas[0].close[0] == nsepyclose:
            adjustedClose = True


tickers = ["NIFTY50_IND_NSE"]

while tickers:
    t = tickers[:50]
    tickers = tickers[50:]
    cerebro = bt.Cerebro(runonce=False)
    cerebro.addstrategy(TestSt2)
    cerebro.addcalendar("NSE")
    store = bt.stores.IBStore(port=7497, _debug=False)
    cerebro.setbroker(store.getbroker())

    for ticker in t:
        data0 = store.getdata(dataname=ticker, fromdate=fromdate, todate=todate,
                              sessionstart=sessionstart,
                              sessionend=sessionend,
                              historical=True, timeframe=bt.TimeFrame.Days)

        cerebro.adddata(data0)

    try:
        thestrats = cerebro.run(stdstats=False)
    except Exception as e:
        print(t[0])

if __name__ == '__main__':
    print("Close Adjusted", adjustedClose)
