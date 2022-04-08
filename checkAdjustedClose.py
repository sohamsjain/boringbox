from datetime import datetime, timedelta
from typing import List

import backtrader as bt
import pandas_market_calendars
from pynse import Nse, IndexSymbol

pmcnse = pandas_market_calendars.get_calendar("NSE")

schedule = \
    pmcnse.schedule(datetime.now().date() - timedelta(days=10), datetime.now().date() + timedelta(days=10),
                    tz="Asia/Kolkata")[
        "market_close"].to_list()

schedule = [t.to_pydatetime().replace(tzinfo=None) for t in schedule]

now = datetime.now()

closes = [dt for dt in schedule if dt < now]
pastcloses: List[datetime] = [dt for dt in schedule if dt < now]
futurecloses = [dt for dt in schedule if dt > now]

nse = Nse()

pynseclose = nse.get_indices(IndexSymbol.Nifty50)["last"][0]
adjustedClose = False
fromdate = pastcloses[-1].date()
todate = pastcloses[-1].date()
sessionstart = datetime.now().time().replace(hour=9, minute=15, second=0, microsecond=0)
sessionend = datetime.now().time().replace(hour=15, minute=30, second=0, microsecond=0)


class TestSt2(bt.Strategy):

    def next(self):
        self.cerebro.runstop()

    def stop(self):
        global adjustedClose, pynseclose
        if self.datas[0].close[0] == pynseclose:
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
