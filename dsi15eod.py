from datetime import time

import backtrader as bt

from dsicache.allobjects import *
from mytelegram.skyler import Skyler
from tradingschedule import lastclosingtime

fromdate = lastclosingtime.date()
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)

indexes = [
    "NIFTY50_IND_NSE",
    "BANKNIFTY_IND_NSE",
]
lastclose_dict = dict()


class Strategy(bt.Strategy):
    def next(self):
        if self.datas[0].datetime.datetime(0) == lastclosingtime:
            self.cerebro.runstop()

    def stop(self):
        global lastclose_dict
        for data in self.datas:
            ticker = data._dataname
            close = data.close[0]
            lastclose_dict.update({ticker: close})


def getdata(t):
    global cerebro
    data0 = store.getdata(dataname=t, fromdate=fromdate,
                          sessionstart=sessionstart,
                          sessionend=sessionend,
                          historical=True, timeframe=bt.TimeFrame.Days)

    cerebro.adddata(data0)


cerebro = bt.Cerebro(runonce=False)
cerebro.addstrategy(Strategy)

store = bt.stores.IBStore(port=7497, _debug=True)
cerebro.addcalendar("BSE")

for index in indexes:
    getdata(index)

cerebro.run()

skyler = Skyler()

for ticker, close in lastclose_dict.items():
    s15 = dsi15x[ticker]
    atr = round(s15["curvestate"]["atrvalue"], 2)
    upperband = round(close + atr * 3, 2)
    lowerband = round(close - atr * 3, 2)
    print(atr, close, upperband, lowerband)

    message = ""
    message += "Date: " + str(dsi15_lts.date()) + "\n\n"

    szs = []
    for sz in s15["supplyzones"]:
        if sz.entry <= upperband:
            szs.append(sz)
        else:
            break

    while len(szs):
        sz = szs.pop(-1)
        entry = str(round(sz.entry))
        sl = str(round(sz.sl))
        ratio = str(round(sz.ratio)) if sz.ratio != float("inf") else "inf"
        score = str(sz.score)
        message += f"-{entry} SL {sl} ® {ratio} Ω {score}\n"

    #####################################
    # ____________________________________

    message += f"\n{ticker.split('_')[0]}: {round(close)} @ {round(atr)}\n\n"

    # ____________________________________
    #####################################

    dzs = []
    for dz in s15["demandzones"]:
        if dz.entry >= lowerband:
            dzs.append(dz)
        else:
            break

    while len(dzs):
        dz = dzs.pop(0)
        entry = str(round(dz.entry))
        sl = str(round(dz.sl))
        ratio = str(round(dz.ratio)) if dz.ratio != float("inf") else "inf"
        score = str(dz.score)
        message += f"+{entry} SL {sl} ® {ratio} Ω {score}\n"

    skyler.send_all_clients(message)

skyler.stop()
