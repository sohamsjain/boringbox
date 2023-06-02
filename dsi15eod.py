from datetime import time
from time import sleep

import backtrader as bt

from dsicache.allobjects import *
from mytelegram.skyler import Skyler
from tradingschedule import *

fromdate = lastclosingtime.date()
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)
nexttradingsession = nextclosingtime.date()

tickers = ['NIFTY50_IND_NSE', 'BANKNIFTY_IND_NSE', 'RELIANCE_STK_NSE', 'SBIN_STK_NSE', 'TATASTEEL_STK_NSE',
          'HCLTECH_STK_NSE', 'TATAMOTOR_STK_NSE', 'ADANIPORT_STK_NSE', 'AXISBANK_STK_NSE', 'BAJFINANC_STK_NSE',
           'BAJAJFINS_STK_NSE', 'BHARTIART_STK_NSE', 'INDUSINDB_STK_NSE', 'MARUTI_STK_NSE', 'ONGC_STK_NSE',
           'TECHM_STK_NSE', 'TITAN_STK_NSE', 'WIPRO_STK_NSE', 'HDFC_STK_NSE', 'TCS_STK_NSE', 'HINDALCO_STK_NSE',
           'JSWSTEEL_STK_NSE']

tickers.sort()


lastclose_dict = dict()


def round05(n):
    return round(n * 20) / 20


def format2d(n):
    return format(n, '.2f')


def get_arrow(n):
    if n == 1:
        return '⬆️'
    elif n == 0:
        return '➡️'
    elif n == -1:
        return '⬇️'
    else:
        return 'Invalid input'


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

store = bt.stores.IBStore(port=7496, _debug=True)
cerebro.addcalendar("BSE")

for index in tickers:
    getdata(index)

cerebro.run()

skyler = Skyler()

sleep(10)

for ticker, close in lastclose_dict.items():
    dsi15 = dsi15x[ticker]
    dsi75 = dsi75x[ticker]
    dsiD = dsiDx[ticker]
    atr = round05(dsi15["curvestate"]["atrvalue"])
    upperband = round05(close + atr * 3)
    lowerband = round05(close - atr * 3)
    print(atr, close, upperband, lowerband)

    message = ""
    message += "*Plan for: " + str(nexttradingsession.strftime("%d-%m-%Y")) + "*\n\n"

    trendDay = get_arrow(dsiD["trendstate"]["trend"])
    trend75 = get_arrow(dsi75["trendstate"]["trend"])
    trend15 = get_arrow(dsi15["trendstate"]["trend"])

    message += f"Daily Trend: {trendDay}\n"
    message += f"75 Min Trend: {trend75}\n"
    message += f"15 Min Trend: {trend15}\n\n"

    szs = dsiD["supplyzones"][:2]
    while len(szs):
        sz = szs.pop(-1)
        entry = format2d(round05(sz.entry))
        sl = format2d(round05(sz.sl))
        score = str(sz.score)
        message += f"-{entry} SL {sl}  Ω {score}\n"

    message += f"- " * 25 + "\n"
    message += f"- " * 25 + "\n"

    #####################################
    # ____________________________________

    szs = []
    for sz in dsi75["supplyzones"]:
        if sz.entry <= upperband:
            szs.append(sz)
        else:
            break

    while len(szs):
        sz = szs.pop(-1)
        entry = format2d(round05(sz.entry))
        sl = format2d(round05(sz.sl))
        score = str(sz.score)
        message += f"-{entry} SL {sl}  Ω {score}\n"

    message += f"- " * 25 + "\n"
    message += f"- " * 25 + "\n"

    #####################################
    # ____________________________________

    szs = []
    for sz in dsi15["supplyzones"]:
        if sz.entry <= upperband:
            szs.append(sz)
        else:
            break

    while len(szs):
        sz = szs.pop(-1)
        entry = format2d(round05(sz.entry))
        sl = format2d(round05(sz.sl))
        score = str(sz.score)
        message += f"-{entry} SL {sl}  Ω {score}\n"

    #####################################
    # ____________________________________

    message += f"\n*{ticker.split('_')[0]}: {round05(close)} @ {round05(atr)}*\n\n"

    # ____________________________________
    #####################################

    dzs = []
    for dz in dsi15["demandzones"]:
        if dz.entry >= lowerband:
            dzs.append(dz)
        else:
            break

    while len(dzs):
        dz = dzs.pop(0)
        entry = format2d(round05(dz.entry))
        sl = format2d(round05(dz.sl))
        score = str(dz.score)
        message += f"+{entry} SL {sl}  Ω {score}\n"

    # ____________________________________
    #####################################

    message += f"- " * 25 + "\n"
    message += f"- " * 25 + "\n"

    dzs = []
    for dz in dsi75["demandzones"]:
        if dz.entry >= lowerband:
            dzs.append(dz)
        else:
            break

    while len(dzs):
        dz = dzs.pop(0)
        entry = format2d(round05(dz.entry))
        sl = format2d(round05(dz.sl))
        score = str(dz.score)
        message += f"+{entry} SL {sl}  Ω {score}\n"

    # ____________________________________
    #####################################

    message += f"- " * 25 + "\n"
    message += f"- " * 25 + "\n"

    dzs = dsiD["demandzones"][:2]
    while len(dzs):
        dz = dzs.pop(0)
        entry = format2d(round05(dz.entry))
        sl = format2d(round05(dz.sl))
        score = str(dz.score)
        message += f"+{entry} SL {sl}  Ω {score}\n"

    skyler.send_all_clients(message, parse_mode="Markdown")
    # skyler.send_message(975198388, message, parse_mode="Markdown")

skyler.send_all_clients(f"*Disclaimer:*\nTrade Levels for educational purpose only. Consult your advisor before trading.", parse_mode="Markdown")
skyler.stop()
