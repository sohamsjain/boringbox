import pickle
import sys
import traceback
from datetime import timedelta, time

import backtrader as bt
import pandas as pd
from backtrader.utils import AutoOrderedDict

from dsicache.allobjects import loadobjects, dsiD_lts
from indicators.oddenhancers import DSIndicator
from mygoogle.sprint import GoogleSprint
from mytelegram.raven import Raven
from tradingschedule import lastclosingtime

raven = Raven()

if lastclosingtime == dsiD_lts:
    msg = f"Cache Update\n Timeframe: Days\n Compression: 1\n Updated Till: {dsiD_lts}"
    print(msg)
    raven.send_all_clients(msg)
    raven.stop()
    sys.exit()

daysago500 = lastclosingtime.date() - timedelta(days=440)
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)

with open("dsicache/day/tickers.obj", "rb") as file:
    tickers: pd.DataFrame = pickle.load(file)

filename = "dsicache/day/dsi.obj"
g = GoogleSprint()
wb = g.gs.open("Demand Supply Daily")
ws = wb.worksheet("Zones1")
finaldf = None


class DailyUpdate(bt.Strategy):

    def __init__(self):
        self.resource = AutoOrderedDict()
        self.states = self.deserialize()
        nonce = 0
        while nonce < len(self.datas):
            dsidata = self.datas[nonce]  # Day 1
            trenddata = self.datas[nonce + 1]  # Week 1
            curvedata = self.datas[nonce + 2]  # Month 1

            dname = dsidata._dataname
            try:
                savedstate = self.states[dname] if self.states else None
            except KeyError:
                savedstate = None
            self.resource[dname].datas.daily = dsidata
            self.resource[dname].datas.weekly = trenddata
            self.resource[dname].datas.monthly = curvedata
            self.resource[dname].ds = DSIndicator(dsidata, trenddata, curvedata, savedstate=savedstate)
            nonce += 3

    def next(self):
        global finaldf
        if self.data0.datetime.datetime(0) == lastclosingtime:
            allrows = list()
            for dname, val in self.resource.items():
                ds: DSIndicator = val.ds
                close = val.datas.daily.close[0]
                atr = ds.atr[0]

                try:
                    d1 = ds.demandzones[0]
                    d1_entry = d1.entry
                    d1_stoploss = d1.sl
                    d1_score = d1.score
                    ptd = close - d1_entry
                    atd = round(ptd / atr, 2)
                    dstrength = d1.strength
                    dtbase = d1.timeatbase
                    dratio = d1.ratio if d1.ratio != float("inf") else "Anant"
                    dcurve = d1.location
                    dtrend = d1.trend
                    dtest = d1.testcount
                except IndexError:
                    d1_entry = None
                    d1_stoploss = None
                    d1_score = None
                    ptd = None
                    atd = None
                    dstrength = None
                    dtbase = None
                    dratio = None
                    dcurve = None
                    dtrend = None
                    dtest = None

                try:
                    s1 = ds.supplyzones[0]
                    s1_entry = s1.entry
                    s1_stoploss = s1.sl
                    s1_score = s1.score
                    pts = s1_entry - close
                    ats = round(pts / atr, 2)
                    sstrength = s1.strength
                    stbase = s1.timeatbase
                    sratio = s1.ratio if s1.ratio != float("inf") else "Anant"
                    scurve = s1.location
                    strend = s1.trend
                    stest = s1.testcount
                except IndexError:
                    s1_entry = None
                    s1_stoploss = None
                    s1_score = None
                    pts = None
                    ats = None
                    sstrength = None
                    stbase = None
                    sratio = None
                    scurve = None
                    strend = None
                    stest = None

                row = {
                    "Ticker": dname.split("_")[0],
                    "D Trend": dtrend,
                    "D Curve": dcurve,
                    "D Time": dtbase,
                    "D Strength": dstrength,
                    "D Test": dtest,
                    "D Ratio": dratio,
                    "D ATR": atd,
                    "D Points": ptd,
                    "D Stoploss": d1_stoploss,
                    "D Entry": d1_entry,
                    "D Score": d1_score,
                    "Last Close": close,
                    "S Score": s1_score,
                    "S Entry": s1_entry,
                    "S Stoploss": s1_stoploss,
                    "S Points": pts,
                    "S ATR": ats,
                    "S Ratio": sratio,
                    "S Test": stest,
                    "S Strength": sstrength,
                    "S Time": stbase,
                    "S Curve": scurve,
                    "S Trend": strend,
                }

                allrows.append(row)

            df = pd.DataFrame(allrows)
            df.fillna("", inplace=True)
            if finaldf is None:
                finaldf = df
            else:
                finaldf = finaldf.append(df, ignore_index=True)
            finaldf.drop_duplicates(inplace=True)
            g.update_sheet(ws, finaldf)
            self.cerebro.runstop()

    def serialize(self):
        blocks = {dname: val.ds.getstate() for dname, val in self.resource.items()}
        if self.states:
            self.states.update(blocks)
        else:
            self.states = blocks
        with open(filename, "wb") as file:
            pickle.dump(self.states, file)

    def deserialize(self):
        try:
            with open(filename, "rb") as file:
                dsidict = pickle.load(file)
            return dsidict
        except (EOFError, FileNotFoundError):
            return None

    def stop(self):
        self.serialize()


olderthan500days = tickers[tickers.date < daysago500]
newerthan500days = tickers[tickers.date >= daysago500]

while len(olderthan500days):

    subset50 = olderthan500days[:50]
    olderthan500days = olderthan500days[50:]
    print(subset50)

    cerebro = bt.Cerebro(runonce=False)
    cerebro.addstrategy(DailyUpdate)
    cerebro.addcalendar("NSE")
    store = bt.stores.IBStore(port=7497, _debug=False)

    for element in subset50.iterrows():
        ticker = element[1]
        data0 = store.getdata(dataname=ticker.btsymbol, fromdate=daysago500,
                              sessionstart=sessionstart,
                              sessionend=sessionend,
                              historical=True, timeframe=bt.TimeFrame.Days)

        cerebro.adddata(data0)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Weeks)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Months)
    try:
        thestrats = cerebro.run(stdstats=False)
    except Exception as e:
        print(subset50)
        print(traceback.format_exc())
        raven.send_all_clients(list(subset50.symbol))
        raven.send_all_clients(traceback.format_exc())

while len(newerthan500days):

    subset1 = newerthan500days[:1]
    newerthan500days = newerthan500days[1:]
    print(subset1)

    cerebro = bt.Cerebro(runonce=False)
    cerebro.addstrategy(DailyUpdate)
    cerebro.addcalendar("NSE")
    store = bt.stores.IBStore(port=7497, _debug=False)

    for element in subset1.iterrows():
        ticker = element[1]
        data0 = store.getdata(dataname=ticker.btsymbol, fromdate=ticker.date,
                              sessionstart=sessionstart,
                              sessionend=sessionend,
                              historical=True, timeframe=bt.TimeFrame.Days)

        cerebro.adddata(data0)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Weeks)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Months)
    try:
        thestrats = cerebro.run(stdstats=False)
    except Exception as e:
        print(subset1)
        print(traceback.format_exc())
        raven.send_all_clients(list(subset1.symbol))
        raven.send_all_clients(traceback.format_exc())

loadobjects()
msg = f"Cache Update\n Timeframe: Days\n Compression: 1\n Updated Till: {dsiD_lts}"
raven.send_all_clients(msg)
raven.stop()
