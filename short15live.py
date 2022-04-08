import pickle
from datetime import timedelta, time
from threading import Thread
from typing import Dict, Any

import backtrader as bt
import pandas as pd
from backtrader.utils import AutoOrderedDict
from pynse import Nse, IndexSymbol
from sqlalchemy import create_engine, inspect

from indicators.supertrend import SuperTrend
from models import *
from mytelegram.raven import Raven
from tradingschedule import nextclosingtime


class Db:

    def __init__(self):
        self.dialect = "mysql"
        self.driver = "pymysql"
        self.username = "soham"
        self.password = "Soham19jain98"
        self.host = "52.70.61.124"
        self.port = "3306"
        self.database = "cerebelle2"
        self.engine = create_engine(
            f"{self.dialect}+{self.driver}://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}",
            echo=False, pool_size=10, max_overflow=20)
        self.insp = inspect(self.engine)
        self.tables = self.insp.get_table_names()


db = Db()

cds = pd.read_sql('select * from contracts where underlying in ("NIFTY50", "BANKNIFTY")', con=db.engine)

expirydates = cds[cds["expiry"].notnull()]["expiry"].drop_duplicates().sort_values().values
nearest_expiry = expirydates[0]

contractsbybtsymbol = dict()
contractsbysymbol = dict()


def get_contract(underlying=None, sectype=None, symbol=None, btsymbol=None, expiry=None, strike=None, right=None):
    u = True
    st = True
    s = True
    bts = True
    e = True
    sk = True
    r = True

    if not symbol and not btsymbol:
        if not underlying:
            return "Missing: underlying contractname"

        u = cds.underlying == underlying

        if not sectype:
            return "Missing: Contract sectype"

        st = cds.sectype == sectype

        if sectype in [SecType.FUT, SecType.OPT]:
            if not expiry:
                return "Missing: contract expiry date"
            e = cds.expiry == expiry

        elif sectype == SecType.OPT:
            if not strike:
                return "Missing: strike price"
            sk = cds.strike == strike

            if not right:
                return "Missing: contract right"
            r = cds.right == right

    else:

        if symbol:

            if symbol in contractsbysymbol:
                return contractsbysymbol[symbol]

            s = cds.symbol == symbol

        if btsymbol:

            if btsymbol in contractsbybtsymbol:
                return contractsbybtsymbol[btsymbol]

            bts = cds.btsymbol == btsymbol

    fdf: pd.DataFrame = cds[u & st & s & bts & e & sk & r]
    count = fdf.shape[0]
    if count < 1:
        return "No contract found"
    elif count > 1:
        return "Multiple contracts found"

    if fdf.btsymbol.values[0] in contractsbybtsymbol:
        return contractsbybtsymbol[fdf.btsymbol.values[0]]

    contract = Contract(
        underlying=fdf.underlying.values[0],
        sectype=fdf.sectype.values[0],
        exchange=fdf.exchange.values[0],
        currency=fdf.currency.values[0],
        symbol=fdf.symbol.values[0],
        strike=fdf.strike.values[0],
        right=fdf.right.values[0],
        expiry=fdf.expiry.values[0],
        multiplier=fdf.multiplier.values[0],
        btsymbol=fdf.btsymbol.values[0],
        lotsize=int(fdf.lotsize.values[0])
    )

    contractsbybtsymbol[fdf.btsymbol.values[0]] = contract
    contractsbysymbol[fdf.symbol.values[0]] = contract

    return contract


def nearest_call(underlying, expiry, price):
    u = cds.underlying == underlying
    e = cds.expiry == expiry
    r = cds.right == "C"
    fdf = cds[u & e & r]
    strikes: list = fdf.strike.to_list()
    strikes.sort()
    strike = None

    if price in strikes:
        strike = price

    else:
        strikes.append(price)
        strikes.sort()
        priceindex = strikes.index(price)
        strikeabove = strikes[priceindex + 1]
        strikebelow = strikes[priceindex - 1]
        differenceabove = strikeabove - price
        differencebelow = price - strikebelow
        if differenceabove > differencebelow:
            strike = strikebelow
        elif differencebelow >= differenceabove:
            strike = strikeabove

    if not strike:
        return "Strike price not resolved"

    fdf = fdf[fdf.strike == strike]
    return get_contract(symbol=fdf.symbol.values[0])


def nearest_put(underlying, expiry, price):
    u = cds.underlying == underlying
    e = cds.expiry == expiry
    r = cds.right == "P"
    fdf = cds[u & e & r]
    strikes: list = fdf.strike.to_list()
    strikes.sort()
    strike = None

    if price in strikes:
        strike = price

    else:
        strikes.append(price)
        strikes.sort()
        priceindex = strikes.index(price)
        strikeabove = strikes[priceindex + 1]
        strikebelow = strikes[priceindex - 1]
        differenceabove = strikeabove - price
        differencebelow = price - strikebelow
        if differenceabove >= differencebelow:
            strike = strikebelow
        elif differencebelow > differenceabove:
            strike = strikeabove

    if not strike:
        return "Strike price not resolved"

    fdf = fdf[fdf.strike == strike]
    return get_contract(symbol=fdf.symbol.values[0])


nse = Nse()
btsymboltopynse = {"NIFTY50_IND_NSE": IndexSymbol.Nifty50,
                   "BANKNIFTY_IND_NSE": IndexSymbol.NiftyBank}

# datafeed params
fromdate = datetime.now().date() - timedelta(days=3)
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)

tradingday = AutoOrderedDict()
tradingday.date = nextclosingtime.date()
tradingday.sessionstart = nextclosingtime.replace(hour=9, minute=15)
tradingday.lastorder = nextclosingtime.replace(hour=15, minute=0)
tradingday.intraday_squareoff = nextclosingtime.replace(hour=15, minute=25)
tradingday.sessionend = nextclosingtime


class MyStrategy(bt.Strategy):
    params = (("maxpos", 5),)

    def __init__(self):

        self.xone_resource = AutoOrderedDict()
        self.child_resource = AutoOrderedDict()

        self.allxones: List[Xone] = list()
        self.openxones: List[Xone] = list()
        self.xonesbyorigin: Dict[Optional[DZone, SZone], Xone] = dict()
        self.childrenbyorder: Dict[Any, Child] = dict()

        self.openordercount = 0

        self.raven: Raven = Raven()
        self.ravenq: Optional[Queue] = None
        self.raventhread = Thread(target=self.sendaraven, name="raven", daemon=True)
        self.raventhread.start()

        self.states = self.deserialize()
        self.states_restored = datetime.max

        self.intraday_squareoff_triggered = False

        nonce = 0
        while nonce < len(self.datas):
            tickdata = self.datas[nonce]  # RTbar
            dsidata = self.datas[nonce + 1]  # Minute 15
            trenddata = self.datas[nonce + 2]  # Minute 75
            curvedata = self.datas[nonce]  # RTbar proxying Day 1

            btsymbol = dsidata._dataname
            try:
                savedstate = self.states[btsymbol] if self.states else None
            except KeyError:
                savedstate = None

            self.xone_resource[btsymbol].tickdata = tickdata
            self.xone_resource[btsymbol].dsidata = dsidata
            self.xone_resource[btsymbol].trenddata = trenddata
            self.xone_resource[btsymbol].curvedata = curvedata
            self.xone_resource[btsymbol].dsi = DSIndicator(tickdata, dsidata, trenddata, curvedata,
                                                           savedstate=savedstate,
                                                           curvebreakoutsonly=True)
            nonce += 3

    def notify_order(self, order):

        if order.status in [order.Submitted, order.Accepted, order.Partial]:
            return

        try:
            child = self.childrenbyorder[order.ref]
            self.childrenbyorder.pop(order.ref)
        except KeyError as k:
            print(k)
            return

        if order.status == order.Completed:

            if order.isbuy():

                child.buying_price = order.executed.price
                child.buying_cost = order.executed.value
                child.buying_commission = order.executed.comm

                if child.isbuy:
                    child.filled += order.executed.size
                    child.status = ChildStatus.BOUGHT
                    child.opened_at = datetime.now()
                else:
                    child.filled += order.executed.size
                    child.status = ChildStatus.SQUAREDOFF
                    child.closed_at = datetime.now()
                    child.pnl = child.selling_cost - child.buying_cost

            else:

                child.selling_price = order.executed.price
                child.selling_cost = order.executed.value
                child.selling_commission = order.executed.comm

                if child.isbuy:
                    child.filled += order.executed.size
                    child.status = ChildStatus.SQUAREDOFF
                    child.closed_at = datetime.now()
                    child.pnl = child.selling_cost - child.buying_cost
                else:
                    child.filled += order.executed.size
                    child.status = ChildStatus.SOLD
                    child.opened_at = datetime.now()

        elif order.status == order.Canceled:
            child.status = ChildStatus.CANCELLED
        elif order.status == order.Margin:
            child.status = ChildStatus.MARGIN
        elif order.status == order.Rejected:
            child.status = ChildStatus.REJECTED

        xone = child.xone
        xone.orders.remove(order)

        if xone.orders == [None]:  # All child orders processed
            xone.orders.clear()

            if xone.status in XoneStatus.PENDING:

                self.openordercount -= 1

                xone.status = XoneStatus.ENTRY
                xone.opened_at = datetime.now()
                self.openxones.append(xone)

                for child in xone.children:
                    if child.status in [ChildStatus.MARGIN, ChildStatus.REJECTED]:
                        xone.status = XoneStatus.ABORT
                        break

            elif xone.status in XoneStatus.OPEN:
                xone.closed_at = datetime.now()
                xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                xone.status = xone.nextstatus
                self.removexone(xone)
                self.openxones.remove(xone)

            elif xone.status == XoneStatus.ABORT:
                xone.closed_at = datetime.now()
                xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                xone.status = XoneStatus.FORCECLOSED
                self.removexone(xone)
                self.openxones.remove(xone)

            self.ravenq.put(xone.notification())

    def notify_trade(self, trade):
        pass

    def notify_data(self, data, status, *args, **kwargs):
        print(datetime.now(), data._dataname, data._getstatusname(status))

    def notify_store(self, msg, *args, **kwargs):
        print(datetime.now(), msg)

    def notify_cashvalue(self, cash, value):
        # print("Cash: ", cash, "Value", value)
        pass

    def notify_fund(self, cash, value, fundvalue, shares):
        # print(cash, value, fundvalue, shares)
        pass

    def prenext(self):
        self.next()

    def next(self):

        for btsymbol, resource in self.xone_resource.items():
            dsi: DSIndicator = resource.dsi
            if not dsi.notifications.empty():
                self.readnotifications(dsi)

        if self.datetime.datetime(0) <= self.states_restored:
            return

        for xone in self.allxones:
            data = xone.datagroup.tickdata

            try:  # In case a new datafeed is added, it may not produce a bar
                xone.lastprice = xone.datagroup.tickdata.close[
                    0]  # This is a work around to eliminate unnecessary IndexError
                for child in xone.children:  # Must apply for xone.data as well as child.data
                    child.lastprice = child.datagroup.tickdata.close[0]
            except IndexError:
                continue

            if xone.orders:  # Skip an iteration until pending orders are completed
                continue

            if xone.status in XoneStatus.PENDING:

                if xone.isbullish:
                    if (xone.status == XoneStatus.ENTRYHIT) and (data.high[0] >= xone.target):
                        xone.status = XoneStatus.MISSED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.low[0] < xone.stoploss:
                        xone.status = XoneStatus.FAILED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.low[0] <= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = datetime.now()
                            self.ravenq.put(xone.notification())
                        if xone.tradable():
                            xone.open_children = True

                else:
                    if (xone.status == XoneStatus.ENTRYHIT) and (data.low[0] <= xone.target):
                        xone.status = XoneStatus.MISSED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.high[0] > xone.stoploss:
                        xone.status = XoneStatus.FAILED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.high[0] >= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = datetime.now()
                            self.ravenq.put(xone.notification())
                        if xone.tradable():
                            xone.open_children = True

                if xone.open_children and data.datetime.datetime(0) <= tradingday.lastorder:
                    xone.open_children = False
                    if not xone.children:
                        continue
                    if (len(self.openxones) + self.openordercount) < self.p.maxpos:
                        self.openordercount += 1
                        for child in xone.children:
                            size = child.contract.lotsize
                            if child.isbuy:
                                order = self.buy(data=child.datagroup.tickdata, size=size)
                            else:
                                order = self.sell(data=child.datagroup.tickdata, size=size)
                            xone.orders.append(order)
                            self.childrenbyorder[order.ref] = child
                        xone.orders.append(None)

            elif xone.status in XoneStatus.OPEN:

                if xone.isbullish:
                    if data.low[0] < xone.stoploss:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.STOPLOSSHIT
                            xone.nextstatus = XoneStatus.STOPLOSS
                            xone.exit_at = datetime.now()
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                    elif data.high[0] >= xone.target:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.TARGETHIT
                            xone.nextstatus = XoneStatus.TARGET
                            xone.exit_at = datetime.now()
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                else:
                    if data.high[0] > xone.stoploss:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.STOPLOSSHIT
                            xone.nextstatus = XoneStatus.STOPLOSS
                            xone.exit_at = datetime.now()
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                    elif data.low[0] <= xone.target:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.TARGETHIT
                            xone.nextstatus = XoneStatus.TARGET
                            xone.exit_at = datetime.now()
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                if xone.close_children:
                    xone.close_children = False
                    if xone.nextstatus is None:
                        if xone.isbullish:
                            xone.nextstatus = XoneStatus.PROFIT if xone.lastprice > xone.entry else XoneStatus.LOSS
                        else:
                            xone.nextstatus = XoneStatus.PROFIT if xone.lastprice < xone.entry else XoneStatus.LOSS

                    for child in xone.children:
                        if child.isbuy:
                            order = self.sell(data=child.datagroup.tickdata, size=child.filled)
                        else:
                            order = self.buy(data=child.datagroup.tickdata, size=child.filled)
                        xone.orders.append(order)
                        self.childrenbyorder[order.ref] = child
                    xone.orders.append(None)

            elif xone.status == XoneStatus.ABORT:
                for child in xone.children:
                    if child.filled > 0:
                        if child.status == ChildStatus.BOUGHT:
                            order = self.sell(data=child.datagroup.tickdata, size=child.filled)
                        else:
                            order = self.buy(data=child.datagroup.tickdata, size=child.filled)
                        xone.orders.append(order)
                        self.childrenbyorder[order.ref] = child
                if len(xone.orders):
                    xone.orders.append(None)
                else:
                    xone.closed_at = datetime.now()
                    xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                    xone.status = XoneStatus.FORCECLOSED
                    self.removexone(xone)
                    self.openxones.remove(xone)

            else:
                self.removexone(xone)

        if self.data0.datetime.datetime(0) >= tradingday.intraday_squareoff:
            if not self.intraday_squareoff_triggered:
                self.intraday_squareoff_triggered = True
                if self.openxones:
                    for xone in self.openxones:
                        if xone.isbullish:
                            xone.nextstatus = XoneStatus.PROFIT if xone.lastprice > xone.entry else XoneStatus.LOSS
                        else:
                            xone.nextstatus = XoneStatus.PROFIT if xone.lastprice < xone.entry else XoneStatus.LOSS

                        for child in xone.children:
                            if child.isbuy:
                                order = self.sell(data=child.datagroup.tickdata, size=child.filled)
                            else:
                                order = self.buy(data=child.datagroup.tickdata, size=child.filled)
                            xone.orders.append(order)
                            self.childrenbyorder[order.ref] = child
                        xone.orders.append(None)

        if self.data0.datetime.datetime(0) >= tradingday.sessionend:
            self.cerebro.runstop()

    def readnotifications(self, dsi: DSIndicator):
        allnotifs = list()

        notif = list()
        while not dsi.notifications.empty():
            notifchunk = dsi.notifications.get()
            if notifchunk == Notifications.NotificationEnds:
                allnotifs.append(notif.copy())
                notif.clear()
                continue
            notif.append(notifchunk)

        for notif in allnotifs:

            subject = notif.pop(0)

            if subject == Notifications.SavedStateRestored:
                self.initialize_xones(dsi)
                self.states_restored = self.datetime.datetime(0)
                return

            if self.datetime.datetime(0) <= self.states_restored:
                continue

            if subject == Notifications.DemandZoneCreated:

                dz = notif.pop(0)
                self.create_xone(dz)

            elif subject == Notifications.DemandZoneModified:

                dz = notif.pop(0)
                if dz in self.xonesbyorigin:
                    xone = self.xonesbyorigin[dz]
                    if xone.entry != dz.entry:
                        xone.entry = dz.entry
                    if xone.originalstoploss != dz.sl:
                        xone.extend_stoploss(dz.sl)

            elif subject == Notifications.DemandZoneBroken:
                pass

            elif subject == Notifications.SupplyZoneCreated:

                sz = notif.pop(0)
                self.create_xone(sz)

            elif subject == Notifications.SupplyZoneModified:

                sz = notif.pop(0)
                if sz in self.xonesbyorigin:
                    xone = self.xonesbyorigin[sz]
                    if xone.entry != sz.entry:
                        xone.entry = sz.entry
                    if xone.originalstoploss != sz.sl:
                        xone.extend_stoploss(sz.sl)

            elif subject == Notifications.SupplyZoneBroken:
                pass

    def initialize_xones(self, dsi: DSIndicator):

        btsymbol = dsi.data0._dataname
        symbol = btsymboltopynse[btsymbol]
        atr = dsi.curve.atrvalue
        close = float(nse.get_indices(symbol)["last"][0])
        upperband = round(close + atr, 2)
        lowerband = round(close - atr, 2)

        zones = []

        for sz in dsi.supplyzones:
            if sz.entry <= upperband:
                zones.append(sz)
            else:
                break

        for dz in dsi.demandzones:
            if dz.entry >= lowerband:
                zones.append(dz)
            else:
                break

        for z in zones:
            self.create_xone(z)

    def create_xone(self, origin: Optional[DZone]):

        if origin in self.xonesbyorigin:
            return self.xonesbyorigin[origin]

        btsymbol = origin.dsi.data0._dataname
        xonecontract = get_contract(btsymbol=btsymbol)

        xone = Xone(origin=origin,
                    contract=xonecontract,
                    status=XoneStatus.CREATED)

        xone.datagroup = self.xone_resource[xone.btsymbol]

        if xone.isbullish:
            childcontract = nearest_put(xonecontract.underlying, nearest_expiry, xone.stoploss)
        else:
            childcontract = nearest_call(xonecontract.underlying, nearest_expiry, xone.stoploss)

        child = Child(contract=childcontract,
                      xone=xone,
                      status=ChildStatus.CREATED)

        child.datagroup = self.get_child_datagroup(child.btsymbol)

        xone.children.append(child)

        self.allxones.append(xone)
        self.xonesbyorigin.update({xone.origin: xone})

        self.ravenq.put(xone.notification())

    def removexone(self, xone):
        if xone.origin in self.xonesbyorigin:
            self.xonesbyorigin.pop(xone.origin)
            self.allxones.remove(xone)

    def get_child_datagroup(self, btsymbol):

        if btsymbol in self.child_resource:
            return self.child_resource[btsymbol]
        print(btsymbol)
        backfill = store.getdata(dataname=btsymbol, fromdate=fromdate, historical=True, timeframe=bt.TimeFrame.Minutes,
                                 compression=1, sessionstart=sessionstart,
                                 sessionend=sessionend)
        backfill.addfilter(MinutesBackwardLookingFilter)

        data = store.getdata(dataname=btsymbol, rtbar=True, timeframe=bt.TimeFrame.Minutes,
                             compression=1, sessionstart=sessionstart,
                             sessionend=sessionend, backfill_from=backfill)

        data.addfilter(SecondsBackwardLookingFilter)

        tickdata = self.cerebro.adddata(data)
        resampleddata = self.cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=15)

        tickdata.reset()
        resampleddata.reset()
        supertrend = SuperTrend(resampleddata)

        if self.cerebro._exactbars < 1:
            tickdata.extend(size=self.cerebro.params.lookahead)
            resampleddata.extend(size=self.cerebro.params.lookahead)

        tickdata._start()
        resampleddata._start()

        if self.cerebro._dopreload:
            tickdata.preload()
            resampleddata.preload()

        self.cerebro.runrestart()

        self.child_resource[btsymbol].tickdata = tickdata
        self.child_resource[btsymbol].supertrenddata = resampleddata
        self.child_resource[btsymbol].supertrend = supertrend

        return self.child_resource[btsymbol]

    def sendaraven(self):
        self.ravenq = Queue()
        while True:
            self.raven.send_all_clients(self.ravenq.get())

    def deserialize(self):
        try:
            with open("dsicache/min15/15dsi.obj", "rb") as file:
                dsidict = pickle.load(file)
            return dsidict
        except (EOFError, FileNotFoundError):
            return None

    def stop(self):
        self.raven.stop()


class SecondsBackwardLookingFilter(object):

    def __init__(self, data):
        pass

    def __call__(self, data):
        if data._state == data._ST_LIVE:
            data.datetime[0] = data.date2num(data.datetime.datetime(0) + timedelta(seconds=5))
        return False


class MinutesBackwardLookingFilter(object):

    def __init__(self, data):
        pass

    def __call__(self, data):
        data.datetime[0] = data.date2num(data.datetime.datetime(0) + timedelta(minutes=1))
        return False


def getdata(ticker):
    backfill = store.getdata(dataname=ticker, fromdate=fromdate, historical=True,
                             timeframe=bt.TimeFrame.Minutes,
                             compression=1, sessionstart=sessionstart,
                             sessionend=sessionend)
    backfill.addfilter(MinutesBackwardLookingFilter)

    data = store.getdata(dataname=ticker, rtbar=True, timeframe=bt.TimeFrame.Minutes,
                         compression=1, sessionstart=sessionstart,
                         sessionend=sessionend, backfill_from=backfill)

    data.addfilter(SecondsBackwardLookingFilter)

    cerebro.adddata(data)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=15)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=75,
                         boundoff=45)


indexes = [
    "NIFTY50_IND_NSE",
    "BANKNIFTY_IND_NSE",
]

cerebro = bt.Cerebro(runonce=False)
cerebro.addstrategy(MyStrategy)

store = bt.stores.IBStore(port=7497, _debug=True)
cerebro.setbroker(store.getbroker())

cerebro.addcalendar("BSE")

for index in indexes:
    getdata(index)

thestrats = cerebro.run()