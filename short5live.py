import pickle
import sys
from datetime import timedelta
from threading import Thread
from typing import Dict, Any

import backtrader as bt
import pandas as pd
from backtrader.filters import SessionFilter
from backtrader.utils import AutoOrderedDict
from backtrader.utils.dateintern import num2date
from sqlalchemy import create_engine, inspect

from indicators.supertrend import SuperTrend
from models import *
from mygoogle.sprint import GoogleSprint
from mylogger.logger import ExecutionReport
from mytelegram.spark import Spark
from tradingschedule import nextclosingtime, lastclosingtime

exRep = ExecutionReport(__file__.split("/")[-1].split(".")[0])


class Db:

    def __init__(self):
        self.dialect = "mysql"
        self.driver = "pymysql"
        self.username = "soham"
        self.password = "Soham19jain98"
        self.host = "52.70.61.124"
        self.port = "3306"
        self.database = "cerebelle"
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


# datafeed params
fromdate = lastclosingtime.date() - timedelta(days=3)
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

        self.raven: Spark = Spark()
        self.ravenq: Optional[Queue] = None
        self.raventhread = Thread(target=self.sendaraven, name="raven", daemon=True)
        self.raventhread.start()

        self.sprint = GoogleSprint()
        self.spread = self.sprint.gs.open("Short5Live Trading Reports")
        self.sheet = self.spread.worksheet("Trades")

        self.states = self.deserialize()
        self.states_restored = datetime.max

        self.intraday_squareoff_triggered = False

        nonce = 0
        while nonce < len(self.datas):
            tickdata = self.datas[nonce]  # RTbar
            dsidata = self.datas[nonce + 1]  # Minute 5
            trenddata = self.datas[nonce + 2]  # Minute 15
            curvedata = self.datas[nonce + 3]  # Minute 75

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
            nonce += 4

        self.prevlen = None

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
                child.filled += order.executed.size
                child.buying_cost = child.buying_price * child.filled
                child.buying_commission = round(order.executed.comm)

                if child.isbuy:
                    child.status = ChildStatus.BOUGHT
                    child.opened_at = num2date(order.executed.dt)
                else:
                    child.status = ChildStatus.SQUAREDOFF
                    child.closed_at = num2date(order.executed.dt)
                    child.pnl = child.selling_cost - child.buying_cost

            else:

                child.selling_price = order.executed.price
                child.filled += order.executed.size
                child.selling_cost = child.selling_price * child.filled
                child.selling_commission = round(order.executed.comm)

                if child.isbuy:
                    child.status = ChildStatus.SQUAREDOFF
                    child.closed_at = num2date(order.executed.dt)
                    child.pnl = child.selling_cost - child.buying_cost
                else:
                    child.status = ChildStatus.SOLD
                    child.opened_at = num2date(order.executed.dt)

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
                xone.opened_at = num2date(order.executed.dt)
                self.openxones.append(xone)

                for child in xone.children:
                    if child.status in [ChildStatus.MARGIN, ChildStatus.REJECTED]:
                        xone.status = XoneStatus.ABORT
                        break

                xone.update_statlist(adin=xone.opened_at.date(), atin=xone.opened_at.time(), zstatus=xone.status,
                                     cstatus=child.status, csize=child.filled,
                                     cbuyprice=child.buying_price, cbuycost=child.buying_cost,
                                     cbuycomm=child.buying_commission,
                                     csellprice=child.selling_price, csellcost=child.selling_cost,
                                     csellcomm=child.selling_commission)
                self.updatesheet(xone)

            elif xone.status in XoneStatus.OPEN:
                xone.closed_at = num2date(order.executed.dt)
                xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                xone.status = xone.nextstatus
                self.removexone(xone)
                self.openxones.remove(xone)

                xone.update_statlist(adout=xone.closed_at.date(), atout=xone.closed_at.time(), zstatus=xone.status,
                                     cstatus=child.status,
                                     cbuyprice=child.buying_price, cbuycost=child.buying_cost,
                                     cbuycomm=child.buying_commission,
                                     csellprice=child.selling_price, csellcost=child.selling_cost,
                                     csellcomm=child.selling_commission, pnl=xone.pnl)
                self.updatesheet(xone)

            elif xone.status == XoneStatus.ABORT:
                xone.closed_at = num2date(order.executed.dt)
                xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                xone.status = XoneStatus.FORCECLOSED
                self.removexone(xone)
                self.openxones.remove(xone)
                xone.update_statlist(adout=xone.closed_at.date(), atout=xone.closed_at.time(), zstatus=xone.status,
                                     cstatus=child.status,
                                     cbuyprice=child.buying_price, cbuycost=child.buying_cost,
                                     cbuycomm=child.buying_commission,
                                     csellprice=child.selling_price, csellcost=child.selling_cost,
                                     csellcomm=child.selling_commission, pnl=xone.pnl)
                self.updatesheet(xone)

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

        _len = len(self)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        for btsymbol, resource in self.xone_resource.items():
            dsi: DSIndicator = resource.dsi
            if not dsi.notifications.empty():
                self.readnotifications(dsi)

        if self.datetime.datetime(0) <= self.states_restored:
            return

        for xone in self.allxones:
            data = xone.datagroup.tickdata
            dtime = data.datetime.datetime(0)
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
                        xone.exit_at = dtime
                        xone.exithit = data.close[0]
                        xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(), pnl=0,
                                             zexithit=xone.exithit, zstatus=xone.status)
                        self.updatesheet(xone)
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.low[0] < xone.stoploss:
                        xone.status = XoneStatus.FAILED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        xone.exit_at = dtime
                        xone.exithit = data.close[0]
                        if xone.index:
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(), pnl=0,
                                                 zexithit=xone.exithit, zstatus=xone.status)
                            self.updatesheet(xone)
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.low[0] <= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = dtime
                            xone.entryhit = data.close[0]
                            self.ravenq.put(xone.notification())
                            row, index = self.nextrowindex()
                            xone.init_statlist(row, index)
                            self.updatesheet(xone)

                        if xone.tradable():
                            xone.open_children = True

                else:
                    if (xone.status == XoneStatus.ENTRYHIT) and (data.low[0] <= xone.target):
                        xone.status = XoneStatus.MISSED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        xone.exit_at = dtime
                        xone.exithit = data.close[0]
                        xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(), pnl=0,
                                             zexithit=xone.exithit, zstatus=xone.status)
                        self.updatesheet(xone)
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.high[0] > xone.stoploss:
                        xone.status = XoneStatus.FAILED
                        for child in xone.children:
                            child.status = ChildStatus.UNUSED
                        xone.exit_at = dtime
                        xone.exithit = data.close[0]
                        if xone.index:
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(), pnl=0,
                                                 zexithit=xone.exithit, zstatus=xone.status)
                            self.updatesheet(xone)
                        self.removexone(xone)
                        self.ravenq.put(xone.notification())
                        continue

                    if data.high[0] >= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = dtime
                            xone.entryhit = data.close[0]
                            self.ravenq.put(xone.notification())
                            row, index = self.nextrowindex()
                            xone.init_statlist(row, index)
                            self.updatesheet(xone)

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
                            xone.exit_at = dtime
                            xone.exithit = data.close[0]
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                 zexithit=xone.exithit)
                            self.updatesheet(xone)
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                    elif data.high[0] >= xone.target:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.TARGETHIT
                            xone.nextstatus = XoneStatus.TARGET
                            xone.exit_at = dtime
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                 zexithit=xone.exithit)
                            self.updatesheet(xone)
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                else:
                    if data.high[0] > xone.stoploss:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.STOPLOSSHIT
                            xone.nextstatus = XoneStatus.STOPLOSS
                            xone.exit_at = dtime
                            xone.exithit = data.close[0]
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                 zexithit=xone.exithit)
                            self.updatesheet(xone)
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                    elif data.low[0] <= xone.target:
                        if xone.status == XoneStatus.ENTRY:
                            xone.status = XoneStatus.TARGETHIT
                            xone.nextstatus = XoneStatus.TARGET
                            xone.exit_at = dtime
                            xone.exithit = data.close[0]
                            xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                 zexithit=xone.exithit)
                            self.updatesheet(xone)
                            self.ravenq.put(xone.notification())

                        xone.close_children = True

                if not xone.close_children:
                    child = xone.children[0]
                    if not child.supertrend_stoploss:
                        firstCheck = False
                        datamin5 = child.datagroup.supertrenddata
                        if child.supertrenddatalts is None:
                            child.supertrenddatalts = datamin5.datetime.datetime(0)
                            firstCheck = True
                        if firstCheck or (datamin5.datetime.datetime(0) > child.supertrenddatalts):
                            supertrend = child.datagroup.supertrend
                            if datamin5.close[0] < supertrend[0]:
                                child.supertrend_stoploss = True

                    else:
                        datamin5 = child.datagroup.supertrenddata
                        stddt = datamin5.datetime.datetime(0)  # Supertrenddata datetime
                        if stddt > child.supertrenddatalts:
                            child.supertrenddatalts = stddt
                            supertrend = child.datagroup.supertrend
                            if datamin5.close[0] > supertrend[0]:
                                if xone.status == XoneStatus.ENTRY:
                                    xone.status = XoneStatus.STRENDSLHIT
                                    xone.nextstatus = XoneStatus.STRENDSL
                                    xone.exit_at = dtime
                                    xone.exithit = data.close[0]
                                    xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                         zexithit=xone.exithit)
                                    self.updatesheet(xone)
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
                    xone.closed_at = dtime
                    xone.pnl = sum([c.pnl for c in xone.children if c.pnl is not None])
                    xone.status = XoneStatus.FORCECLOSED
                    xone.update_statlist(adout=xone.closed_at.date(), atout=xone.closed_at.time(), zstatus=xone.status,
                                         pnl=xone.pnl)
                    self.updatesheet(xone)
                    self.removexone(xone)
                    self.openxones.remove(xone)

            else:
                self.removexone(xone)

        if self.data0.datetime.datetime(0) >= tradingday.intraday_squareoff:
            if not self.intraday_squareoff_triggered:
                self.intraday_squareoff_triggered = True
                if self.openxones:
                    for xone in self.openxones:
                        xone.exithit = xone.datagroup.tickdata.close[0]
                        xone.exit_at = self.data0.datetime.datetime(0)
                        xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                             zexithit=xone.exithit)
                        self.updatesheet(xone)
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

        atr = dsi.curve.atrvalue
        close = dsi.data0.close[0]
        upperband = round(close + atr * 3, 2)
        lowerband = round(close - atr * 3, 2)

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
            self.create_xone(z, notify=False)

    def create_xone(self, origin: Optional[DZone], notify=True):

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

        if notify:
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
        data.addfilter(SessionFilter)

        tickdata = self.cerebro.adddata(data)
        resampleddata = self.cerebro.resampledata(data, timeframe=bt.TimeFrame.Minutes, compression=5)

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

    def nextrowindex(self):
        allvalues = self.sheet.get_all_values()
        row = len(allvalues) + 1
        index = row - 3
        return row, index

    def updatesheet(self, xone):
        self.sheet.update(f"A{xone.row}", [xone.statlist])

    def deserialize(self):
        try:
            with open("dsicache/min5/5dsi.obj", "rb") as file:
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
    data.addfilter(SessionFilter)
    cerebro.adddata(data)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=5)

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

store = bt.stores.IBStore(port=7497, _debug=False)
cerebro.setbroker(store.getbroker())

cerebro.addcalendar("BSE")

for index in indexes:
    getdata(index)

try:
    thestrats = cerebro.run()
except Exception as e:
    exRep.submit(*sys.exc_info())
