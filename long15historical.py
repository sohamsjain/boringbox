import pickle
import sys
from datetime import timedelta
from threading import Thread
from typing import Dict, Any
from time import sleep

import backtrader as bt
import pandas as pd
from backtrader.filters import SessionFilter
from backtrader.utils import AutoOrderedDict
from backtrader.utils.dateintern import num2date
from sqlalchemy import create_engine, inspect

from models2 import *
from indicators.trail import TrailingStoploss
from mygoogle.sprint import GoogleSprint
from mylogger.logger import ExecutionReport
from mytelegram.raven import Raven
from tradingschedule import nextclosingtime, lastclosingtime
from analyzers.myanalyzers import PivotAnalyzer

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


# datafeed params
fromdate = lastclosingtime.date() - timedelta(days=365*1)
todate = lastclosingtime.date()
# fromdate = date(day=1, month=1, year=2011)
# todate = date(day=1, month=1, year=2012)
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)
lastorder = time(hour=15, minute=0)
intraday_squareoff = time(hour=15, minute=25)


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

        self.clarke: Raven = Raven()
        self.clarkeq: Optional[Queue] = None
        self.clarkethread = Thread(target=self.sendaclarke, name="clarke", daemon=True)
        self.clarkethread.start()

        self.sprint = GoogleSprint()
        self.spread = self.sprint.gs.open("Long15Live Trading Reports")
        self.sheet = self.spread.worksheet("Backtest6")
        self.lastsheetupdate = datetime.now()

        self.states = self.deserialize()
        self.states_restored = datetime.max

        nonce = 0
        while nonce < len(self.datas):
            tickdata = self.datas[nonce]  # Minute 1
            dsidata = self.datas[nonce + 1]  # Minute 15
            trenddata = self.datas[nonce + 2]  # Minute 75
            curvedata = self.datas[nonce + 3]  # Day 1

            btsymbol = dsidata._dataname
            try:
                savedstate = self.states[btsymbol] if self.states else None
            except KeyError:
                savedstate = None

            self.xone_resource[btsymbol].tickdata = tickdata
            self.xone_resource[btsymbol].dsidata = dsidata
            self.xone_resource[btsymbol].trenddata = trenddata
            self.xone_resource[btsymbol].curvedata = curvedata
            self.xone_resource[btsymbol].pivots = Pivots(dsidata)
            self.xone_resource[btsymbol].tsl = TrailingStoploss(dsidata)
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
                child.filled = abs(order.executed.size)
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
                child.filled = abs(order.executed.size)
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

            self.clarkeq.put(xone.notification())

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
                        self.clarkeq.put(xone.notification())
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
                        self.clarkeq.put(xone.notification())
                        continue

                    if data.low[0] <= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = dtime
                            xone.entryhit = data.close[0]
                            self.clarkeq.put(xone.notification())
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
                        self.clarkeq.put(xone.notification())
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
                        self.clarkeq.put(xone.notification())
                        continue

                    if data.high[0] >= xone.entry:
                        if xone.status == XoneStatus.CREATED:
                            xone.status = XoneStatus.ENTRYHIT
                            xone.entry_at = dtime
                            xone.entryhit = data.close[0]
                            self.clarkeq.put(xone.notification())
                            row, index = self.nextrowindex()
                            xone.init_statlist(row, index)
                            self.updatesheet(xone)

                        if xone.tradable():
                            xone.open_children = True

                if xone.open_children and data.datetime.time(0) <= lastorder:
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

                pivots = self.xone_resource[xone.btsymbol].pivots
                if pivots.new_spl and xone.isbullish:
                    if xone.trailing_stoploss is None or pivots.spl_value > xone.trailing_stoploss:
                        xone.trailing_stoploss = pivots.spl_value
                        xone.update_statlist(ztrailingsl=xone.trailing_stoploss)

                if pivots.new_sph and not xone.isbullish:
                    if xone.trailing_stoploss is None or pivots.sph_value < xone.trailing_stoploss:
                        xone.trailing_stoploss = pivots.sph_value
                        xone.update_statlist(ztrailingsl=xone.trailing_stoploss)

                tsl = self.xone_resource[xone.btsymbol].tsl
                if tsl.bullish[0] and xone.isbullish:
                    if xone.trailing_stoploss is None or tsl.bullish[0] > xone.trailing_stoploss:
                        xone.trailing_stoploss = tsl.bullish[0]
                        xone.update_statlist(ztrailingsl=xone.trailing_stoploss)

                if tsl.bearish[0] and not xone.isbullish:
                    if xone.trailing_stoploss is None or tsl.bearish[0] < xone.trailing_stoploss:
                        xone.trailing_stoploss = tsl.bearish[0]
                        xone.update_statlist(ztrailingsl=xone.trailing_stoploss)

                if xone.trailing_stoploss is None:
                        # or (xone.isbullish and xone.trailing_stoploss < xone.entry) \
                        # or (not xone.isbullish and xone.trailing_stoploss > xone.entry):

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
                                self.clarkeq.put(xone.notification())

                            xone.close_children = True

                        elif data.high[0] >= xone.target:
                            if xone.status == XoneStatus.ENTRY:
                                xone.status = XoneStatus.TARGETHIT
                                xone.nextstatus = XoneStatus.TARGET
                                xone.exit_at = dtime
                                xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                     zexithit=xone.exithit)
                                self.updatesheet(xone)
                                self.clarkeq.put(xone.notification())

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
                                self.clarkeq.put(xone.notification())

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
                                self.clarkeq.put(xone.notification())

                            xone.close_children = True

                else:

                    if xone.isbullish:
                        if data.low[0] < xone.trailing_stoploss:
                            if xone.status == XoneStatus.ENTRY:
                                xone.status = XoneStatus.TRAILINGSLHIT
                                xone.nextstatus = XoneStatus.TRAILINGSL
                                xone.exit_at = dtime
                                xone.exithit = data.close[0]
                                xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                     zexithit=xone.exithit)
                                self.updatesheet(xone)
                                self.clarkeq.put(xone.notification())

                            xone.close_children = True

                    else:
                        if data.high[0] > xone.trailing_stoploss:
                            if xone.status == XoneStatus.ENTRY:
                                xone.status = XoneStatus.TRAILINGSLHIT
                                xone.nextstatus = XoneStatus.TRAILINGSL
                                xone.exit_at = dtime
                                xone.exithit = data.close[0]
                                xone.update_statlist(tdout=xone.exit_at.date(), ttout=xone.exit_at.time(),
                                                     zexithit=xone.exithit)
                                self.updatesheet(xone)
                                self.clarkeq.put(xone.notification())

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

        for k, v in self.xone_resource.items():
            pivots: Pivots = v.pivots
            if pivots.new_sph:
                pivots.new_sph = False

            if pivots.new_spl:
                pivots.new_spl = False

        if self.data0.datetime.time(0) == intraday_squareoff:
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

    def create_xone(self, origin: Optional[DZone], notify=True):

        if origin in self.xonesbyorigin:
            return self.xonesbyorigin[origin]

        btsymbol = origin.dsi.data0._dataname
        xonecontract = get_contract(btsymbol=btsymbol)

        xone = Xone(origin=origin,
                    contract=xonecontract,
                    status=XoneStatus.CREATED)

        xone.datagroup = self.xone_resource[xone.btsymbol]

        child = Child(contract=xonecontract,
                      xone=xone,
                      status=ChildStatus.CREATED)

        child.datagroup = self.get_child_datagroup(child.btsymbol)

        xone.children.append(child)

        self.allxones.append(xone)
        self.xonesbyorigin.update({xone.origin: xone})

        if notify:
            self.clarkeq.put(xone.notification())

    def removexone(self, xone):
        if xone.origin in self.xonesbyorigin:
            self.xonesbyorigin.pop(xone.origin)
            self.allxones.remove(xone)

    def get_child_datagroup(self, btsymbol):

        if btsymbol in self.child_resource:
            return self.child_resource[btsymbol]

        self.child_resource[btsymbol].tickdata = self.xone_resource[btsymbol].tickdata

        return self.child_resource[btsymbol]

    def sendaclarke(self):
        self.clarkeq = Queue()
        while True:
            self.clarke.send_all_clients(self.clarkeq.get())

    def nextrowindex(self):
        allvalues = self.sheet.get_all_values()
        row = len(allvalues) + 1
        index = row - 3
        return row, index

    def updatesheet(self, xone):
        if datetime.now() - self.lastsheetupdate < timedelta(seconds=1):
            sleep(1)
        self.sheet.update(f"A{xone.row}", [xone.statlist])
        self.lastsheetupdate = datetime.now()
        pass

    def deserialize(self):
        try:
            with open("dsicache/min15/15dsi.obj", "rb") as file:
                dsidict = pickle.load(file)
            return dsidict
        except (EOFError, FileNotFoundError):
            return None

    def stop(self):
        self.clarke.stop()


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
    data = store.getdata(dataname=ticker, fromdate=fromdate, todate=todate, sessionstart=sessionstart,
                         historical=True, sessionend=sessionend, timeframe=bt.TimeFrame.Minutes,
                         compression=1)

    data.addfilter(MinutesBackwardLookingFilter)
    data.addfilter(SessionFilter)

    cerebro.adddata(data)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=5)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=75,
                         boundoff=45)

    dailydata = store.getdata(dataname=ticker, fromdate=fromdate, todate=todate, sessionstart=sessionstart,
                              historical=True, sessionend=sessionend, timeframe=bt.TimeFrame.Days,
                              compression=1)

    cerebro.adddata(dailydata)

indexes = [
    "NIFTY50_IND_NSE",
    # "BANKNIFTY_IND_NSE",
]

cerebro = bt.Cerebro(runonce=False)
cerebro.addstrategy(MyStrategy)
cerebro.addanalyzer(PivotAnalyzer)
cerebro.broker.set_cash(200000)

store = bt.stores.IBStore(port=7497, _debug=True)

cerebro.addcalendar("BSE")

for index in indexes:
    getdata(index)

try:
    thestrats = cerebro.run()
except Exception as e:
    exRep.submit(*sys.exc_info())
