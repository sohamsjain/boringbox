import sys
from datetime import timedelta, time
from queue import Queue

import backtrader as bt
import pandas as pd
from backtrader.filters import SessionFilter
from backtrader.utils import AutoOrderedDict
from backtrader.utils.dateintern import num2date
from sqlalchemy import create_engine, inspect

from models3 import *
from mylogger.logger import ExecutionReport
from tradingschedule import nextclosingtime

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


def call_list(underlying, expiry, price, width):
    u = cds.underlying == underlying
    e = cds.expiry == expiry
    r = cds.right == "C"
    fdf = cds[u & e & r]
    strikes: list = fdf.strike.to_list()
    strikes.sort()
    itmbound = price - width * 2
    otmbound = price + width * 10
    contracts = list()

    for strike in strikes:
        if itmbound <= strike <= otmbound:
            sdf = fdf[fdf.strike == strike]
            contracts.append(get_contract(symbol=sdf.symbol.values[0]))

    return contracts


def put_list(underlying, expiry, price, width):
    u = cds.underlying == underlying
    e = cds.expiry == expiry
    r = cds.right == "P"
    fdf = cds[u & e & r]
    strikes: list = fdf.strike.to_list()
    strikes.sort()
    itmbound = price + width * 2
    otmbound = price - width * 10
    contracts = list()

    for strike in strikes:
        if otmbound <= strike <= itmbound:
            sdf = fdf[fdf.strike == strike]
            contracts.append(get_contract(symbol=sdf.symbol.values[0]))

    return contracts


# datafeed params
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)

tradingday = AutoOrderedDict()
tradingday.date = nextclosingtime.date()
tradingday.sessionstart = nextclosingtime.replace(hour=9, minute=15)
tradingday.inittime = nextclosingtime.replace(hour=9, minute=40)
tradingday.entrytime = nextclosingtime.replace(hour=9, minute=49, second=45)
tradingday.exittime = nextclosingtime.replace(hour=15, minute=0)
tradingday.sessionend = nextclosingtime


class MyStrategy(bt.Strategy):

    def __init__(self):
        self.underlyingresource: Dict[str, Underlying] = AutoOrderedDict()
        self.legsbyorder: dict = dict()

        for data in self.datas:
            btsymbol = data._dataname
            underlyingcontract = get_contract(btsymbol=btsymbol)
            underlying = Underlying(contract=underlyingcontract, status=Underlying.CREATED)
            underlying.data = data
            self.underlyingresource[btsymbol] = underlying

        # self.clarke: Clarke = Clarke()
        # self.clarkeq: Optional[Queue] = None
        # self.clarkethread = Thread(target=self.sendaclarke, name="clarke", daemon=True)
        # self.clarkethread.start()
        # self.clarkeq.put("Market Starts")

        self.lasttimestamp = tradingday.sessionstart
        self.strikesinitialised = False
        self.strangleinitialised = False
        self.prevlen = None

    def notify_order(self, order):

        if order.status in [order.Submitted, order.Accepted, order.Partial]:
            return

        try:
            leg = self.legsbyorder[order.ref]
            self.legsbyorder.pop(order.ref)
        except KeyError as k:
            print(k)
            return

        if order.status == order.Completed:

            if order.isbuy():

                leg.buying_price = order.executed.price
                leg.filled = abs(order.executed.size)
                leg.buying_cost = leg.buying_price * leg.filled
                leg.buying_commission = round(order.executed.comm)

                if leg.isbuy:
                    leg.status = Child.BOUGHT
                    leg.opened_at = num2date(order.executed.dt)
                else:
                    leg.status = Child.SQUAREDOFF
                    leg.closed_at = num2date(order.executed.dt)
                    leg.pnl = leg.selling_cost - leg.buying_cost

            else:

                leg.selling_price = order.executed.price
                leg.filled = abs(order.executed.size)
                leg.selling_cost = leg.selling_price * leg.filled
                leg.selling_commission = round(order.executed.comm)

                if leg.isbuy:
                    leg.status = Child.SQUAREDOFF
                    leg.closed_at = num2date(order.executed.dt)
                    leg.pnl = leg.selling_cost - leg.buying_cost
                else:
                    leg.status = Child.SOLD
                    leg.opened_at = num2date(order.executed.dt)

        elif order.status == order.Canceled:
            leg.status = Child.CANCELLED
        elif order.status == order.Margin:
            leg.status = Child.MARGIN
        elif order.status == order.Rejected:
            leg.status = Child.REJECTED

        underlying = leg.underlying
        underlying.orders.remove(order)

        if leg.status == Child.SQUAREDOFF:
            underlying.closedlegs.append(leg)
            underlying.legs.remove(leg)

        if underlying.orders == [None]:  # All child orders processed
            underlying.orders.clear()

            if underlying.status in Underlying.PENDING:

                underlying.status = Underlying.ENTRY
                underlying.opened_at = num2date(order.executed.dt)

                for leg in underlying.children:
                    if leg.status in [Child.MARGIN, Child.REJECTED]:
                        underlying.status = Underlying.ABORT
                        break

                    leg.sl = leg.selling_price + 15

            elif underlying.status in Underlying.OPEN:
                underlying.status = underlying.nextstatus

                if underlying.status == Underlying.CALLSQUAREDOFF:
                    putleg = underlying.legs[0]
                    putleg.sl = putleg.selling_price

                elif underlying.status == Underlying.PUTSQUAREDOFF:
                    callleg = underlying.legs[0]
                    callleg.sl = callleg.selling_price

                else:
                    underlying.closed_at = num2date(order.executed.dt)
                    underlying.pnl = sum([c.pnl for c in underlying.children if c.pnl is not None])
                    underlying.status = Underlying.PROFIT if underlying.pnl > 0 else Underlying.LOSS

            elif underlying.status == Underlying.ABORT:
                underlying.closed_at = num2date(order.executed.dt)
                underlying.pnl = sum([c.pnl for c in underlying.children if c.pnl is not None])
                underlying.status = Underlying.FORCECLOSED

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

        for btsymbol, underlying in self.underlyingresource.items():

            try:  # In case a new datafeed is added, it may not produce a bar
                underlying.lastprice = underlying.data.close[
                    0]  # This is a work around to eliminate unnecessary IndexError
                for child in underlying.children:  # Must apply for xone.data as well as child.data
                    child.lastprice = child.data.close[0]
            except IndexError:
                continue

            if underlying.orders:
                continue

            self.lasttimestamp = self.datas[0].datetime.datetime(0)

            if underlying.status == Underlying.CREATED:

                if self.lasttimestamp >= tradingday.inittime:

                    underlying.status = Underlying.INITIALISED

                    lastprice = underlying.lastprice
                    celist: List[Contract] = call_list(underlying=underlying.contract.underlying, expiry=nearest_expiry,
                                                       price=lastprice, width=80)
                    pelist: List[Contract] = put_list(underlying=underlying.contract.underlying, expiry=nearest_expiry,
                                                      price=lastprice, width=80)

                    for callcontract in celist:
                        child = Child(contract=callcontract, underlying=underlying, status=Child.CREATED)
                        data = self.get_child_datagroup(callcontract.btsymbol)
                        child.data = data
                        underlying.cestrikes.update({callcontract.btsymbol: child})
                        underlying.children.append(child)

                    for putcontract in pelist:
                        child = Child(contract=putcontract, underlying=underlying, status=Child.CREATED)
                        data = self.get_child_datagroup(putcontract.btsymbol)
                        child.data = data
                        underlying.pestrikes.update({putcontract.btsymbol: child})
                        underlying.children.append(child)

            elif underlying.status == Underlying.INITIALISED:

                if self.lasttimestamp >= tradingday.entrytime:

                    underlying.status = Underlying.ENTRYHIT

                    callchain = list()
                    for childsymbol, child in underlying.cestrikes.items():
                        callchain.append(dict(
                            lastprice=child.lastprice,
                            strike=child.contract.strike,
                            symbol=child.contract.symbol,
                            btsymbol=child.contract.btsymbol,
                            difference=abs(child.lastprice - 80)
                        ))

                    calldf = pd.DataFrame(callchain)
                    minima = min(calldf.difference)
                    selectedcestrike = calldf[calldf.difference == minima].btsymbol.values[-1]
                    selectedcechild = underlying.cestrikes[selectedcestrike]

                    underlying.legs.append(selectedcechild)

                    putchain = list()
                    for childsymbol, child in underlying.pestrikes.items():
                        putchain.append(dict(
                            lastprice=child.lastprice,
                            strike=child.contract.strike,
                            symbol=child.contract.symbol,
                            btsymbol=child.contract.btsymbol,
                            difference=abs(child.lastprice - 80)
                        ))

                    putdf = pd.DataFrame(putchain)
                    minima = min(putdf.difference)
                    selectedpestrike = putdf[putdf.difference == minima].btsymbol.values[0]
                    selectedpechild = underlying.pestrikes[selectedpestrike]

                    underlying.legs.append(selectedpechild)

                    for leg in underlying.legs:
                        size = leg.contract.lotsize
                        order = self.sell(data=leg.data, size=size)
                        underlying.orders.append(order)
                        self.legsbyorder[order.ref] = leg
                    underlying.orders.append(None)

            elif underlying.status in Underlying.OPEN:

                if underlying.status == Underlying.ENTRY:

                    for leg in underlying.legs:
                        if leg.lastprice > leg.sl:
                            underlying.nextstatus = Underlying.CALLSQUAREDOFF if leg.contract.right == "C" else Underlying.PUTSQUAREDOFF
                            order = self.buy(data=leg.data, size=leg.filled)
                            underlying.orders.append(order)
                            self.legsbyorder[order.ref] = leg
                            underlying.orders.append(None)
                            break

                else:
                    leg = underlying.legs[0]
                    if leg.lastprice > leg.sl:
                        underlying.nextstatus = Underlying.CLOSING
                        order = self.buy(data=leg.data, size=leg.filled)
                        underlying.orders.append(order)
                        self.legsbyorder[order.ref] = leg
                        underlying.orders.append(None)

            elif underlying.status == Underlying.ABORT:
                for leg in underlying.legs:
                    if leg.filled > 0:
                        order = self.buy(data=leg.data, size=leg.filled)
                        underlying.orders.append(order)
                        self.legsbyorder[order.ref] = leg
                if len(underlying.orders):
                    underlying.orders.append(None)
                else:
                    underlying.closed_at = self.lasttimestamp
                    underlying.pnl = sum([c.pnl for c in underlying.children if c.pnl is not None])
                    underlying.status = Underlying.FORCECLOSED

    def get_child_datagroup(self, btsymbol):

        print(btsymbol)

        data = store.getdata(dataname=btsymbol, rtbar=True, timeframe=bt.TimeFrame.Minutes,
                             compression=1, sessionstart=sessionstart,
                             sessionend=sessionend, backfill_start=False)

        data.addfilter(SecondsBackwardLookingFilter)
        data.addfilter(SessionFilter)

        tickdata = self.cerebro.adddata(data)

        tickdata.reset()

        if self.cerebro._exactbars < 1:
            tickdata.extend(size=self.cerebro.params.lookahead)

        tickdata._start()

        if self.cerebro._dopreload:
            tickdata.preload()

        self.cerebro.runrestart()

        return tickdata

    def sendaclarke(self):
        self.clarkeq = Queue()
        while True:
            self.clarke.send_all_clients(self.clarkeq.get())

    def stop(self):
        self.clarke.stop()


class SecondsBackwardLookingFilter(object):

    def __init__(self, data):
        pass

    def __call__(self, data):
        if data._state == data._ST_LIVE:
            data.datetime[0] = data.date2num(data.datetime.datetime(0) + timedelta(seconds=5))
        return False


def getdata(ticker):
    data = store.getdata(dataname=ticker, rtbar=True, timeframe=bt.TimeFrame.Minutes,
                         compression=1, sessionstart=sessionstart,
                         sessionend=sessionend, backfill_start=False)

    data.addfilter(SecondsBackwardLookingFilter)
    data.addfilter(SessionFilter)
    cerebro.adddata(data)


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
