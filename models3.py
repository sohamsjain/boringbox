from __future__ import annotations

from datetime import datetime, date, time
from typing import Optional, List, Dict

from pandas import Timestamp

index, _day, _date, time_in, time_out, symbol, expiry, strike, right, status, _type, size, buy_price, buy_cost, \
buy_comm, sell_price, sell_cost, sell_comm, pnl, highest_from_entry_to_exit, \
lowest_from_entry_to_exit = range(21)

cols = dict(index=0, day=1, date=2, time_in=3, time_out=4, symbol=5, expiry=6, strike=7, right=8,
            status=9, type=10, size=11, buy_price=12, buy_cost=13, buy_comm=14, sell_price=15,
            sell_cost=16, sell_comm=17, pnl=18, highest_from_entry_to_exit=19, lowest_from_entry_to_exit=20)


class SecType:
    IND = "IND"
    STK = "STK"
    FUT = "FUT"
    OPT = "OPT"


class Contract:
    def __init__(self, underlying, sectype, exchange, currency, symbol, strike, right, expiry, multiplier, btsymbol,
                 lotsize):
        self.underlying = underlying
        self.sectype = sectype
        self.exchange = exchange
        self.currency = currency
        self.symbol = symbol
        self.strike = strike
        self.right = right
        self.expiry: Optional[datetime] = Timestamp(expiry).to_pydatetime()
        self.multiplier = multiplier
        self.btsymbol = btsymbol
        self.lotsize = lotsize


class Underlying:
    CREATED = "CREATED"  # PENDING
    INITIALISED = "INITIALISED"  # PENDING
    ENTRYHIT = "ENTRY HIT"  # PENDING
    ENTRY = "ENTRY"  # OPEN
    CALLSQUAREDOFF = "CALL SQUARED OFF"  # OPEN
    PUTSQUAREDOFF = "PUT SQUARED OFF"  # OPEN
    CLOSING = "CLOSING"  # CLOSING
    ABORT = "ABORT"  # CLOSING
    PROFIT = "PROFIT"  # CLOSED
    LOSS = "LOSS"  # CLOSED
    FORCECLOSED = "FORCE CLOSED"  # CLOSED
    PENDING = [CREATED, ENTRYHIT]
    OPEN = [ENTRY, CALLSQUAREDOFF, PUTSQUAREDOFF]
    CLOSED = [PROFIT, LOSS, FORCECLOSED]

    def __init__(self, contract: Contract, status, created_at=None, entry_at=None, exit_at=None, opened_at=None,
                 closed_at=None, pnl=None):
        self.contract: Contract = contract
        self.symbol = self.contract.symbol
        self.sectype = self.contract.sectype
        self.btsymbol = self.contract.btsymbol
        self.expiry = self.contract.expiry
        self.lastprice = None
        self.created_at: Optional[datetime] = created_at
        self.entry_at: Optional[datetime] = entry_at
        self.exit_at: Optional[datetime] = exit_at
        self.opened_at: Optional[datetime] = opened_at
        self.closed_at: Optional[datetime] = closed_at
        self.status = status
        self.pnl = pnl
        self.cestrikes: Dict[str, Child] = dict()
        self.pestrikes: Dict[str, Child] = dict()
        self.children: List[Child] = list()
        self.legs: List[Child] = list()
        self.closedlegs: List[Child] = list()
        self.orders = list()
        self.nextstatus = None
        self.open_children = False
        self.close_children = False
        self.data = None
        self.row = None
        self.index = None
        self.statlist = [""] * 21
        self.sheet = None

    def init_statlist(self, row, idx):
        self.row = row
        self.index = idx
        self.statlist[index] = idx
        self.statlist[_day] = datetime.now().strftime("%A")
        self.statlist[_date] = datetime.now().strftime("%d/%m/%Y")
        self.statlist[time_in] = ""
        self.statlist[time_out] = ""
        self.statlist[symbol] = self.contract.symbol
        self.statlist[expiry] = str(self.contract.expiry)
        self.statlist[strike] = self.contract.strike
        self.statlist[right] = self.contract.right
        self.statlist[status] = ""
        self.statlist[_type] = ""
        self.statlist[size] = ""
        self.statlist[buy_price] = ""
        self.statlist[buy_cost] = ""
        self.statlist[buy_comm] = ""
        self.statlist[sell_price] = ""
        self.statlist[sell_cost] = ""
        self.statlist[sell_comm] = ""
        self.statlist[pnl] = ""
        self.statlist[highest_from_entry_to_exit] = ""
        self.statlist[lowest_from_entry_to_exit] = ""

    def update_statlist(self, **kwargs):
        for k, v in kwargs.items():
            if k in cols:
                indexofk = cols[k]
                if type(v) in [datetime, date, time]:
                    v = str(v)
                self.statlist[indexofk] = v


class Child:
    CREATED = "CREATED"  # CREATED
    BOUGHT = "BOUGHT"  # OPEN
    SOLD = "SOLD"  # OPEN
    REJECTED = "REJECTED"  # CLOSED
    MARGIN = "MARGIN"  # CLOSED
    CANCELLED = "CANCELLED"  # CLOSED
    SQUAREDOFF = "SQUARED OFF"  # CLOSED
    UNUSED = "UNUSED"  # CLOSED
    PENDING = [CREATED]
    OPEN = [BOUGHT, SOLD]
    CLOSED = [REJECTED, MARGIN, CANCELLED, SQUAREDOFF, UNUSED]

    def __init__(self, contract: Contract, underlying: Underlying, status, size=None, created_at=None, opened_at=None,
                 closed_at=None, filled=0, buying_price=None, buying_cost=None, buying_commission=None,
                 selling_price=None, selling_cost=None, selling_commission=None, pnl=None):
        self.contract: Contract = contract
        self.underlying: Underlying = underlying
        self.symbol = self.contract.symbol
        self.lastprice = None
        self.sectype = self.contract.sectype
        self.btsymbol = self.contract.btsymbol
        self.expiry = self.contract.expiry
        self.size = size
        self.status = status
        self.sl = float("inf")
        self.isbuy = False
        self.created_at = created_at
        self.opened_at = opened_at
        self.closed_at = closed_at
        self.filled = filled
        self.buying_price = buying_price
        self.buying_cost = buying_cost
        self.buying_commission = buying_commission
        self.selling_price = selling_price
        self.selling_cost = selling_cost
        self.selling_commission = selling_commission
        self.pnl = pnl
        self.data = None
        self.row = None
        self.index = None
        self.statlist = [""] * 21

    def init_statlist(self, row, idx):
        self.row = row
        self.index = idx
        self.statlist[index] = idx
        self.statlist[_day] = datetime.now().strftime("%A")
        self.statlist[_date] = datetime.now().strftime("%d/%m/%Y")
        self.statlist[time_in] = ""
        self.statlist[time_out] = ""
        self.statlist[symbol] = self.contract.symbol
        self.statlist[expiry] = str(self.contract.expiry)
        self.statlist[strike] = self.contract.strike
        self.statlist[right] = self.contract.right
        self.statlist[status] = self.status
        self.statlist[_type] = ""
        self.statlist[size] = ""
        self.statlist[buy_price] = ""
        self.statlist[buy_cost] = ""
        self.statlist[buy_comm] = ""
        self.statlist[sell_price] = ""
        self.statlist[sell_cost] = ""
        self.statlist[sell_comm] = ""
        self.statlist[pnl] = ""
        self.statlist[highest_from_entry_to_exit] = ""
        self.statlist[lowest_from_entry_to_exit] = ""

    def update_statlist(self, **kwargs):
        for k, v in kwargs.items():
            if k in cols:
                indexofk = cols[k]
                if type(v) in [datetime, date, time]:
                    v = str(v)
                self.statlist[indexofk] = v
