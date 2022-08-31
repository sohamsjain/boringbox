from __future__ import annotations

from datetime import datetime
from typing import Optional, List, Dict

from pandas import Timestamp


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
