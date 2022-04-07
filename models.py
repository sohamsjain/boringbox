from __future__ import annotations

from datetime import datetime

from indicators.oddenhancers import *
from util import *


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
        self.expiry: Optional[datetime] = expiry
        self.multiplier = multiplier
        self.btsymbol = btsymbol
        self.lotsize = lotsize


class Xone:
    def __init__(self, origin, contract: Contract, status, created_at=None, entry_at=None, exit_at=None, opened_at=None,
                 closed_at=None, pnl=None):
        self.origin: DZone = origin
        self.contract: Contract = contract
        self.symbol = self.contract.symbol
        self.sectype = self.contract.sectype
        self.btsymbol = self.contract.btsymbol
        self.expiry = self.contract.expiry
        self.entry = self.origin.entry
        self.originalstoploss = self.origin.sl
        self.stoploss = self.origin.sl  # Gets calculated inside extend_stoploss method
        self.target = self.origin.target
        self.lastprice = None
        self.type = XoneType.identify(self.entry, self.stoploss)
        self.created_at: Optional[datetime] = created_at
        self.entry_at: Optional[datetime] = entry_at
        self.exit_at: Optional[datetime] = exit_at
        self.opened_at: Optional[datetime] = opened_at
        self.closed_at: Optional[datetime] = closed_at
        self.status = status
        self.pnl = pnl
        self.children: List[Child] = list()
        self.isbullish = True if self.type == XoneType.BULLISH else False
        self.orders = list()
        self.nextstatus = None
        self.open_children = False
        self.close_children = False
        self.datagroup = None
        self.datr = self.origin.dsi.curve.atrvalue
        self.stopbuffer = round(self.datr * 0.02, 2)
        self.extend_stoploss(self.origin.sl)

    def attributes(self):
        attributes = dict(symbol=self.symbol,
                          type=self.type,
                          entry=self.entry,
                          stoploss=self.stoploss,
                          target=self.target,
                          status=self.status,
                          pnl=self.pnl,
                          lastprice=self.lastprice,
                          sectype=self.sectype,
                          expiry=self.expiry)
        attributes = {k: v for k, v in attributes.items() if v}
        return attributes

    def notification(self):
        type = '+' if self.type == XoneType.BULLISH else '-'
        notif = f"{self.status}\n{self.symbol}:\t {self.lastprice}\n{type} {self.entry}  SL {self.stoploss}  T {self.target}"
        for child in self.children:
            ctype = '+' if child.type == ChildType.BUY else '-'
            notif += "\n______________________________\n"
            notif += f"{child.status}\n{child.symbol}:\t {child.lastprice}\n{ctype} {child.size}"
        return notif

    def tradable(self) -> bool:
        if self.origin.score >= 10:
            return True
        elif self.origin.score >= 9 and self.origin.ratio >= 3:
            return True
        return False

    def extend_stoploss(self, stoploss):
        self.datr = self.origin.dsi.curve.atrvalue
        self.stopbuffer = round(self.datr * 0.02, 2)
        self.originalstoploss = stoploss
        if self.isbullish:
            self.stoploss -= self.stopbuffer
        else:
            self.stoploss += self.stopbuffer


class Child:
    def __init__(self, contract: Contract, xone: Xone, status, size=None, created_at=None, opened_at=None,
                 closed_at=None, filled=0, buying_price=None, buying_cost=None, buying_commission=None,
                 selling_price=None, selling_cost=None, selling_commission=None, pnl=None):
        self.contract: Contract = contract
        self.xone: Xone = xone
        self.symbol = self.contract.symbol
        self.lastprice = None
        self.sectype = self.contract.sectype
        self.btsymbol = self.contract.btsymbol
        self.expiry = self.contract.expiry
        self.type = ChildType.BUY if self.xone.type == XoneType.BULLISH else ChildType.SELL
        self.type = ChildType.invert(self.sectype) if self.contract.right == "P" else self.sectype
        self.size = size
        self.status = status
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
        self.isbuy = True if self.type == ChildType.BUY else False
        self.datagroup = None

    def attributes(self):
        attributes = dict(symbol=self.symbol,
                          type=self.type,
                          size=self.size,
                          status=self.status,
                          filled=self.filled,
                          pnl=self.pnl,
                          lastprice=self.lastprice,
                          sectype=self.sectype,
                          expiry=self.expiry)
        attributes = {k: v for k, v in attributes.items() if v}
        return attributes
