from __future__ import annotations

from datetime import date, time

from pandas import Timestamp

from indicators.oddenhancers import *
from util import *

tindex, tdin, ttin, tdout, ttout, adin, atin, adout, atout, zsym, zstatus, zscore, ztype, \
zentry, zorgsl, zsl, ztrailingsl, ztarget, zentryhit, zexithit, pnl, oerisk, oereward, oeratio, oetest, \
oestrength, oetab, oecurve, oetrend, csym, cexpiry, cstrike, cright, cstatus, ctype, \
csize, cbuyprice, cbuycost, cbuycomm, csellprice, csellcost, csellcomm = range(42)

tradecols = dict(index=0, tdin=1, ttin=2, tdout=3, ttout=4, adin=5, atin=6, adout=7, atout=8, zsym=9, zstatus=10,
                 zscore=11, ztype=12, zentry=13, zorgsl=14, zsl=15, ztrailingsl=16, ztarget=17, zentryhit=18,
                 zexithit=19, pnl=20,
                 oerisk=21, oereward=22, oeratio=23, oetest=24, oestrength=25, oetab=26, oecurve=27, oetrend=28,
                 csym=29, cexpiry=30, cstrike=31, cright=32, cstatus=33, ctype=34, csize=35, cbuyprice=36, cbuycost=37,
                 cbuycomm=38, csellprice=39, csellcost=40, csellcomm=41)

lasttoken = 0


def gettoken():
    global lasttoken
    lasttoken += 1
    return lasttoken


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


class Xone:
    def __init__(self, origin, contract: Contract, status, created_at=None, entry_at=None, exit_at=None, opened_at=None,
                 closed_at=None, pnl=None):
        self.token = gettoken()
        self.origin: DZone = origin
        self.contract: Contract = contract
        self.symbol = self.contract.symbol
        self.sectype = self.contract.sectype
        self.btsymbol = self.contract.btsymbol
        self.expiry = self.contract.expiry
        self.entry = self.origin.entry
        self.originalstoploss = self.origin.sl
        self.stoploss = self.origin.sl  # Gets calculated inside extend_stoploss method
        self.trailing_stoploss = None
        self.entryhit = None
        self.exithit = None
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
        self.target = self.calculate_target()
        self.row = None
        self.index = None
        self.statlist = [""] * 42

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
        notif = f"{self.token}\t{self.status}\n{self.symbol}:\t {self.lastprice}\n{type} {self.entry}  SL {self.stoploss}  T {self.target}"
        for child in self.children:
            ctype = '+' if child.type == ChildType.BUY else '-'
            notif += "\n______________________________\n"
            notif += f"{child.status}\n{child.symbol}:\t {child.lastprice}\n{ctype} {child.size}"
        return notif

    def tradable(self) -> bool:
        width = self.origin.atrwidth <= 1
        # tab = self.origin.timeatbase <= 3
        strength = self.origin.strength >= 70
        # tcount = self.origin.testcount <= 1
        # tperc = self.origin.testperc <= 25
        # if self.isbullish:
        #     overnight = self.origin.dsi.lasttimestamp.date() > self.origin.demands[0].dtsl.date()
        # else:
        #     overnight = self.origin.dsi.lasttimestamp.date() > self.origin.supplies[0].dtsl.date()
        if width and strength:
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
        self.stoploss = round(self.stoploss, 2)

    def calculate_target(self):
        reward = self.origin.risk * 3
        if self.isbullish:
            self.target = round(self.entry + reward, 2)
        else:
            self.target = round(self.entry - reward, 2)
        return self.target

    def init_statlist(self, row, index):
        self.row = row
        self.index = index
        self.statlist[tindex] = index
        self.statlist[tdin] = str(self.entry_at.date())
        self.statlist[ttin] = str(self.entry_at.time())
        self.statlist[zsym] = self.symbol
        self.statlist[zstatus] = self.status
        self.statlist[zscore] = self.origin.score
        self.statlist[ztype] = self.type
        self.statlist[zentry] = self.entry
        self.statlist[zorgsl] = self.originalstoploss
        self.statlist[zsl] = self.stoploss
        self.statlist[ztrailingsl] = self.trailing_stoploss
        self.statlist[ztarget] = self.target
        self.statlist[zentryhit] = self.entryhit
        self.statlist[oerisk] = self.origin.risk
        self.statlist[oereward] = min(self.origin.reward, 999999999) if self.origin.reward is not None else None
        self.statlist[oeratio] = min(self.origin.ratio, 99999999) if self.origin.ratio is not None else None
        self.statlist[oetest] = self.origin.testcount
        self.statlist[oestrength] = self.origin.strength
        self.statlist[oetab] = self.origin.timeatbase
        self.statlist[oecurve] = self.origin.location
        self.statlist[oetrend] = self.origin.trend
        self.statlist[csym] = self.children[0].symbol
        self.statlist[cexpiry] = str(self.children[0].expiry)
        self.statlist[cstrike] = self.children[0].contract.strike
        self.statlist[cright] = self.children[0].contract.right
        self.statlist[cstatus] = self.children[0].status
        self.statlist[ctype] = self.children[0].type

    def update_statlist(self, **kwargs):
        for k, v in kwargs.items():
            if k in tradecols:
                indexofk = tradecols[k]
                if type(v) in [datetime, date, time]:
                    v = str(v)
                self.statlist[indexofk] = v


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
        self.type = ChildType.invert(self.type) if self.contract.right == "P" else self.type
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
