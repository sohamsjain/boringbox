from collections import OrderedDict


class AutoOrderedDict(OrderedDict):
    _closed = False

    def _close(self):
        self._closed = True
        for key, val in self.items():
            if isinstance(val, AutoOrderedDict):
                val._close()

    def _open(self):
        self._closed = False

    def __missing__(self, key):
        if self._closed:
            raise KeyError

        # value = self[key] = type(self)()
        value = self[key] = AutoOrderedDict()
        return value

    def __getattr__(self, key):
        if key.startswith('_'):
            raise AttributeError

        return self[key]

    def __setattr__(self, key, value):
        if key.startswith('_'):
            self.__dict__[key] = value
            return

        self[key] = value

    # Define math operations
    def __iadd__(self, other):
        if type(self) != type(other):
            return type(other)() + other

        return self + other

    def __isub__(self, other):
        if type(self) != type(other):
            return type(other)() - other

        return self - other

    def __imul__(self, other):
        if type(self) != type(other):
            return type(other)() * other

        return self + other

    def __idiv__(self, other):
        if type(self) != type(other):
            return type(other)() // other

        return self + other

    def __itruediv__(self, other):
        if type(self) != type(other):
            return type(other)() / other

        return self + other


class SecType:
    IND = "IND"
    STK = "STK"
    FUT = "FUT"
    OPT = "OPT"


class XoneType:
    BULLISH = "BULLISH"
    BEARISH = "BEARISH"

    @classmethod
    def identify(cls, entry, stoploss):
        return cls.BULLISH if entry > stoploss else cls.BEARISH


class XoneStatus:
    CREATED = "CREATED"  # CREATED
    ENTRYHIT = "ENTRY HIT"  # CREATED
    ENTRY = "ENTRY"  # OPEN
    STOPLOSSHIT = "STOPLOSS HIT"  # OPEN
    TARGETHIT = "TARGET HIT"  # OPEN
    STOPLOSS = "STOPLOSS"  # CLOSED
    TARGET = "TARGET"  # CLOSED
    MISSED = "MISSED"  # CLOSED
    FAILED = "FAILED"  # CLOSED
    PROFIT = "PROFIT"  # CLOSED
    LOSS = "LOSS"  # CLOSED
    ABORT = "ABORT"  # CLOSING
    FORCECLOSED = "FORCE CLOSED"  # CLOSED
    PENDING = [CREATED, ENTRYHIT]
    OPEN = [ENTRY, STOPLOSSHIT, TARGETHIT]
    CLOSED = [STOPLOSS, TARGET, MISSED, FAILED, PROFIT, LOSS, FORCECLOSED]


class ChildType:
    BUY = "BUY"
    SELL = "SELL"

    @classmethod
    def invert(cls, _type):
        return cls.BUY if _type == cls.SELL else cls.SELL


class ChildStatus:
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
