from backtrader import Indicator


class TrailingStoploss(Indicator):
    lines = ("bullish", "bearish")

    def __init__(self):
        self.prevlen = None

    def next(self):

        _len = len(self)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if _len < 2:
            return

        phigh, plow, pclose = self.datas[0].high[-1], self.datas[0].low[-1], self.datas[0].close[-1]
        chigh, clow, cclose = self.datas[0].high[0], self.datas[0].low[0], self.datas[0].close[0]

        if (chigh > phigh) \
                and (clow > plow) \
                and (cclose > pclose):
            self.lines.bullish[0] = plow
        else:
            self.lines.bullish[0] = 0

        if (chigh < phigh) \
                and (clow < plow) \
                and (cclose < pclose):
            self.lines.bearish[0] = phigh
        else:
            self.lines.bearish[0] = 0