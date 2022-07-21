from backtrader import Indicator


class BarType(Indicator):
    lines = ("nature", "bullish", "bearish")
    params = ()
    plotinfo = dict(subplot=False)
    plotlines = dict(
        bullish=dict(marker='^', markersize=8.0, color='black', fillstyle='full'),
        bearish=dict(marker='v', markersize=8.0, color='black', fillstyle='full'),
        nature=dict(_plotskip=True),
    )

    def next(self):
        barbody = round(self.data0.close[0] - self.data0.open[0], 2)
        barrange = round(self.data0.high[0] - self.data0.low[0], 2)
        # closeposition = round(self.data0.close[0] - self.data0.low[0], 2) / barrange
        density = abs(barbody) / barrange
        if barbody > 0:  # green

            if density >= 0.5:
                self.lines.nature[0] = 1
                self.lines.bullish[0] = self.data0.low[0]

        elif barbody < 0:  # red

            if density >= 0.5:
                self.lines.nature[0] = -1
                self.lines.bearish[0] = self.data0.high[0]

        else:
            self.lines.nature[0] = 0


class Boring(Indicator):
    lines = ("boring", "isboring", "isbackwardboring", "bodyhigh", "bodylow")
    params = ()
    plotinfo = dict(subplot=False)
    plotlines = dict(
        boring=dict(marker='^', markersize=8.0, color='blue', fillstyle='full'),
    )

    def __init__(self):
        self.prevlen = None

    def next(self):

        _len = len(self)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if self.data0.close[0] > self.data0.open[0]:
            self.lines.bodyhigh[0] = self.data0.close[0]
            self.lines.bodylow[0] = self.data0.open[0]
        elif self.data0.open[0] > self.data0.close[0]:
            self.lines.bodyhigh[0] = self.data0.open[0]
            self.lines.bodylow[0] = self.data0.close[0]
        else:
            self.lines.bodyhigh[0] = self.lines.bodylow[0] = self.data0.open[0]

        barbody = round(
            max(
                abs(self.data0.close[0] - self.data0.open[0]),  # Body
                abs(self.data0.close[0] - self.data0.close[-1])  # Change
            ), 2)

        barrange = round(
            max(
                abs(self.data0.high[0] - self.data0.low[0]),  # Range
                abs(self.data0.close[-1] - self.data0.high[0]),  # True Range
                abs(self.data0.close[-1] - self.data0.low[0])  # True Range
            ), 2)

        if barrange == 0:
            barrange = 1

        density = abs(barbody) / barrange

        if density <= 0.5:
            self.lines.isboring[0] = 1
            self.lines.boring[0] = self.data0.low[0]
        else:
            self.lines.isboring[0] = 0

        barbody = round(
            max(
                abs(self.data0.close[-1] - self.data0.open[-1]),  # Body
                abs(self.data0.open[0] - self.data0.open[-1])  # Change
            ), 2)

        barrange = round(
            max(
                abs(self.data0.high[-1] - self.data0.low[-1]),  # Range
                abs(self.data0.open[0] - self.data0.high[-1]),  # True Range
                abs(self.data0.open[0] - self.data0.low[-1])  # True Range
            ), 2)

        if barrange == 0:
            barrange = 1

        density = abs(barbody) / barrange

        if density <= 0.5:
            self.lines.isbackwardboring[-1] = 1
        else:
            self.lines.isbackwardboring[-1] = 0
