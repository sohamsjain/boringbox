from backtrader import Indicator

SPH = "SPH"
SPL = "SPL"
LPH = "LPH"
LPL = "LPL"
x, h, l, c, r = 'x', 'h', 'l', 'c', 'r'


class Pivots(Indicator):
    lines = ('sph', 'spl', 'bar', 'shift', 'anchor', 'lpl', 'lph')
    params = ()
    plotinfo = dict(subplot=False)
    plotlines = dict(
        sph=dict(marker='v', markersize=8.0, color='black', fillstyle='full'),
        spl=dict(marker='^', markersize=8.0, color='black', fillstyle='full'),
        lph=dict(marker='v', markersize=16.0, color='red', fillstyle='full'),
        lpl=dict(marker='^', markersize=16.0, color='red', fillstyle='full'),
        bar=dict(marker='o', markersize=8.0, color='grey', fillstyle='full'),
        shift=dict(_plotskip=True),
        anchor=dict(_plotskip=True),
    )

    def __init__(self):
        self.new_lph = False
        self.new_lpl = False
        self.new_sph = False
        self.new_spl = False
        self.small_pivot = SPH
        self.large_pivot = LPL
        self.prevlen = None

    def next(self):

        _len = len(self)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if _len == 1:
            self.anchor_index = 0
            self.anchor_high, self.anchor_low, self.anchor_close = self.data0.high[0], self.data0.low[0], \
                                                                   self.data0.close[0]
            self.spl_value, self.sph_value = self.anchor_low, self.anchor_high
            self.spl_index, self.sph_index = 0, 0
            self.lpl_value, self.lph_value = self.anchor_low, self.anchor_high
            self.lpl_index, self.lph_index = 0, 0
            self.lowest_spl, self.highest_sph = self.anchor_low, self.anchor_high
            self.lowest_spl_index, self.highest_sph_index = 0, 0
            self.highest_high = self.anchor_high
            self.highest_high_index = 0
            self.lowest_low = self.anchor_low
            self.lowest_low_index = 0
            self.anchors = [
                {x: self.anchor_index, h: self.anchor_high, l: self.anchor_low, c: self.anchor_close, r: []}
            ]
            return

        self.spl_index -= 1
        self.sph_index -= 1
        self.highest_high_index -= 1
        self.lowest_low_index -= 1
        self.lpl_index -= 1
        self.lph_index -= 1
        self.lowest_spl_index -= 1
        self.highest_sph_index -= 1

        for anchor in self.anchors:
            anchor[x] -= 1

        high = self.data0.high[0]
        low = self.data0.low[0]
        close = self.data0.close[0]

        # New Low
        if low <= self.lowest_low:
            self.lowest_low = low
            self.lowest_low_index = 0

        # New High
        if high >= self.highest_high:
            self.highest_high = high
            self.highest_high_index = 0

        # New Lph
        if self.large_pivot == LPL:

            # Find Spl or lowest point after Spl
            lp, lpx = self.spl_value, self.spl_index
            slice_of_low = self.data0.low.get(ago=-1, size=abs(lpx + 1))
            for sliced_low in slice_of_low:
                if sliced_low < lp:
                    lp = sliced_low

            # Spl break, New Lph formed
            if low < lp:

                # Find Highest Sph or highest point after Highest Sph
                lph, lphx = self.highest_sph, self.highest_sph_index
                slice_of_high = self.data0.high.get(size=abs(lphx))
                dex = lphx + 1
                for sliced_high in slice_of_high:
                    if sliced_high > lph:
                        lph = sliced_high
                        lphx = dex
                    dex += 1

                self.lines.lph[lphx] = lph
                self.lph_value, self.lph_index = lph, lphx
                self.new_lph = True
                self.large_pivot = LPH
                self.lowest_spl = self.spl_value
                self.lowest_spl_index = self.spl_index

        # New Lpl
        elif self.large_pivot == LPH:

            # Find Sph or highest point after Sph
            hp, hpx = self.sph_value, self.sph_index
            slice_of_high = self.data0.high.get(ago=-1, size=abs(hpx + 1))
            for sliced_high in slice_of_high:
                if sliced_high > hp:
                    hp = sliced_high

            # Sph break, New Lpl formed
            if high > hp:

                # Find Lowest Spl or lowest point after Lowest Spl
                lpl, lplx = self.lowest_spl, self.lowest_spl_index
                slice_of_low = self.data0.low.get(size=abs(lplx))
                dex = lplx + 1
                for sliced_low in slice_of_low:
                    if sliced_low < lpl:
                        lpl = sliced_low
                        lplx = dex
                    dex += 1

                self.lines.lpl[lplx] = lpl
                self.lpl_value, self.lpl_index = lpl, lplx
                self.new_lpl = True
                self.large_pivot = LPL
                self.highest_sph = self.sph_value
                self.highest_sph_index = self.sph_index

        # New Spl
        if self.small_pivot == SPH:  # Down Trend

            addbartoanchors = True
            for ach in self.anchors:
                ach_index = ach[x]
                ach_high = ach[h]
                ach_low = ach[l]
                ach_close = ach[c]
                ach_ref = ach[r]

                if high > ach_high and close > ach_close:
                    addbartoanchors = False
                    ach_ref.append({x: 0, h: high, l: low, c: close})
                    if len(ach_ref) == 2:
                        self.lines.bar[0] = float(low)
                        self.lines.spl[self.lowest_low_index] = float(self.lowest_low)
                        self.spl_value, self.spl_index = self.lowest_low, self.lowest_low_index
                        self.new_spl = True

                        if self.spl_value < self.lowest_spl:
                            self.lowest_spl = self.spl_value
                            self.lowest_spl_index = self.spl_index

                        # for j in range(self.sph_index + 1, self.spl_index):
                        #     jh = self.df.high.values[j]
                        #     if jh > self.sph_value:
                        #         self.df['shift'].at[j] = jh

                        self.lines.anchor[ach_index] = ach_low
                        self.small_pivot = SPL
                        self.anchors.clear()
                        self.anchors.append({x: 0, h: high, l: low, c: close, r: []})
                        self.highest_high = self.data0.high[self.lowest_low_index]
                        self.highest_high_index = self.lowest_low_index

                        slice_of_high = self.data0.high.get(size=abs(self.lowest_low_index) + 1)
                        dex = self.lowest_low_index
                        for sliced_high in slice_of_high:
                            if sliced_high > self.highest_high:
                                self.highest_high = sliced_high
                                self.highest_high_index = dex
                            dex += 1
                        break
            if addbartoanchors:
                self.anchors.append({x: 0, h: high, l: low, c: close, r: []})

        # New Sph
        elif self.small_pivot == SPL:  # Up Trend

            addbartoanchors = True
            for ach in self.anchors:
                ach_index = ach[x]
                ach_high = ach[h]
                ach_low = ach[l]
                ach_close = ach[c]
                ach_ref = ach[r]

                if low < ach_low and close < ach_close:
                    addbartoanchors = False
                    ach_ref.append({x: 0, h: high, l: low, c: close})
                    if len(ach_ref) == 2:
                        self.lines.bar[0] = float(high)
                        self.lines.sph[self.highest_high_index] = float(self.highest_high)
                        self.sph_value, self.sph_index = self.highest_high, self.highest_high_index
                        self.new_sph = True

                        if self.sph_value > self.highest_sph:
                            self.highest_sph = self.sph_value
                            self.highest_sph_index = self.sph_index

                        # for j in range(self.spl_index + 1, self.sph_index):
                        #     jl = self.df.low.values[j]
                        #     if jl < self.spl_value:
                        #         self.df['shift'].at[j] = jl

                        self.lines.anchor[ach_index] = float(ach_high)
                        self.small_pivot = SPH
                        self.anchors.clear()
                        self.anchors.append({x: 0, h: high, l: low, c: close, r: []})
                        self.lowest_low = self.data0.low[self.highest_high_index]
                        self.lowest_low_index = self.highest_high_index

                        slice_of_low = self.data0.low.get(size=abs(self.highest_high_index) + 1)
                        dex = self.highest_high_index
                        for sliced_low in slice_of_low:
                            if sliced_low < self.lowest_low:
                                self.lowest_low = sliced_low
                                self.lowest_low_index = dex
                            dex += 1
                        break
            if addbartoanchors:
                self.anchors.append({x: 0, h: high, l: low, c: close, r: []})
