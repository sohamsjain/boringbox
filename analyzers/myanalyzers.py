import pandas as pd
from backtrader import Analyzer
from backtrader.analyzers import TradeAnalyzer, DrawDown, SQN, SharpeRatio, SharpeRatio_A
from backtrader.utils import AutoOrderedDict, AutoDict


woncols = ['won', 'freq']
lostcols = ['lost', 'freq']


class PivotAnalyzer(Analyzer):

    def __init__(self):
        self.tradeanalyzer = TradeAnalyzer()
        self.drawdown = DrawDown()
        self.sqn = SQN()
        self.sharpe = SharpeRatio()
        self.sharpeA = SharpeRatio_A()
        self.streakpro = StreakProfile()
        self.sheet = self.strategy.spread.worksheet("Stats")

    def create_analysis(self):
        self.rets = AutoOrderedDict()

    def stop(self):
        super(PivotAnalyzer, self).stop()
        self.rets.starting_value = self.strategy.broker.startingcash
        self.rets.value = self.strategy.broker.get_value()
        self.rets.max_value = self.drawdown._maxvalue
        self.rets.tarets = self.tradeanalyzer.rets
        self.rets.ddrets = self.drawdown.rets
        self.rets.sqn = self.sqn.rets.sqn
        trades = self.rets.tarets.total.total
        won = self.rets.tarets.won.total
        lost = self.rets.tarets.lost.total
        winrate = won / trades
        lossrate = lost / trades
        maxwon = self.rets.tarets.won.pnl.max
        averagewon = self.rets.tarets.won.pnl.average
        totalwon = self.rets.tarets.won.pnl.total
        maxlost = self.rets.tarets.lost.pnl.max
        averagelost = self.rets.tarets.lost.pnl.average
        totallost = self.rets.tarets.lost.pnl.total
        self.rets.expectancy = winrate * abs(averagewon / averagelost) - lossrate
        stats = [
            ("Starting Cash", self.rets.starting_value),
            ("Cash", self.rets.value),
            ("Max Cash", self.rets.max_value),
            ("Max Moneydown", self.rets.ddrets.max.moneydown),
            ("Max Drawdown", self.rets.ddrets.max.drawdown),
            ("Winning Streak", self.rets.tarets.streak.won.longest),
            ("Losing Streak", self.rets.tarets.streak.lost.longest),
            ("Total Trades", trades),
            ("Trades Won", won),
            ("Trades Lost", lost),
            ("Win Rate", winrate),
            ("Loss Rate", lossrate),
            ("Max Won", maxwon),
            ("Average Won", averagewon),
            ("Total Won", totalwon),
            ("Max Lost", maxlost),
            ("Average Lost", averagelost),
            ("Total Lost", totallost),
            ("Expectancy", self.rets.expectancy),
            ("SQN", self.rets.sqn),
            ("Sharpe", self.sharpe.rets['sharperatio']),
            ("Sharpe Annual", self.sharpeA.rets['sharperatio']),
        ]
        statsdf = pd.DataFrame(stats, columns=["Keys", "Values"])
        print(statsdf)
        won = pd.DataFrame([[k, v] for k, v in self.streakpro.rets.won.items() if type(k) == int], columns=woncols)
        lost = pd.DataFrame([[k, v] for k, v in self.streakpro.rets.lost.items() if type(k) == int], columns=lostcols)
        import numpy as np
        statsdf.replace({np.nan: None}, inplace=True)
        self.sheet.update([statsdf.columns.values.tolist()] + statsdf.values.tolist())
        # won.to_excel(w, index=False, sheet_name="Stats", startcol=3)
        # lost.to_excel(w, index=False, sheet_name="Stats", startcol=5)


class StreakProfile(Analyzer):

    def create_analysis(self):
        self.rets = AutoOrderedDict()
        self.rets.won = AutoOrderedDict()
        self.rets.lost = AutoOrderedDict()

    def stop(self):
        super(StreakProfile, self).stop()
        self.rets._close()

    def notify_trade(self, trade):

        if trade.status == trade.Closed:
            trades = self.rets

            res = AutoDict()
            # Trade just closed

            won = res.won = int(trade.pnlcomm >= 0.0)
            lost = res.lost = int(not won)

            # Streak
            for wlname in ['won', 'lost']:
                wl = res[wlname]
                prev = trades.streak[wlname].current

                trades.streak[wlname].current *= wl
                trades.streak[wlname].current += wl

                if type(prev) == int:
                    if prev > 0 and trades.streak[wlname].current == 0:
                        self.rets[wlname][prev] += 1
