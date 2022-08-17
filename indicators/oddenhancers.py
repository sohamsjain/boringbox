from __future__ import annotations

from datetime import datetime
from queue import Queue
from typing import List, Optional, Callable

import math
from backtrader.indicators import AverageTrueRange

from indicators.boring import *
from indicators.pivots import *

trends = ["Side Trend", "Up Trend", "Down Trend"]


class Notifications:
    NotificationEnds = -1
    SavedStateRestored = 0
    DemandZoneCreated = 1
    DemandZoneModified = 2
    DemandZoneBroken = 3
    SupplyZoneCreated = 4
    SupplyZoneModified = 5
    SupplyZoneBroken = 6


class Demand:

    def __init__(self, index, sl, dtsl, valid=True):
        self.index = index
        self.sl = sl
        self.dtsl: datetime = dtsl
        self.growing = True
        self.power = 0
        self.zonedout = False
        self.displacement = 0
        self.time = None
        self.speed = None
        self.atrspeed = None
        self.slope = None
        self.potency = None
        self.breakoutpoint = None
        self.entry = None
        self.dtentry: Optional[datetime] = None
        self.influence = None
        self.dtinfluence: Optional[datetime] = None
        self.timeatbase = None
        self.testopen = False
        self.testcount = 0
        self.testperc = 0
        self.testlow = float("inf")
        self.dzone: Optional[DZone] = None
        self.postbreakoutjourney = None
        self.prebreakoutjourney = None
        self.valid = valid

    def calculate_potency(self, high, atr):
        displacement = round(abs(self.sl - high), 2)
        if displacement > self.displacement:
            self.displacement = displacement
            self.time = abs(self.index) + 1
            self.speed = self.displacement / self.time
            self.atrspeed = self.speed / atr
            self.slope = math.degrees(math.atan(self.atrspeed))
            self.potency = self.slope * math.sqrt(self.time)
            if self.dzone:
                self.dzone.set_strength(self.potency)
                if self.entry:
                    valid = self.displacement / abs(self.entry - self.sl) >= 2
                    self.dzone.valid = valid

    def set_dzone(self, dzone):
        self.dzone = dzone
        self.dzone.set_strength(self.potency)
        self.dzone.set_timeatbase(self.timeatbase)
        self.dzone.set_test(self.testcount, self.testopen, self.testperc)

    def reindex(self):
        self.index -= 1


class Supply:

    def __init__(self, index, sl, dtsl, valid=True):
        self.index = index
        self.sl = sl
        self.dtsl: datetime = dtsl
        self.growing = True
        self.power = 0
        self.zonedout = False
        self.displacement = 0
        self.time = None
        self.speed = None
        self.atrspeed = None
        self.slope = None
        self.potency = None
        self.breakoutpoint = None
        self.entry = None
        self.dtentry: Optional[datetime] = None
        self.influence = None
        self.dtinfluence: Optional[datetime] = None
        self.timeatbase = None
        self.testopen = False
        self.testcount = 0
        self.testperc = 0
        self.testhigh = 0
        self.szone: Optional[SZone] = None
        self.postbreakoutjourney = None
        self.prebreakoutjourney = None
        self.valid = valid

    def calculate_potency(self, low, atr):
        displacement = round(abs(self.sl - low), 2)
        if displacement > self.displacement:
            self.displacement = displacement
            self.time = abs(self.index) + 1
            self.speed = self.displacement / self.time
            self.atrspeed = self.speed / atr
            self.slope = math.degrees(math.atan(self.atrspeed))
            self.potency = self.slope * math.sqrt(self.time)
            if self.szone:
                self.szone.set_strength(self.potency)
                if self.entry:
                    valid = self.displacement / abs(self.entry - self.sl) >= 2
                    self.szone.valid = valid

    def set_szone(self, szone):
        self.szone = szone
        self.szone.set_strength(self.potency)
        self.szone.set_timeatbase(self.timeatbase)
        self.szone.set_test(self.testcount, self.testopen, self.testperc)

    def reindex(self):
        self.index -= 1


class DZone:
    def __init__(self, demand: Demand, dsi: DSIndicator, created_at):
        self.dsi = dsi
        self.sl = demand.sl
        self.dtsl = demand.dtsl
        self.entry = demand.entry
        self.dtentry = demand.dtentry
        self.risk = self.entry - self.sl
        self.target = None
        self.dttarget = None
        self.reward = None
        self.ratio = None
        self.influence = demand.influence
        self.dtinfluence = demand.dtinfluence
        self.strength = round(demand.potency, 2)
        self.timeatbase = demand.timeatbase
        self.created_at = created_at
        self.modified_at = list()
        self.mergers = 0
        self.testopen = False
        self.testcount = 0
        self.testperc = 0
        self.trend = 0
        self.location = 0
        self.score = 0
        self.relocate_curve()
        self.set_trend()
        self.atrwidth = self.calculate_atrwidth()
        self.valid = False
        self.demands: List[Demand] = list()
        self.demands.insert(0, demand)

    def calculate_ratio(self):
        if self.reward:
            self.ratio = round(self.reward / self.risk, 2)
            self.calculate_score()
            return self.ratio
        return None

    def calculate_score(self):
        if not self.ratio:
            return

        score = 0

        if self.ratio > 5:
            score += 3
        elif self.ratio > 3:
            score += 1.5

        if self.timeatbase < 4:
            score += 2
        elif self.timeatbase < 7:
            score += 1

        if self.strength > 100:
            score += 2
        elif self.strength > 80:
            score += 1

        if self.testcount == 0:
            score += 3
        elif self.testcount == 1:
            score += 1.5

        score += self.trend + self.location

        self.score = score

    def set_target(self, target, dttarget):
        self.target = target
        self.dttarget = dttarget
        self.reward = self.target - self.entry
        self.calculate_ratio()

    def set_entry(self, entry, dtentry):
        self.entry = entry
        self.dtentry = dtentry
        self.risk = self.entry - self.sl
        self.relocate_curve()
        if self.target:
            self.reward = self.target - self.entry
            self.calculate_ratio()

    def set_sl(self, sl, dtsl):
        self.sl = sl
        self.dtsl = dtsl
        self.risk = self.entry - self.sl
        self.calculate_ratio()

    def set_influence(self, influence, dtinfluence):
        self.influence = influence
        self.dtinfluence = dtinfluence

    def set_strength(self, strength):
        self.strength = round(strength, 2)
        self.calculate_score()

    def set_timeatbase(self, timeatbase):
        self.timeatbase = timeatbase
        self.calculate_score()

    def set_test(self, count, _open, perc):
        self.testcount = count
        self.testopen = _open
        self.testperc = perc
        self.calculate_score()

    def set_trend(self):
        if self.dsi.trend.trend == 1:
            self.trend = 1
        elif self.dsi.trend.trend == 0:
            self.trend = 0.5
        elif self.dsi.trend.trend == -1:
            self.trend = 0
        self.calculate_score()

    def relocate_curve(self):
        if self.entry >= self.dsi.curve.highoncurve:
            self.location = 0
        elif self.entry >= self.dsi.curve.lowoncurve:
            self.location = 1
        else:
            self.location = 2
        self.calculate_score()

    def extend_past_powerless_demand(self, demand: Demand, atr):
        difference = self.sl - demand.sl
        if 0 <= difference < atr:
            self.set_sl(demand.sl, demand.dtsl)
            self.demands.append(demand)

    def merge(self, dzone: DZone, atr):
        difference = abs(self.sl - dzone.sl)
        underinfluence = dzone.sl <= self.influence
        if underinfluence or difference <= atr:
            if dzone.entry > self.entry:
                self.set_entry(dzone.entry, dzone.dtentry)
            if dzone.influence > self.influence:
                self.set_influence(dzone.influence, dzone.dtinfluence)
            if dzone.sl < self.sl:  # In rare cases like dual breakouts in one candle HINDUNILV 2013-04-29
                self.set_sl(dzone.sl, dzone.dtsl)
            self.mergers += 1
            self.modified_at.append(self.dsi.lasttimestamp)
            self.demands = dzone.demands + self.demands
            return True
        return False

    def demerge(self, count):
        self.mergers -= count
        self.modified_at.append(self.dsi.lasttimestamp)

        sl = float("inf")
        entry = influence = 0
        dtentry, dtinfluence, dtsl = None, None, None

        for demand in self.demands:

            if demand.power:

                if demand.entry > entry:
                    entry = demand.entry
                    dtentry = demand.dtentry

                if demand.influence > influence:
                    influence = demand.influence
                    dtinfluence = demand.dtinfluence

            if demand.sl < sl:
                sl = demand.sl
                dtsl = demand.dtsl

        assert entry != 0 and sl != float("inf")
        self.set_entry(entry, dtentry)
        self.set_influence(influence, dtinfluence)
        self.set_sl(sl, dtsl)

        self.demands[0].set_dzone(self)

    def calculate_atrwidth(self):
        self.atrwidth = round(self.risk / self.dsi.atr[0])
        return self.atrwidth


class SZone:
    def __init__(self, supply: Supply, dsi: DSIndicator, created_at):
        self.dsi = dsi
        self.sl = supply.sl
        self.dtsl = supply.dtsl
        self.entry = supply.entry
        self.dtentry = supply.dtentry
        self.risk = self.sl - self.entry
        self.target = None
        self.dttarget = None
        self.reward = None
        self.ratio = None
        self.influence = supply.influence
        self.dtinfluence = supply.dtinfluence
        self.strength = round(supply.potency, 2)
        self.timeatbase = supply.timeatbase
        self.created_at = created_at
        self.modified_at = list()
        self.mergers = 0
        self.testopen = False
        self.testcount = 0
        self.testperc = 0
        self.trend = 0
        self.location = 0
        self.score = 0
        self.relocate_curve()
        self.set_trend()
        self.atrwidth = self.calculate_atrwidth()
        self.valid = False
        self.supplies: List[Supply] = list()
        self.supplies.insert(0, supply)

    def calculate_ratio(self):
        if self.reward:
            self.ratio = round(self.reward / self.risk, 2)
            self.calculate_score()
            return self.ratio
        return None

    def calculate_score(self):
        if not self.ratio:
            return

        score = 0

        if self.ratio > 5:
            score += 3
        elif self.ratio > 3:
            score += 1.5

        if self.timeatbase < 4:
            score += 2
        elif self.timeatbase < 7:
            score += 1

        if self.strength > 100:
            score += 2
        elif self.strength > 80:
            score += 1

        if self.testcount == 0:
            score += 3
        elif self.testcount == 1:
            score += 1.5

        score += self.trend + self.location

        self.score = score

    def set_target(self, target, dttarget):
        self.target = target
        self.dttarget = dttarget
        self.reward = self.entry - self.target
        self.calculate_ratio()

    def set_entry(self, entry, dtentry):
        self.entry = entry
        self.dtentry = dtentry
        self.risk = self.sl - self.entry
        self.relocate_curve()
        if self.target:
            self.reward = self.entry - self.target
            self.calculate_ratio()

    def set_sl(self, sl, dtsl):
        self.sl = sl
        self.dtsl = dtsl
        self.risk = self.sl - self.entry
        self.calculate_ratio()

    def set_influence(self, influence, dtinfluence):
        self.influence = influence
        self.dtinfluence = dtinfluence

    def set_strength(self, strength):
        self.strength = round(strength, 2)
        self.calculate_score()

    def set_timeatbase(self, timeatbase):
        self.timeatbase = timeatbase
        self.calculate_score()

    def set_test(self, count, _open, perc):
        self.testcount = count
        self.testopen = _open
        self.testperc = perc
        self.calculate_score()

    def set_trend(self):
        if self.dsi.trend.trend == -1:
            self.trend = 1
        elif self.dsi.trend.trend == 0:
            self.trend = 0.5
        elif self.dsi.trend.trend == 1:
            self.trend = 0
        self.calculate_score()

    def relocate_curve(self):
        if self.entry <= self.dsi.curve.lowoncurve:
            self.location = 0
        elif self.entry <= self.dsi.curve.highoncurve:
            self.location = 1
        else:
            self.location = 2
        self.calculate_score()

    def extend_past_powerless_supply(self, supply: Supply, atr):
        difference = supply.sl - self.sl
        if 0 <= difference < atr:
            self.set_sl(supply.sl, supply.dtsl)
            self.supplies.append(supply)

    def merge(self, szone: SZone, atr):
        difference = abs(self.sl - szone.sl)
        underinfluence = szone.sl >= self.influence
        if underinfluence or difference <= atr:
            if szone.entry < self.entry:
                self.set_entry(szone.entry, szone.dtentry)
            if szone.influence < self.influence:
                self.set_influence(szone.influence, szone.dtinfluence)
            if szone.sl > self.sl:  # In rare cases like dual breakouts in one candle HINDUNILV 2013-04-29
                self.set_sl(szone.sl, szone.dtsl)
            self.mergers += 1
            self.modified_at.append(self.dsi.lasttimestamp)
            self.supplies = szone.supplies + self.supplies
            return True
        return False

    def demerge(self, count):
        self.mergers -= count
        self.modified_at.append(self.dsi.lasttimestamp)

        sl = 0
        entry = influence = float("inf")
        dtentry, dtinfluence, dtsl = None, None, None

        for supply in self.supplies:

            if supply.power:

                if supply.entry < entry:
                    entry = supply.entry
                    dtentry = supply.dtentry

                if supply.influence < influence:
                    influence = supply.influence
                    dtinfluence = supply.dtinfluence

            if supply.sl > sl:
                sl = supply.sl
                dtsl = supply.dtsl

        assert entry != float("inf") and sl != 0
        self.set_entry(entry, dtentry)
        self.set_influence(influence, dtinfluence)
        self.set_sl(sl, dtsl)

        self.supplies[0].set_szone(self)

    def calculate_atrwidth(self):
        self.atrwidth = round(self.risk / self.dsi.atr[0])
        return self.atrwidth


class DSIndicator(Indicator):
    lines = ('supply', 'demand')
    params = (('savedstate', None), ('curvebreakoutsonly', False))

    def __init__(self):
        self.tickdatano = 0
        if len(self.datas) == 4:
            self.dsidatano = 1
            self.trenddatano = 2
            self.curvedatano = 3
        elif len(self.datas) == 3:
            self.dsidatano = 0
            self.trenddatano = 1
            self.curvedatano = 2

        self.pivots = Pivots(self.datas[self.dsidatano])
        self.boring = Boring(self.datas[self.dsidatano])
        self.atr = AverageTrueRange(self.datas[self.dsidatano])
        self.trend = Trend(self.datas[self.trenddatano], callback=self.trendcallback)
        self.curve = Curve(self.datas[self.curvedatano], callback=self.curvecallback,
                           breakoutsonly=self.p.curvebreakoutsonly)
        self.supplies: List[Supply] = list()
        self.demands: List[Demand] = list()
        self.demandzones: List[DZone] = list()
        self.supplyzones: List[SZone] = list()
        self.prevlen = None
        self.onsphcreated = self.trialcall_sph
        self.onsplcreated = self.trialcall_spl
        self.small_pivot = self.pivots.small_pivot
        self.lasttimestamp = None
        self.notifications = Queue()

    def next(self):

        _len = len(self.datas[self.dsidatano])
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if _len <= 1:
            return

        self.lasttimestamp = self.datas[self.dsidatano].datetime.datetime(0)

        for each in [self.demands, self.supplies]:
            for item in each:
                item.reindex()

        open_ = self.datas[self.dsidatano].open[0]
        high = self.datas[self.dsidatano].high[0]
        low = self.datas[self.dsidatano].low[0]
        close = self.datas[self.dsidatano].close[0]
        phigh = self.datas[self.dsidatano].high[-1]
        plow = self.datas[self.dsidatano].low[-1]

        self.checktests(open_, high, low, close)

        if high > phigh:
            self.higherhigh(high)
        if low < plow:
            self.lowerlow(low)

        if self.pivots.new_sph:
            self.pivots.new_sph = False
            self.onsphcreated()

        elif self.pivots.new_spl:
            self.pivots.new_spl = False
            self.onsplcreated()

        for each in [self.demandzones, self.supplyzones]:
            for item in each:
                item.calculate_atrwidth()

        if self.p.savedstate and self.p.savedstate["lasttimestamp"] == self.lasttimestamp:
            self.rebase(self.p.savedstate)
            self.p.savedstate = None
            self.notify(Notifications.SavedStateRestored)

    def notify(self, notification, body=None):
        self.notifications.put(notification)
        if body:
            self.notifications.put(body)
        self.notifications.put(Notifications.NotificationEnds)

    def trialcall_sph(self):
        self.onsphcreated = self.sphcreated

    def trialcall_spl(self):
        self.onsplcreated = self.splcreated

    def higherhigh(self, high):
        while len(self.supplyzones):
            szone = self.supplyzones[0]
            if high > szone.sl:
                self.supplyzones.remove(szone)
                self.supplyzonebroken(szone)
            else:
                break

        while len(self.supplies):
            supply = self.supplies[0]
            if high > supply.sl:
                self.supplies.remove(supply)
                self.supplybroken(supply)
            else:
                break

        if len(self.supplyzones):
            szone = self.supplyzones[0]
            demerge_count = 0

            while szone.mergers - demerge_count:

                powerfullsupplies = [supply for supply in szone.supplies if supply.power > 0]
                assert len(powerfullsupplies) >= 2
                pfsupply2 = powerfullsupplies[1]
                pfsupply2index = szone.supplies.index(pfsupply2)
                lot1 = szone.supplies[:pfsupply2index]
                maxsl = max([supply.sl for supply in lot1])

                if high > maxsl:
                    for supply in lot1:
                        szone.supplies.remove(supply)
                    demerge_count += 1
                else:
                    break

            if demerge_count:
                szone.demerge(demerge_count)
                self.supplyzonemodified(szone)

        for demand in self.demands:
            if demand.growing:
                demand.calculate_potency(high=high, atr=self.atr[0])
            else:
                break

    def lowerlow(self, low):

        while len(self.demandzones):
            dzone = self.demandzones[0]
            if low < dzone.sl:
                self.demandzones.remove(dzone)
                self.demandzonebroken(dzone)
            else:
                break

        while len(self.demands):
            demand = self.demands[0]
            if low < demand.sl:
                self.demands.remove(demand)
                self.demandbroken(demand)
            else:
                break

        if len(self.demandzones):
            dzone = self.demandzones[0]
            demerge_count = 0

            while dzone.mergers - demerge_count:

                powerfulldemands = [demand for demand in dzone.demands if demand.power > 0]
                assert len(powerfulldemands) >= 2
                pfdemand2 = powerfulldemands[1]
                pfdemand2index = dzone.demands.index(pfdemand2)
                lot1 = dzone.demands[:pfdemand2index]
                minsl = min([demand.sl for demand in lot1])

                if low < minsl:
                    for demand in lot1:
                        dzone.demands.remove(demand)
                    demerge_count += 1
                else:
                    break

            if demerge_count:
                dzone.demerge(demerge_count)
                self.demandzonemodified(dzone)

        for supply in self.supplies:
            if supply.growing:
                supply.calculate_potency(low=low, atr=self.atr[0])
            else:
                break

    def splcreated(self):
        self.small_pivot = SPL

        for supply in self.supplies:
            if not supply.growing:
                break
            supply.growing = False

        latest_demand_index = self.demands[0].index if self.demands else 1
        if self.pivots.spl_index != latest_demand_index:
            demand = Demand(index=self.pivots.spl_index, sl=self.pivots.spl_value,
                            dtsl=self.datas[self.dsidatano].datetime.datetime(self.pivots.spl_index))
            demand.calculate_potency(high=self.pivots.highest_high, atr=self.atr[0])
            self.demands.insert(0, demand)
        else:
            self.demands[0].valid = True

    def sphcreated(self):
        self.small_pivot = SPH

        for demand in self.demands:
            if not demand.growing:
                break
            demand.growing = False

        latest_supply_index = self.supplies[0].index if self.supplies else 1
        if self.pivots.sph_index != latest_supply_index:
            supply = Supply(index=self.pivots.sph_index, sl=self.pivots.sph_value,
                            dtsl=self.datas[self.dsidatano].datetime.datetime(self.pivots.sph_index))
            supply.calculate_potency(low=self.pivots.lowest_low, atr=self.atr[0])
            self.supplies.insert(0, supply)
        else:
            self.supplies[0].valid = True

    def demandbroken(self, demand: Demand):

        if self.small_pivot == SPL:

            latest_supply_index = self.supplies[0].index if self.supplies else 1
            if self.pivots.highest_high_index != latest_supply_index:
                supply = Supply(index=self.pivots.highest_high_index, sl=self.pivots.highest_high,
                                dtsl=self.datas[self.dsidatano].datetime.datetime(self.pivots.highest_high_index),
                                valid=False)
                supply.calculate_potency(low=self.pivots.lowest_low, atr=self.atr[0])
                self.supplies.insert(0, supply)

        if not self.supplies:
            return

        if not demand.valid:
            return

        latest_supply = self.supplies[0]
        old_power = latest_supply.power
        new_power = old_power + 1
        latest_supply.power = new_power

        if new_power == 1:

            boringbodylows = list()
            boringlows = list()
            boringdts = list()
            timeatbase = 1

            if self.boring.isboring[latest_supply.index]:

                i = latest_supply.index - 1
                stop_index = demand.index

                while i >= stop_index:
                    if not self.boring.isbackwardboring[i]:
                        break
                    bodylow = self.boring.bodylow[i]
                    boringbodylows.append(bodylow)
                    boringlows.append(self.datas[self.dsidatano].low[i])
                    boringdts.append(self.datas[self.dsidatano].datetime.datetime(i))
                    timeatbase += 1
                    i -= 1

            i = latest_supply.index + 1
            stop_index = 0

            while i <= stop_index:
                if not self.boring.isboring[i]:
                    break
                bodylow = self.boring.bodylow[i]
                boringbodylows.append(bodylow)
                boringlows.append(self.datas[self.dsidatano].low[i])
                boringdts.append(self.datas[self.dsidatano].datetime.datetime(i))
                timeatbase += 1
                i += 1

            if boringbodylows:
                latest_supply.entry = min(boringbodylows)
                latest_supply.dtentry = boringdts[boringbodylows.index(latest_supply.entry)]
                latest_supply.influence = min(boringlows)
                latest_supply.dtinfluence = boringdts[boringlows.index(latest_supply.influence)]
                latest_supply.timeatbase = timeatbase

            else:
                if self.boring.bodylow[latest_supply.index] > demand.sl:
                    latest_supply.entry = self.boring.bodylow[latest_supply.index]
                    latest_supply.dtentry = self.datas[self.dsidatano].datetime.datetime(latest_supply.index)
                else:
                    latest_supply.entry = demand.sl
                    latest_supply.dtentry = demand.dtsl

                if self.datas[self.dsidatano].low[latest_supply.index] > demand.sl:
                    latest_supply.influence = self.datas[self.dsidatano].low[latest_supply.index]
                    latest_supply.dtinfluence = self.datas[self.dsidatano].datetime.datetime(latest_supply.index)
                else:
                    latest_supply.influence = demand.sl
                    latest_supply.dtinfluence = demand.dtsl

                latest_supply.timeatbase = 1

            if latest_supply.entry == latest_supply.sl:  # In Rare cases eg: one candle supply has o=c=h
                latest_supply.entry = demand.sl
                latest_supply.dtentry = demand.dtsl
                latest_supply.influence = demand.sl
                latest_supply.dtinfluence = demand.dtsl
                latest_supply.timeatbase = abs(latest_supply.index) + 1

            szone = SZone(supply=latest_supply, dsi=self, created_at=self.lasttimestamp)
            latest_supply.zonedout = True

            if self.demandzones:
                closestdz = self.demandzones[0]
                target, dttarget = closestdz.influence, closestdz.dtinfluence
            else:
                target, dttarget = 0, None
            szone.set_target(target, dttarget)

            # check for past powerless supplies
            for supply in self.supplies[1:]:
                if supply.zonedout:
                    break
                szone.extend_past_powerless_supply(supply=supply, atr=self.atr[0])
                supply.zonedout = True

            createzone = True
            if self.supplyzones:
                previous_zone = self.supplyzones[0]
                merged = previous_zone.merge(szone=szone, atr=self.atr[0])
                createzone = not merged
                if merged:
                    self.supplyzonemodified(previous_zone)

            if createzone:
                self.supplyzones.insert(0, szone)
                self.supplyzonecreated(szone)

            self.supplyzones[0].supplies[0].set_szone(self.supplyzones[0])

    def supplybroken(self, supply: Supply):

        if self.small_pivot == SPH:

            latest_demand_index = self.demands[0].index if self.demands else 1
            if self.pivots.lowest_low_index != latest_demand_index:
                demand = Demand(index=self.pivots.lowest_low_index, sl=self.pivots.lowest_low,
                                dtsl=self.datas[self.dsidatano].datetime.datetime(self.pivots.lowest_low_index),
                                valid=False)
                demand.calculate_potency(high=self.pivots.highest_high, atr=self.atr[0])
                self.demands.insert(0, demand)

        if not self.demands:
            return

        if not supply.valid:
            return

        latest_demand = self.demands[0]
        old_power = latest_demand.power
        new_power = old_power + 1
        latest_demand.power = new_power

        if new_power == 1:

            boringbodyhighs = list()
            boringhighs = list()
            boringdts = list()
            timeatbase = 1

            if self.boring.isboring[latest_demand.index]:

                i = latest_demand.index - 1
                stop_index = supply.index

                while i >= stop_index:
                    if not self.boring.isbackwardboring[i]:
                        break
                    bodyhigh = self.boring.bodyhigh[i]
                    boringbodyhighs.append(bodyhigh)
                    boringhighs.append(self.datas[self.dsidatano].high[i])
                    boringdts.append(self.datas[self.dsidatano].datetime.datetime(i))
                    timeatbase += 1
                    i -= 1

            i = latest_demand.index + 1
            stop_index = 0

            while i <= stop_index:
                if not self.boring.isboring[i]:
                    break
                bodyhigh = self.boring.bodyhigh[i]
                boringbodyhighs.append(bodyhigh)
                boringhighs.append(self.datas[self.dsidatano].high[i])
                boringdts.append(self.datas[self.dsidatano].datetime.datetime(i))
                timeatbase += 1
                i += 1

            if boringbodyhighs:
                latest_demand.entry = max(boringbodyhighs)
                latest_demand.dtentry = boringdts[boringbodyhighs.index(latest_demand.entry)]
                latest_demand.influence = max(boringhighs)
                latest_demand.dtinfluence = boringdts[boringhighs.index(latest_demand.influence)]
                latest_demand.timeatbase = timeatbase

            else:
                if self.boring.bodyhigh[latest_demand.index] < supply.sl:
                    latest_demand.entry = self.boring.bodyhigh[latest_demand.index]
                    latest_demand.dtentry = self.datas[self.dsidatano].datetime.datetime(latest_demand.index)
                else:
                    latest_demand.entry = supply.sl
                    latest_demand.dtentry = supply.dtsl

                if self.datas[self.dsidatano].high[latest_demand.index] < supply.sl:
                    latest_demand.influence = self.datas[self.dsidatano].high[latest_demand.index]
                    latest_demand.dtinfluence = self.datas[self.dsidatano].datetime.datetime(latest_demand.index)
                else:
                    latest_demand.influence = supply.sl
                    latest_demand.dtinfluence = supply.dtsl

                latest_demand.timeatbase = 1

            if latest_demand.entry == latest_demand.sl:  # In Rare cases eg: one candle demand has o=c=l
                latest_demand.entry = supply.sl
                latest_demand.dtentry = supply.dtsl
                latest_demand.influence = supply.sl
                latest_demand.dtinfluence = supply.dtsl
                latest_demand.timeatbase = abs(latest_demand.index) + 1

            dzone = DZone(demand=latest_demand, dsi=self, created_at=self.lasttimestamp)
            latest_demand.zonedout = True
            if self.supplyzones:
                closestsz = self.supplyzones[0]
                target, dttarget = closestsz.influence, closestsz.dtinfluence
            else:
                target, dttarget = float("inf"), None
            dzone.set_target(target, dttarget)

            # check for past powerless demands
            for demand in self.demands[1:]:
                if demand.zonedout:
                    break
                dzone.extend_past_powerless_demand(demand=demand, atr=self.atr[0])
                demand.zonedout = True

            createzone = True
            if self.demandzones:
                previous_zone = self.demandzones[0]
                merged = previous_zone.merge(dzone=dzone, atr=self.atr[0])
                createzone = not merged
                if merged:
                    self.demandzonemodified(previous_zone)

            if createzone:
                self.demandzones.insert(0, dzone)
                self.demandzonecreated(dzone)

            self.demandzones[0].demands[0].set_dzone(self.demandzones[0])

    def checktests(self, open_, high, low, close):

        if len(self.supplyzones):
            szone = self.supplyzones[0]
            supplies = szone.supplies
            i = 0

            for supply in supplies:

                i += 1
                if not supply.power:
                    continue

                notify = False
                if supply.testopen:
                    if open_ < supply.influence:
                        supply.testopen = False
                        supply.testcount += 1
                        notify = True

                if not supply.testopen:
                    if high > supply.influence:
                        supply.testopen = True
                        notify = True

                if supply.testopen:
                    if close < supply.influence:
                        supply.testopen = False
                        supply.testcount += 1
                        notify = True

                if high > supply.influence and high > supply.testhigh:
                    supply.testhigh = high
                    penetration = abs(high - supply.influence)
                    width = abs(supply.sl - supply.influence)
                    supply.testperc = round(penetration / width * 100, 2)
                    notify = True

                if i == 1 and notify:
                    szone.set_test(supply.testcount, supply.testopen, supply.testperc)

        if len(self.demandzones):
            dzone = self.demandzones[0]
            demands = dzone.demands
            i = 0

            for demand in demands:

                i += 1
                if not demand.power:
                    continue

                notify = False
                if demand.testopen:
                    if open_ > demand.influence:
                        demand.testopen = False
                        demand.testcount += 1
                        notify = True

                if not demand.testopen:
                    if low < demand.influence:
                        demand.testopen = True
                        notify = True

                if demand.testopen:
                    if close > demand.influence:
                        demand.testopen = False
                        demand.testcount += 1
                        notify = True

                if low < demand.influence and low < demand.testlow:
                    demand.testlow = low
                    penetration = abs(demand.influence - low)
                    width = abs(demand.influence - demand.sl)
                    demand.testperc = round(penetration / width * 100, 2)
                    notify = True

                if i == 1 and notify:
                    dzone.set_test(demand.testcount, demand.testopen, demand.testperc)

    def trendcallback(self):
        for dzone in self.demandzones:
            dzone.set_trend()

        for szone in self.supplyzones:
            szone.set_trend()

    def curvecallback(self):
        for dzone in self.demandzones:
            dzone.relocate_curve()

        for szone in self.supplyzones:
            szone.relocate_curve()

    def demandzonecreated(self, dzone):
        self.notify(Notifications.DemandZoneCreated, body=dzone)
        self.reset_szone_targets()

    def demandzonemodified(self, dzone):
        self.notify(Notifications.DemandZoneModified, body=dzone)
        self.reset_szone_targets()

    def demandzonebroken(self, dzone):
        self.notify(Notifications.DemandZoneBroken, body=dzone)
        self.reset_szone_targets()

    def supplyzonecreated(self, szone):
        self.notify(Notifications.SupplyZoneCreated, body=szone)
        self.reset_dzone_targets()

    def supplyzonemodified(self, szone):
        self.notify(Notifications.SupplyZoneModified, body=szone)
        self.reset_dzone_targets()

    def supplyzonebroken(self, szone):
        self.notify(Notifications.SupplyZoneBroken, body=szone)
        self.reset_dzone_targets()

    def reset_dzone_targets(self):
        if self.supplyzones:
            closestsz = self.supplyzones[0]
            target, dttarget = closestsz.influence, closestsz.dtinfluence
        else:
            target, dttarget = float("inf"), None
        for dzone in self.demandzones:
            dzone.set_target(target, dttarget)

    def reset_szone_targets(self):
        if self.demandzones:
            closestdz = self.demandzones[0]
            target, dttarget = closestdz.influence, closestdz.dtinfluence
        else:
            target, dttarget = 0, None

        for szone in self.supplyzones:
            szone.set_target(target, dttarget)

    def getstate(self):

        for zonecollection in [self.demandzones, self.supplyzones]:
            for zone in zonecollection:
                zone.dsi = None

        state = dict(
            demands=self.demands,
            supplies=self.supplies,
            demandzones=self.demandzones,
            supplyzones=self.supplyzones,
            curvestate=self.curve.getstate(),
            trendstate=self.trend.getstate(),
            lasttimestamp=self.lasttimestamp
        )
        return state

    def rebase(self, savedstate):
        self.demands = savedstate["demands"]
        self.supplies = savedstate["supplies"]
        self.demandzones = savedstate["demandzones"]
        self.supplyzones = savedstate["supplyzones"]
        self.curve.rebase(savedstate["curvestate"])
        self.trend.rebase(savedstate["trendstate"])

        for zonecollection in [self.demandzones, self.supplyzones]:
            for zone in zonecollection:
                zone.dsi = self


class Base:
    def __init__(self):
        self.high = float("-inf")
        self.low = float("inf")
        self.bodyhigh = float("-inf")
        self.bodylow = float("inf")
        self.upperband = None
        self.lowerband = None
        self.nature = None
        self.entry = None
        self.sl = None
        self.influence = None
        self.testopen = False
        self.testcount = 0

    def add(self, high, low, bodyhigh, bodylow):
        self.high = max(self.high, high)
        self.low = min(self.low, low)
        self.bodyhigh = max(self.bodyhigh, bodyhigh)
        self.bodylow = min(self.bodylow, bodylow)

    def checkbreakout(self, high, low):

        if high > self.high:
            self.low = min(self.low, low)
            self.entry = self.bodyhigh
            self.sl = self.low
            self.influence = round(self.entry + abs(self.entry - self.sl), 2)
            return 1

        elif low < self.low:
            self.high = max(self.high, high)
            self.entry = self.bodylow
            self.sl = self.high
            self.influence = round(self.entry - abs(self.entry - self.sl), 2)
            return -1

        return False


class Curve(Indicator):
    lines = ('supply', 'demand')
    params = (('callback', None), ('breakoutsonly', False))

    def __init__(self):
        self.prevlen = None
        self.boring = Boring(self.data0)
        self.atr = AverageTrueRange(self.data0)
        self.bullbases: List[Base] = []
        self.bearbases: List[Base] = []
        self.base = Base()
        self.distalhigh = float("inf")
        self.proximalhigh = float("inf")
        self.highoncurve = float("inf")
        self.lowoncurve = float("-inf")
        self.proximallow = float("-inf")
        self.distallow = float("-inf")
        self.atrvalue = 0
        self.callback: Callable = self.p.callback

    def next(self):

        _len = len(self.data0)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if _len <= 1:
            return

        open_ = self.data0.open[0]
        high = self.data0.high[0]
        low = self.data0.low[0]
        close = self.data0.close[0]
        phigh = self.data0.high[-1]
        plow = self.data0.low[-1]

        if high > phigh:
            self.higherhigh(high)
        if low < plow:
            self.lowerlow(low)

        if self.p.breakoutsonly:
            return

        self.checktests(open_, high, low, close)

        if self.boring.isboring[0]:
            self.base.add(high=high, low=low, bodyhigh=self.boring.bodyhigh[0], bodylow=self.boring.bodylow[0])
        else:
            breakout = self.base.checkbreakout(high=high, low=low)
            if breakout == 1:
                self.bullbases.insert(0, self.base)
                self.bullbasecreated()
                self.base = Base()
            if breakout == -1:
                self.bearbases.insert(0, self.base)
                self.bearbasecreated()
                self.base = Base()

        self.atrvalue = self.atr[0]

    def higherhigh(self, high):
        while len(self.bearbases):
            base = self.bearbases[0]
            if high > base.sl:
                self.bearbases.remove(base)
                self.bearbasebroken()
            else:
                break

    def lowerlow(self, low):
        while len(self.bullbases):
            base = self.bullbases[0]
            if low < base.sl:
                self.bullbases.remove(base)
                self.bullbasebroken()
            else:
                break

    def bullbasecreated(self):
        self.redrawcurve()

    def bearbasecreated(self):
        self.redrawcurve()

    def bullbasebroken(self):
        self.redrawcurve()

    def bearbasebroken(self):
        self.redrawcurve()

    def redrawcurve(self):
        if not self.bearbases:
            if not self.bullbases:
                self.distalhigh = float("inf")
                self.proximalhigh = float("inf")
                self.highoncurve = float("inf")
                self.lowoncurve = float("-inf")
                self.proximallow = float("-inf")
                self.distallow = float("-inf")
            else:
                self.distalhigh = float("inf")
                self.proximalhigh = float("inf")
                self.highoncurve = float("inf")
                self.lowoncurve = self.bullbases[0].entry
                self.proximallow = self.bullbases[0].entry
                self.distallow = self.bullbases[0].sl
        else:
            if not self.bullbases:
                self.distalhigh = self.bearbases[0].sl
                self.proximalhigh = self.bearbases[0].entry
                self.highoncurve = self.bearbases[0].entry
                self.lowoncurve = float("-inf")
                self.proximallow = float("-inf")
                self.distallow = float("-inf")
            else:
                self.distalhigh = self.bearbases[0].sl
                self.proximalhigh = self.bearbases[0].entry
                self.proximallow = self.bullbases[0].entry
                self.distallow = self.bullbases[0].sl

                region = round(self.proximalhigh - self.proximallow, 2)
                if region < self.atrvalue:
                    if self.bullbases[0].testcount > self.bearbases[0].testcount:
                        self.bullbases.pop(0)
                        self.redrawcurve()
                        return
                    elif self.bullbases[0].testcount < self.bearbases[0].testcount:
                        self.bearbases.pop(0)
                        self.redrawcurve()
                        return
                    else:
                        if self.data0.close[0] > self.proximalhigh:
                            self.bearbases.pop(0)
                            self.redrawcurve()
                            return
                        elif self.data0.close[0] < self.proximallow:
                            self.bullbases.pop(0)
                            self.redrawcurve()
                            return
                self.lowoncurve = round(self.proximallow + (region / 3), 2)
                self.highoncurve = round(self.proximalhigh - (region / 3), 2)
        self.callback()

    def checktests(self, open_, high, low, close):
        if self.proximalhigh != float("inf"):
            if self.bearbases[0].testopen:
                if open_ < self.proximalhigh:
                    self.bearbases[0].testopen = False
            if not self.bearbases[0].testopen:
                if high > self.proximalhigh:
                    self.bearbases[0].testopen = True
                    self.bearbases[0].testcount += 1
            if self.bearbases[0].testopen:
                if close < self.proximalhigh:
                    self.bearbases[0].testopen = False

        if self.proximallow != float("-inf"):
            if self.bullbases[0].testopen:
                if open_ > self.proximallow:
                    self.bullbases[0].testopen = False
            if not self.bullbases[0].testopen:
                if low < self.proximallow:
                    self.bullbases[0].testopen = True
                    self.bullbases[0].testcount += 1
            if self.bullbases[0].testopen:
                if close > self.proximallow:
                    self.bullbases[0].testopen = False

    def getstate(self):
        state = dict(
            bullbases=self.bullbases,
            bearbases=self.bearbases,
            base=self.base,
            distalhigh=self.distalhigh,
            proximalhigh=self.proximalhigh,
            highoncurve=self.highoncurve,
            lowoncurve=self.lowoncurve,
            proximallow=self.proximallow,
            distallow=self.distallow,
            atrvalue=self.atrvalue
        )
        return state

    def rebase(self, savedstate):
        self.bullbases = savedstate["bullbases"]
        self.bearbases = savedstate["bearbases"]
        self.base = savedstate["base"]
        self.distalhigh = savedstate["distalhigh"]
        self.proximalhigh = savedstate["proximalhigh"]
        self.highoncurve = savedstate["highoncurve"]
        self.lowoncurve = savedstate["lowoncurve"]
        self.proximallow = savedstate["proximallow"]
        self.distallow = savedstate["distallow"]
        self.atrvalue = savedstate["atrvalue"]


class Trend(Indicator):
    lines = ('supply', 'demand')
    params = (('callback', None),)

    def __init__(self):
        self.prevlen = None
        self.pivots = Pivots(self.data0)
        self.onsphcreated = self.trialcall_sph
        self.onsplcreated = self.trialcall_spl
        self.sph0, self.sph1 = None, None
        self.spl0, self.spl1 = None, None
        self.trend = 0
        self.callback: Callable = self.p.callback

    def next(self):

        _len = len(self.data0)
        if self.prevlen and _len == self.prevlen:
            return

        self.prevlen = _len

        if _len < 1:
            return

        if self.pivots.new_sph:
            self.pivots.new_sph = False
            self.onsphcreated()

        elif self.pivots.new_spl:
            self.pivots.new_spl = False
            self.onsplcreated()

    def trialcall_sph(self):
        self.onsphcreated = self.sphcreated

    def trialcall_spl(self):
        self.onsplcreated = self.splcreated

    def splcreated(self):
        self.spl1 = self.spl0
        self.spl0 = self.pivots.spl_value
        self.checktrend()

    def sphcreated(self):
        self.sph1 = self.sph0
        self.sph0 = self.pivots.sph_value
        self.checktrend()

    def checktrend(self):
        prevtrend = self.trend

        if self.sph1 and self.spl1:
            up, down = 0, 0

            if self.sph0 > self.sph1:
                up += 1
            elif self.sph0 < self.sph1:
                down += 1

            if self.spl0 > self.spl1:
                up += 1
            elif self.spl0 < self.spl1:
                down += 1

            if up > down:
                self.trend = 1
            elif down > up:
                self.trend = -1
            else:
                self.trend = 0
        else:
            self.trend = 0

        if self.trend != prevtrend:
            self.callback()

    def getstate(self):
        state = dict(
            sph0=self.sph0,
            sph1=self.sph1,
            spl0=self.spl0,
            spl1=self.spl1,
            trend=self.trend
        )
        return state

    def rebase(self, savedstate):
        self.sph0 = savedstate["sph0"]
        self.sph1 = savedstate["sph1"]
        self.spl0 = savedstate["spl0"]
        self.spl1 = savedstate["spl1"]
        self.trend = savedstate["trend"]
