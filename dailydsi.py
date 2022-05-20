import pickle
import sys
import traceback
from datetime import timedelta, time

import backtrader as bt
import pandas as pd
from backtrader.utils import AutoOrderedDict

from dsicache.allobjects import loadobjects, dsiD_lts
from indicators.oddenhancers import DSIndicator
from mygoogle.sprint import GoogleSprint
from mytelegram.raven import Raven
from tradingschedule import lastclosingtime

raven = Raven()

if lastclosingtime == dsiD_lts:
    msg = f"Cache Update\n Timeframe: Days\n Compression: 1\n Updated Till: {dsiD_lts}"
    print(msg)
    raven.send_all_clients(msg)
    raven.stop()
    sys.exit()

fromdate = lastclosingtime.date() - timedelta(days=500)
sessionstart = time(hour=9, minute=15)
sessionend = time(hour=15, minute=30)

# tickers = ["NIFTY50_IND_NSE",
#            "BANKNIFTY_IND_NSE",
#            "RELIANCE_STK_NSE",
#            "TCS_STK_NSE",
#            "INFY_STK_NSE",
#            "WIPRO_STK_NSE",
#            "ITC_STK_NSE",
#            "SAIL_STK_NSE",
#            "LT_STK_NSE",
#            "HINDUNILV_STK_NSE",
#            "TATASTEEL_STK_NSE",
#            "GAIL_STK_NSE",
#            "HCLTECH_STK_NSE",
#            "SIEMENS_STK_NSE",
#            "MARUTI_STK_NSE",
#            "HINDALCO_STK_NSE",
#            "AMBUJACEM_STK_NSE",
#            "CIPLA_STK_NSE",
#            "ACC_STK_NSE",
#            "HEROMOTOC_STK_NSE",
#            "BPCL_STK_NSE",
#            "ZEEL_STK_NSE",
#            "DRREDDY_STK_NSE",
#            "HINDPETRO_STK_NSE",
#            "DABUR_STK_NSE",
#            "DLF_STK_NSE",
#            "NTPC_STK_NSE",
#            "POWERGRID_STK_NSE",
#            "IDEA_STK_NSE",
#            "UBL_STK_NSE",
#            "UPL_STK_NSE",
#            "ULTRACEMC_STK_NSE",
#            "OFSS_STK_NSE",
#            "VOLTAS_STK_NSE",
#            "DIVISLAB_STK_NSE",
#            "JINDALSTE_STK_NSE",
#            "ESCORTS_STK_NSE",
#            "COFORGE_STK_NSE",
#            "NMDC_STK_NSE",
#            "RECLTD_STK_NSE",
#            "CUMMINSIN_STK_NSE",
#            "CANBK_STK_NSE",
#            "EXIDEIND_STK_NSE",
#            "COLPAL_STK_NSE",
#            "CUB_STK_NSE",
#            "INDHOTEL_STK_NSE",
#            "INDUSINDB_STK_NSE",
#            "NAUKRI_STK_NSE",
#            "IOC_STK_NSE",
#            "GODREJCP_STK_NSE",
#            "GLENMARK_STK_NSE",
#            "PAGEIND_STK_NSE",
#            "PFIZER_STK_NSE",
#            "PIDILITIN_STK_NSE",
#            "ADANIENT_STK_NSE",
#            "TVSMOTOR_STK_NSE",
#            "TORNTPOWE_STK_NSE",
#            "TORNTPHAR_STK_NSE",
#            "TATACHEM_STK_NSE",
#            "PEL_STK_NSE",
#            "PETRONET_STK_NSE",
#            "PFC_STK_NSE",
#            "PVR_STK_NSE",
#            "MFSL_STK_NSE",
#            "RAMCOCEM_STK_NSE",
#            "MPHASIS_STK_NSE",
#            "MARICO_STK_NSE",
#            "MRF_STK_NSE",
#            "MOTHERSUM_STK_NSE",
#            "MINDTREE_STK_NSE",
#            "BHARATFOR_STK_NSE",
#            "BIOCON_STK_NSE",
#            "BAJAJ-AUT_STK_NSE",
#            "BAJAJFINS_STK_NSE",
#            "BOSCHLTD_STK_NSE",
#            "ASHOKLEY_STK_NSE",
#            "APOLLOTYR_STK_NSE",
#            "AARTIIND_STK_NSE",
#            "VEDL_STK_NSE",
#            "SRTRANSFI_STK_NSE",
#            "SHREECEM_STK_NSE",
#            "SRF_STK_NSE",
#            "STAR_STK_NSE",
#            "SUNTV_STK_NSE",
#            "NESTLEIND_STK_NSE",
#            "BHARTIART_STK_NSE",
#            "GMRINFRA_STK_NSE",
#            "JUBLFOOD_STK_NSE",
#            "IPCALAB_STK_NSE",
#            "MM_STK_NSE",
#            "MANAPPURA_STK_NSE",
#            "TATACONSU_STK_NSE",
#            "HDFC_STK_NSE",
#            "LUPIN_STK_NSE",
#            "APOLLOHOS_STK_NSE",
#            "KOTAKBANK_STK_NSE",
#            "ADANIPORT_STK_NSE",
#            "COALINDIA_STK_NSE",
#            "SUNPHARMA_STK_NSE",
#            "BALKRISIN_STK_NSE",
#            "COROMANDE_STK_NSE",
#            "LICHSGFIN_STK_NSE",
#            "ONGC_STK_NSE",
#            "AUROPHARM_STK_NSE",
#            "NATIONALU_STK_NSE",
#            "MUTHOOTFI_STK_NSE",
#            "TITAN_STK_NSE",
#            "LTFH_STK_NSE",
#            "TATAMOTOR_STK_NSE",
#            "APLLTD_STK_NSE",
#            "TATAPOWER_STK_NSE",
#            "BHEL_STK_NSE",
#            "MCX_STK_NSE",
#            "AMARAJABA_STK_NSE",
#            "INDUSTOWE_STK_NSE",
#            "MMFIN_STK_NSE",
#            "IBULHSGFI_STK_NSE",
#            "PIIND_STK_NSE",
#            "ABFRL_STK_NSE",
#            "ASIANPAIN_STK_NSE",
#            "FEDERALBN_STK_NSE",
#            "GODREJPRO_STK_NSE",
#            "DEEPAKNTR_STK_NSE",
#            "AXISBANK_STK_NSE",
#            "HAVELLS_STK_NSE",
#            "ASTRAL_STK_NSE",
#            "SBIN_STK_NSE",
#            "ICICIBANK_STK_NSE",
#            "PNB_STK_NSE",
#            "BERGEPAIN_STK_NSE",
#            "BANKBAROD_STK_NSE",
#            "GRANULES_STK_NSE",
#            "TECHM_STK_NSE",
#            "SYNGENE_STK_NSE",
#            "CADILAHC_STK_NSE",
#            "BATAINDIA_STK_NSE",
#            "IDFCFIRST_STK_NSE",
#            "INDIGO_STK_NSE",
#            "ALKEM_STK_NSE",
#            "LALPATHLA_STK_NSE",
#            "MGL_STK_NSE",
#            "LTI_STK_NSE",
#            "RBLBANK_STK_NSE",
#            "BAJFINANC_STK_NSE",
#            "TRENT_STK_NSE",
#            "LTTS_STK_NSE",
#            "ICICIPRUL_STK_NSE",
#            "GRASIM_STK_NSE",
#            "JSWSTEEL_STK_NSE",
#            "BEL_STK_NSE",
#            "AUBANK_STK_NSE",
#            "NAVINFLUO_STK_NSE",
#            "ICICIGI_STK_NSE",
#            "SBILIFE_STK_NSE",
#            "CANFINHOM_STK_NSE",
#            "IGL_STK_NSE",
#            "NAM-INDIA_STK_NSE",
#            "HDFCLIFE_STK_NSE",
#            "BANDHANBN_STK_NSE",
#            "HAL_STK_NSE",
#            "MCDOWELL-_STK_NSE",
#            "CONCOR_STK_NSE",
#            "HDFCAMC_STK_NSE",
#            "IEX_STK_NSE",
#            "BRITANNIA_STK_NSE",
#            "GUJGASLTD_STK_NSE",
#            "METROPOLI_STK_NSE",
#            "POLYCAB_STK_NSE",
#            "CHOLAFIN_STK_NSE",
#            "INDIAMART_STK_NSE",
#            "HDFCBANK_STK_NSE",
#            "IRCTC_STK_NSE",
#            "EICHERMOT_STK_NSE",
#            "DIXON_STK_NSE"]
valid = ['NIFTY50_IND_NSE',
         'BANKNIFTY_IND_NSE',
         'RELIANCE_STK_NSE',
         'TCS_STK_NSE',
         'INFY_STK_NSE',
         'WIPRO_STK_NSE',
         'ITC_STK_NSE',
         'SAIL_STK_NSE',
         'LT_STK_NSE',
         'HINDUNILV_STK_NSE',
         'TATASTEEL_STK_NSE',
         'GAIL_STK_NSE',
         'HCLTECH_STK_NSE',
         'SIEMENS_STK_NSE',
         'MARUTI_STK_NSE',
         'HINDALCO_STK_NSE',
         'AMBUJACEM_STK_NSE',
         'CIPLA_STK_NSE',
         'ACC_STK_NSE',
         'HEROMOTOC_STK_NSE',
         'BPCL_STK_NSE',
         'ZEEL_STK_NSE',
         'DRREDDY_STK_NSE',
         'HINDPETRO_STK_NSE',
         'DABUR_STK_NSE',
         'DLF_STK_NSE',
         'NTPC_STK_NSE',
         'POWERGRID_STK_NSE',
         'IDEA_STK_NSE',
         'UBL_STK_NSE',
         'UPL_STK_NSE',
         'ULTRACEMC_STK_NSE',
         'OFSS_STK_NSE',
         'VOLTAS_STK_NSE',
         'DIVISLAB_STK_NSE',
         'JINDALSTE_STK_NSE',
         'ESCORTS_STK_NSE',
         'COFORGE_STK_NSE',
         'NMDC_STK_NSE',
         'RECLTD_STK_NSE',
         'CUMMINSIN_STK_NSE',
         'CANBK_STK_NSE',
         'EXIDEIND_STK_NSE',
         'COLPAL_STK_NSE',
         'CUB_STK_NSE',
         'INDHOTEL_STK_NSE',
         'INDUSINDB_STK_NSE',
         'NAUKRI_STK_NSE',
         'IOC_STK_NSE',
         'GODREJCP_STK_NSE',
         'GLENMARK_STK_NSE',
         'PAGEIND_STK_NSE',
         'PFIZER_STK_NSE',
         'PIDILITIN_STK_NSE',
         'ADANIENT_STK_NSE',
         'TVSMOTOR_STK_NSE',
         'TORNTPOWE_STK_NSE',
         'TORNTPHAR_STK_NSE',
         'TATACHEM_STK_NSE',
         'PEL_STK_NSE',
         'PETRONET_STK_NSE',
         'PFC_STK_NSE',
         'PVR_STK_NSE',
         'MFSL_STK_NSE',
         'RAMCOCEM_STK_NSE',
         'MPHASIS_STK_NSE',
         'MARICO_STK_NSE',
         'MRF_STK_NSE',
         'MOTHERSUM_STK_NSE',
         'MINDTREE_STK_NSE',
         'BHARATFOR_STK_NSE',
         'BIOCON_STK_NSE',
         'BAJAJ-AUT_STK_NSE',
         'BAJAJFINS_STK_NSE',
         'BOSCHLTD_STK_NSE',
         'ASHOKLEY_STK_NSE',
         'APOLLOTYR_STK_NSE',
         'AARTIIND_STK_NSE',
         'VEDL_STK_NSE',
         'SRTRANSFI_STK_NSE',
         'SHREECEM_STK_NSE',
         'SRF_STK_NSE',
         'STAR_STK_NSE',
         'SUNTV_STK_NSE',
         'NESTLEIND_STK_NSE',
         'BHARTIART_STK_NSE',
         'GMRINFRA_STK_NSE',
         'JUBLFOOD_STK_NSE',
         'IPCALAB_STK_NSE',
         'MM_STK_NSE',
         'MANAPPURA_STK_NSE',
         'TATACONSU_STK_NSE',
         'HDFC_STK_NSE',
         'APOLLOHOS_STK_NSE',
         'KOTAKBANK_STK_NSE',
         'ADANIPORT_STK_NSE',
         'COALINDIA_STK_NSE',
         'SUNPHARMA_STK_NSE',
         'BALKRISIN_STK_NSE',
         'COROMANDE_STK_NSE',
         'LICHSGFIN_STK_NSE',
         'ONGC_STK_NSE',
         'AUROPHARM_STK_NSE',
         'NATIONALU_STK_NSE',
         'MUTHOOTFI_STK_NSE',
         'TITAN_STK_NSE',
         'LTFH_STK_NSE',
         'TATAMOTOR_STK_NSE',
         'APLLTD_STK_NSE',
         'TATAPOWER_STK_NSE',
         'BHEL_STK_NSE',
         'MCX_STK_NSE',
         'AMARAJABA_STK_NSE',
         'MMFIN_STK_NSE',
         'PIIND_STK_NSE',
         'ASIANPAIN_STK_NSE',
         'FEDERALBN_STK_NSE',
         'GODREJPRO_STK_NSE',
         'DEEPAKNTR_STK_NSE',
         'AXISBANK_STK_NSE',
         'HAVELLS_STK_NSE',
         'ASTRAL_STK_NSE',
         'SBIN_STK_NSE',
         'ICICIBANK_STK_NSE',
         'PNB_STK_NSE',
         'BERGEPAIN_STK_NSE',
         'BANKBAROD_STK_NSE',
         'GRANULES_STK_NSE',
         'TECHM_STK_NSE',
         'BATAINDIA_STK_NSE',
         'BAJFINANC_STK_NSE',
         'TRENT_STK_NSE',
         'GRASIM_STK_NSE',
         'JSWSTEEL_STK_NSE',
         'BEL_STK_NSE',
         'NAVINFLUO_STK_NSE',
         'CANFINHOM_STK_NSE',
         'IGL_STK_NSE',
         'MCDOWELL-_STK_NSE',
         'CONCOR_STK_NSE',
         'BRITANNIA_STK_NSE',
         'CHOLAFIN_STK_NSE',
         'HDFCBANK_STK_NSE',
         'EICHERMOT_STK_NSE'
         ]
invalid = [
    "LUPIN_STK_NSE",
    "INDUSTOWE_STK_NSE",
    "IBULHSGFI_STK_NSE",
    "ABFRL_STK_NSE",
    "ICICIPRUL_STK_NSE",
    "LTTS_STK_NSE",
    "RBLBANK_STK_NSE",
    "LTI_STK_NSE",
    "MGL_STK_NSE",
    "LALPATHLA_STK_NSE",
    "ALKEM_STK_NSE",
    "INDIGO_STK_NSE",
    "IDFCFIRST_STK_NSE",
    "CADILAHC_STK_NSE",
    "SYNGENE_STK_NSE",
    "AUBANK_STK_NSE",
    "ICICIGI_STK_NSE",
    "SBILIFE_STK_NSE",
    "NAM-INDIA_STK_NSE",
    "HDFCLIFE_STK_NSE",
    "BANDHANBN_STK_NSE",
    "HAL_STK_NSE",
    "HDFCAMC_STK_NSE",
    "IEX_STK_NSE",
    "GUJGASLTD_STK_NSE",
    "METROPOLI_STK_NSE",
    "POLYCAB_STK_NSE",
    "INDIAMART_STK_NSE",
    "IRCTC_STK_NSE",
    "DIXON_STK_NSE",
]
filename = "dsicache/day/dsi.obj"
g = GoogleSprint()
wb = g.gs.open("Demand Supply Daily")
ws = wb.worksheet("Zones1")
finaldf = None


class TestSt2(bt.Strategy):

    def __init__(self):
        self.resource = AutoOrderedDict()
        self.states = self.deserialize()
        nonce = 0
        while nonce < len(self.datas):
            dsidata = self.datas[nonce]  # Day 1
            trenddata = self.datas[nonce + 1]  # Week 1
            curvedata = self.datas[nonce + 2]  # Month 1

            dname = dsidata._dataname
            try:
                savedstate = self.states[dname] if self.states else None
            except KeyError:
                savedstate = None
            self.resource[dname].datas.daily = dsidata
            self.resource[dname].datas.weekly = trenddata
            self.resource[dname].datas.monthly = curvedata
            self.resource[dname].ds = DSIndicator(dsidata, trenddata, curvedata, savedstate=savedstate)
            nonce += 3

    def next(self):
        global finaldf
        if self.data0.datetime.datetime(0) == lastclosingtime:
            allrows = list()
            for dname, val in self.resource.items():
                ds: DSIndicator = val.ds
                close = val.datas.daily.close[0]
                atr = ds.atr[0]

                try:
                    d1 = ds.demandzones[0]
                    d1_entry = d1.entry
                    d1_stoploss = d1.sl
                    d1_score = d1.score
                    ptd = close - d1_entry
                    atd = round(ptd / atr, 2)
                    dstrength = d1.strength
                    dtbase = d1.timeatbase
                    dratio = d1.ratio if d1.ratio != float("inf") else "Anant"
                    dcurve = d1.location
                    dtrend = d1.trend
                    dtest = d1.testcount
                except IndexError:
                    d1_entry = None
                    d1_stoploss = None
                    d1_score = None
                    ptd = None
                    atd = None
                    dstrength = None
                    dtbase = None
                    dratio = None
                    dcurve = None
                    dtrend = None
                    dtest = None

                try:
                    s1 = ds.supplyzones[0]
                    s1_entry = s1.entry
                    s1_stoploss = s1.sl
                    s1_score = s1.score
                    pts = s1_entry - close
                    ats = round(pts / atr, 2)
                    sstrength = s1.strength
                    stbase = s1.timeatbase
                    sratio = s1.ratio if s1.ratio != float("inf") else "Anant"
                    scurve = s1.location
                    strend = s1.trend
                    stest = s1.testcount
                except IndexError:
                    s1_entry = None
                    s1_stoploss = None
                    s1_score = None
                    pts = None
                    ats = None
                    sstrength = None
                    stbase = None
                    sratio = None
                    scurve = None
                    strend = None
                    stest = None

                row = {
                    "Ticker": dname.split("_")[0],
                    "D Trend": dtrend,
                    "D Curve": dcurve,
                    "D Time": dtbase,
                    "D Strength": dstrength,
                    "D Test": dtest,
                    "D Ratio": dratio,
                    "D ATR": atd,
                    "D Points": ptd,
                    "D Stoploss": d1_stoploss,
                    "D Entry": d1_entry,
                    "D Score": d1_score,
                    "Last Close": close,
                    "S Score": s1_score,
                    "S Entry": s1_entry,
                    "S Stoploss": s1_stoploss,
                    "S Points": pts,
                    "S ATR": ats,
                    "S Ratio": sratio,
                    "S Test": stest,
                    "S Strength": sstrength,
                    "S Time": stbase,
                    "S Curve": scurve,
                    "S Trend": strend,
                }

                allrows.append(row)

            df = pd.DataFrame(allrows)
            df.fillna("", inplace=True)
            if finaldf is None:
                finaldf = df
            else:
                finaldf = finaldf.append(df, ignore_index=True)
            finaldf.drop_duplicates(inplace=True)
            g.update_sheet(ws, finaldf)
            self.cerebro.runstop()

    def serialize(self):
        blocks = {dname: val.ds.getstate() for dname, val in self.resource.items()}
        if self.states:
            self.states.update(blocks)
        else:
            self.states = blocks
        with open(filename, "wb") as file:
            pickle.dump(self.states, file)

    def deserialize(self):
        try:
            with open(filename, "rb") as file:
                dsidict = pickle.load(file)
            return dsidict
        except (EOFError, FileNotFoundError):
            return None

    def stop(self):
        self.serialize()


tickers = valid

while tickers:
    t = tickers[:50]
    tickers = tickers[50:]
    print(t)
    cerebro = bt.Cerebro(runonce=False)
    cerebro.addstrategy(TestSt2)
    cerebro.addcalendar("BSE")
    store = bt.stores.IBStore(port=7497, _debug=False)
    cerebro.setbroker(store.getbroker())

    for ticker in t:
        data0 = store.getdata(dataname=ticker, fromdate=fromdate,
                              sessionstart=sessionstart,
                              sessionend=sessionend,
                              historical=True, timeframe=bt.TimeFrame.Days)

        cerebro.adddata(data0)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Weeks)

        cerebro.resampledata(data0, timeframe=bt.TimeFrame.Months)
    try:
        thestrats = cerebro.run(stdstats=False)
    except Exception as e:
        print(t[0])
        print(traceback.format_exc())
        raven.send_all_clients(t[0])
        raven.send_all_clients(traceback.format_exc())

loadobjects()
msg = f"Cache Update\n Timeframe: Days\n Compression: 1\n Updated Till: {dsiD_lts}"
raven.send_all_clients(msg)
raven.stop()
