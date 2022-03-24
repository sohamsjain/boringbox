import pickle
from datetime import datetime, timedelta

import backtrader as bt
from backtrader.utils import AutoOrderedDict

from indicators.oddenhancers import DSIndicator

fromdate = datetime.now().date() - timedelta(days=30)
sessionstart = datetime.now().time().replace(hour=9, minute=15, second=0, microsecond=0)
sessionend = datetime.now().time().replace(hour=15, minute=30, second=0, microsecond=0)
valid = [
    'NIFTY50_IND_NSE',
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


class TasteStretejy(bt.Strategy):

    def __init__(self):
        self.resource = AutoOrderedDict()
        self.states = self.deserialize()

        nonce = 0
        while nonce < len(self.datas):
            dsidata = self.datas[nonce]  # Minute 15
            trenddata = self.datas[nonce + 1]  # Minute 75
            curvedata = self.datas[nonce + 2]  # Day 1

            dname = dsidata._dataname
            try:
                savedstate = self.states[dname] if self.states else None
            except KeyError:
                savedstate = None
            self.resource[dname].ds = DSIndicator(dsidata, trenddata, curvedata, savedstate=savedstate,
                                                  curvebreakoutsonly=dsidata.islive())
            nonce += 3

    def next(self):

        if str(self.data0.datetime.datetime(0)) == "2022-03-24 15:30:00":
            self.cerebro.runstop()

    def serialize(self):
        blocks = {dname: val.ds.getstate() for dname, val in self.resource.items()}
        if self.states:
            self.states.update(blocks)
        else:
            self.states = blocks
        with open("15dsi.obj", "wb") as file:
            pickle.dump(self.states, file)

    def deserialize(self):
        try:
            with open("15dsi.obj", "rb") as file:
                dsidict = pickle.load(file)
            return dsidict
        except (EOFError, FileNotFoundError):
            return None

    def stop(self):
        self.serialize()


class Minutes15BackwardLookingFilter(object):

    def __init__(self, data):
        pass

    def __call__(self, data):
        data.datetime[0] = data.date2num(data.datetime.datetime(0) + timedelta(minutes=15))
        return False


def getdata(ticker):
    data = store.getdata(dataname=ticker, fromdate=fromdate, sessionstart=sessionstart,
                         historical=True, sessionend=sessionend, timeframe=bt.TimeFrame.Minutes,
                         compression=15)

    data.addfilter(Minutes15BackwardLookingFilter)

    cerebro.adddata(data)

    cerebro.resampledata(data,
                         timeframe=bt.TimeFrame.Minutes,
                         compression=75,
                         boundoff=45)

    dailydata = store.getdata(dataname=ticker, fromdate=fromdate, sessionstart=sessionstart,
                              historical=True, sessionend=sessionend, timeframe=bt.TimeFrame.Days,
                              compression=1)

    cerebro.adddata(dailydata)


tickers = ['NIFTY50_IND_NSE', "BANKNIFTY_IND_NSE"]

if __name__ == '__main__':

    while tickers:
        t = tickers[0]
        tickers = tickers[1:]
        print(t)
        cerebro = bt.Cerebro(runonce=False)
        cerebro.addstrategy(TasteStretejy)
        store = bt.stores.IBStore(port=7497)
        cerebro.setbroker(store.getbroker())
        cerebro.addcalendar("BSE")
        getdata(t)
        thestrats = cerebro.run()
