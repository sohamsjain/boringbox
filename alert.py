from pynse import *

from mytelegram.raven import Raven

raven = Raven()
nse = Nse()
f15 = r"C:\Users\PC2\PycharmProjects\boringbox\15dsi.obj"
fD = r"C:\Users\PC2\PycharmProjects\boringbox\dsi.obj"
indexes = {IndexSymbol.Nifty50: "NIFTY50_IND_NSE",
           IndexSymbol.NiftyBank: "BANKNIFTY_IND_NSE"}

atrs = {IndexSymbol.Nifty50: 350.0,
        IndexSymbol.NiftyBank: 1053.0}

with open(f15, "rb") as file:
    state15 = pickle.load(file)

with open(fD, "rb") as file:
    stateD = pickle.load(file)

for k, v in indexes.items():
    s15 = state15[v]
    sD = stateD[v]
    # atr = s15["curvestate"]["atrvalue"]
    atr = atrs[k]
    close = float(nse.get_indices(k)["last"][0])
    upperband = round(close + atr, 2)
    lowerband = round(close - atr, 2)
    print(atr, close, upperband, lowerband)

    message = ""
    message += "Date: " + str(dt.datetime.now().date()) + "\n\n"

    szs = []
    for sz in sD["supplyzones"]:
        if sz.entry <= upperband:
            szs.append(sz)
        else:
            break

    while len(szs):
        sz = szs.pop(-1)
        message += f"D  -{str(sz.entry)} Sl: {str(sz.sl)} {str(sz.ratio)} {str(sz.score)}\n"

    #####################################

    szs = []
    for sz in s15["supplyzones"]:
        if sz.entry <= upperband:
            szs.append(sz)
        else:
            break

    while len(szs):
        sz = szs.pop(-1)
        message += f"15  -{str(sz.entry)} Sl: {str(sz.sl)} {str(sz.ratio)} {str(sz.score)}\n"

    #####################################
    # ____________________________________

    message += f"\n{v.split('_')[0]} {close} {atr}\n\n"

    # ____________________________________
    #####################################

    dzs = []
    for dz in s15["demandzones"]:
        if dz.entry >= lowerband:
            dzs.append(dz)
        else:
            break

    while len(dzs):
        dz = dzs.pop(0)
        message += f"15  {str(dz.entry)} Sl: {str(dz.sl)} {str(dz.ratio)} {str(dz.score)}\n"

    #####################################

    dzs = []
    for dz in sD["demandzones"]:
        if dz.entry >= lowerband:
            dzs.append(dz)
        else:
            break

    while len(dzs):
        dz = dzs.pop(0)
        message += f"D  {str(dz.entry)} Sl: {str(dz.sl)} {str(dz.ratio)} {str(dz.score)}\n"

    raven.send_all_clients(message)

raven.stop()
