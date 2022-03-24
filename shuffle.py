import pandas as pd
from mygoogle.sprint import GoogleSprint

columns = [
    "Ticker",
    "D Entry",
    "D Stoploss",
    "D ATR",
    "D Points",
    "D Ratio",
    "D Strength",
    "D Test",
    "D Trend",
    "D Curve",
    "D Time",
    "D Score",
    "Last Close",
    "S Score",
    "S Ratio",
    "S Strength",
    "S Test",
    "S Trend",
    "S Curve",
    "S Time",
    "S Entry",
    "S Stoploss",
    "S ATR",
    "S Points",
]

g = GoogleSprint()
wb = g.gs.open("Demand Supply Daily")
ws = wb.worksheet("Zones1")
ws2 = wb.worksheet("Zones2")
df = g.fetch_sheet_values(ws)
shuffledf = df[columns]
g.update_sheet(ws2, shuffledf)
print("Done")