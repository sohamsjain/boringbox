import pickle

dsiobj = r"dsicache/day/dsi.obj"
dsicopyobj = r"dsicache/day/dsicopy.obj"
dsi15 = r"dsicache/min15/15dsi.obj"
dsi15copy = r"dsicache/min15/15dsicopy.obj"

with open(dsiobj, "rb") as file:
    dsiobjx = pickle.load(file)
with open(dsicopyobj, "rb") as file:
    dsireplicaobjx = pickle.load(file)
with open(dsi15, "rb") as file:
    dsi15x = pickle.load(file)
with open(dsi15copy, "rb") as file:
    dsi15copyx = pickle.load(file)