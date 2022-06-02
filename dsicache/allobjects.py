import pickle

dsiobj = r"dsicache/day/dsi.obj"
dsicopyobj = r"dsicache/day/dsicopy.obj"
dsi15 = r"dsicache/min15/15dsi.obj"
dsi15copy = r"dsicache/min15/15dsicopy.obj"

dsiDx = None
dsiD_lts = None
dsiDcopyx = None
dsiDcopy_lts = None
dsi15x = None
dsi15_lts = None
dsi15copyx = None
dsi15copy_lts = None


def loadobjects():
    global dsiDx, dsiDcopyx, dsi15x, dsi15copyx, dsiD_lts, dsiDcopy_lts, dsi15_lts, dsi15copy_lts

    with open(dsiobj, "rb") as file:
        dsiDx = pickle.load(file)
        dsiD_lts = max([v["lasttimestamp"] for k, v in dsiDx.items() if v["lasttimestamp"] is not None])
    with open(dsicopyobj, "rb") as file:
        dsiDcopyx = pickle.load(file)
        dsiDcopy_lts = max([v["lasttimestamp"] for k, v in dsiDcopyx.items() if v["lasttimestamp"] is not None])
    with open(dsi15, "rb") as file:
        dsi15x = pickle.load(file)
        dsi15_lts = max([v["lasttimestamp"] for k, v in dsi15x.items() if v["lasttimestamp"] is not None])
    with open(dsi15copy, "rb") as file:
        dsi15copyx = pickle.load(file)
        dsi15copy_lts = max([v["lasttimestamp"] for k, v in dsi15copyx.items() if v["lasttimestamp"] is not None])


loadobjects()
