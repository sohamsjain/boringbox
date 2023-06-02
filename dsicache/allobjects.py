import pickle

dsiD = r"dsicache/day/dsi.obj"
dsiDcopy = r"dsicache/day/dsicopy.obj"
dsi15 = r"dsicache/min15/15dsi.obj"
dsi15copy = r"dsicache/min15/15dsicopy.obj"
dsi5 = r"dsicache/min5/5dsi.obj"
dsi5copy = r"dsicache/min5/5dsicopy.obj"
dsi75 = r"dsicache/min75/75dsi.obj"
dsi75copy = r"dsicache/min75/75dsicopy.obj"


dsiDx = None
dsiD_lts = None
dsiDcopyx = None
dsiDcopy_lts = None
dsi15x = None
dsi15_lts = None
dsi15copyx = None
dsi15copy_lts = None
dsi75x = None
dsi75_lts = None
dsi75copyx = None
dsi75copy_lts = None
dsi5x = None
dsi5_lts = None
dsi5copyx = None
dsi5copy_lts = None


def loadobjects():
    global dsiDx, dsiDcopyx, dsi15x, dsi15copyx, dsi5x, dsi5copyx, dsi75x, dsi75copyx, dsiD_lts, dsiDcopy_lts, dsi15_lts, dsi15copy_lts, dsi5_lts, dsi5copy_lts, dsi75_lts, dsi75copy_lts

    with open(dsiD, "rb") as file:
        dsiDx = pickle.load(file)
        dsiD_lts = max([v["lasttimestamp"] for k, v in dsiDx.items() if v["lasttimestamp"] is not None])
    with open(dsiDcopy, "rb") as file:
        dsiDcopyx = pickle.load(file)
        dsiDcopy_lts = max([v["lasttimestamp"] for k, v in dsiDcopyx.items() if v["lasttimestamp"] is not None])
    with open(dsi15, "rb") as file:
        dsi15x = pickle.load(file)
        # dsi15_lts = max([v["lasttimestamp"] for k, v in dsi15x.items() if v["lasttimestamp"] is not None])
    with open(dsi15copy, "rb") as file:
        dsi15copyx = pickle.load(file)
        dsi15copy_lts = max([v["lasttimestamp"] for k, v in dsi15copyx.items() if v["lasttimestamp"] is not None])
    with open(dsi5, "rb") as file:
        dsi5x = pickle.load(file)
        dsi5_lts = max([v["lasttimestamp"] for k, v in dsi5x.items() if v["lasttimestamp"] is not None])
    with open(dsi5copy, "rb") as file:
        dsi5copyx = pickle.load(file)
        dsi5copy_lts = max([v["lasttimestamp"] for k, v in dsi5copyx.items() if v["lasttimestamp"] is not None])
    with open(dsi75, "rb") as file:
        dsi75x = pickle.load(file)
        dsi75_lts = max([v["lasttimestamp"] for k, v in dsi75x.items() if v["lasttimestamp"] is not None])
    with open(dsi75copy, "rb") as file:
        dsi75copyx = pickle.load(file)
        dsi75copy_lts = max([v["lasttimestamp"] for k, v in dsi75copyx.items() if v["lasttimestamp"] is not None])

loadobjects()
