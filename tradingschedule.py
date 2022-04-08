from datetime import datetime, timedelta
from typing import List

import pandas_market_calendars

pmcnse = pandas_market_calendars.get_calendar("NSE")

schedule = \
    pmcnse.schedule(datetime.now().date() - timedelta(days=10),
                    datetime.now().date() + timedelta(days=10),
                    tz="Asia/Kolkata")["market_close"].to_list()

schedule = [t.to_pydatetime().replace(tzinfo=None) for t in schedule]
now = datetime.now()
pastcloses: List[datetime] = [dt for dt in schedule if dt < now]
lastclosingtime: datetime = pastcloses[-1]
futurecloses: List[datetime] = [dt for dt in schedule if dt > now]
nextclosingtime: datetime = futurecloses[0]
