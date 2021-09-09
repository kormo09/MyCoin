import os
import sys
import warnings
import numpy as np
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.static import timedelta_sec, now
warnings.filterwarnings("ignore", category=np.VisibleDeprecationWarning)


class UpdaterTick:
    def __init__(self, tickQ, queryQ, windowQ):
        self.tickQ = tickQ
        self.queryQ = queryQ
        self.windowQ = windowQ

        self.dict_df = {}
        self.time_info = timedelta_sec(60)
        self.Start()

    def Start(self):
        while True:
            tick = self.tickQ.get()
            self.UpdateTickData(tick[0], tick[1], tick[2], tick[3], tick[4], tick[5],
                                tick[6], tick[7], tick[8], tick[9], tick[10])

    def UpdateTickData(self, ticker, c, h, low, per, dm, bid, ask, d, t, receiv_time):
        hlm = round((h + low) / 2)
        hlmp = round((c / hlm - 1) * 100, 2)
        ch = round(bid / ask * 100, 2)
        dt = d + t
        if ticker not in self.dict_df.keys():
            sm = 0
            self.dict_df[ticker] = pd.DataFrame(
                [[c, h, per, hlmp, sm, dm, ch]],
                columns=['현재가', '고가', '등락율', '고저평균대비등락율', '거래대금', '누적거래대금', '체결강도'],
                index=[dt]
            )
        else:
            predm = self.dict_df[ticker]['누적거래대금'][0]
            sm = dm - predm
            self.dict_df[ticker].at[dt] = c, h, per, hlmp, sm, dm, ch

        if now() > self.time_info:
            gap = (now() - receiv_time).total_seconds()
            self.windowQ.put(f'수신시간과 갱신시간의 차이는 [{gap}]초입니다.')
            self.queryQ.put(self.dict_df)
            self.dict_df = {}
            self.time_info = timedelta_sec(60)
