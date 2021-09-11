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

        self.dict_df = {}                   # 틱데이터 저장용 딕셔너리 key: ticker, value: datafame
        self.dict_last_dm = {}              # 직전 누적거래대금 저장용
        self.dict_orderbook = {}            # 오더북 저장용 딕셔너리
        self.time_info = timedelta_sec(60)  # 틱데이터 저장주기
        self.Start()

    def Start(self):
        while True:
            data = self.tickQ.get()
            if type(data) == list:
                self.UpdateTickData(data[0], data[1])
            else:
                self.UpdateOrderbook(data)

    def UpdateTickData(self, data, receiv_time):
        ticker = data['code']
        if ticker not in self.dict_orderbook.keys():
            return

        hlm = round((data['high_price'] + data['low_price']) / 2)
        data['avg_highlow_per'] = round((data['trade_price'] / hlm - 1) * 100, 2)
        dm = data['acc_trade_price']
        dt = data['trade_date'] + data['trade_time']
        try:
            data['second_money'] = dm - self.dict_last_dm[ticker]
        except KeyError:
            data['second_money'] = 0

        data.update(self.dict_orderbook[ticker])
        self.dict_last_dm[ticker] = dm

        if ticker not in self.dict_df.keys():
            self.dict_df[ticker] = pd.DataFrame(data, index=[dt])
        else:
            self.dict_df[ticker].at[dt] = list(data.values())

        if now() > self.time_info:
            gap = (now() - receiv_time).total_seconds()
            self.windowQ.put(f'수신시간과 갱신시간의 차이는 [{gap}]초입니다.')
            self.queryQ.put(self.dict_df)
            self.dict_df = {}
            self.time_info = timedelta_sec(60)

    def UpdateOrderbook(self, data):
        ticker = data['code']
        dict_ob = {'total_ask_size': data['total_ask_size'],
                   'total_bid_size': data['total_bid_size'],
                   'ask_price_2': data['orderbook_units'][1]['ask_price'],
                   'ask_price_1': data['orderbook_units'][0]['ask_price'],
                   'bid_price_1': data['orderbook_units'][0]['bid_price'],
                   'bid_price_2': data['orderbook_units'][1]['bid_price'],
                   'ask_size_2': data['orderbook_units'][1]['ask_size'],
                   'ask_size_1': data['orderbook_units'][0]['ask_size'],
                   'bid_size_1': data['orderbook_units'][0]['bid_size'],
                   'bid_size_2': data['orderbook_units'][1]['bid_size']}
        self.dict_orderbook[ticker] = dict_ob
