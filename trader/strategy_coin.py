import os
import psutil
import sqlite3
import numpy as np
import pandas as pd
from setting import columns_gj1, db_stg, ui_num
from static import now, timedelta_sec, thread_decorator


class StrategyCoin:
    def __init__(self, gubun, windowQ, workerQ, queryQ, stgQ):
        self.gubun = gubun
        self.windowQ = windowQ
        self.workerQ = workerQ
        self.queryQ = queryQ
        self.stgQ = stgQ

        self.list_buy = []
        self.list_sell = []
        self.dict_csan = {}     # key: 종목코드, value: datetime
        self.dict_gsjm = {}     # key: 종목코드, value: DataFrame
        self.dict_intg = {
            '체결강도차이': 0.,
            '거래대금차이': 0,
            '평균시간': 0,
            '청산시간': 0,
            '체결강도하한': 0.,
            '누적거래대금하한': 0,
            '등락율상한': 0.,
            '고저평균대비등락율하한': 0.,

            '스레드': 0,
            '시피유': 0.,
            '메모리': 0.
        }
        self.dict_time = {
            '관심종목': now(),
            '부가정보': now()
        }

        self.Start()

    def Start(self):
        con = sqlite3.connect(db_stg)
        df = pd.read_sql('SELECT * FROM setting', con)
        df = df.set_index('index')
        self.dict_intg['체결강도차이'] = df['체결강도차이'][0]
        self.dict_intg['거래대금차이'] = df['거래대금차이'][0]
        self.dict_intg['평균시간'] = df['평균시간'][0]
        self.dict_intg['청산시간'] = df['청산시간'][0]
        self.dict_intg['체결강도하한'] = df['체결강도하한'][0]
        self.dict_intg['누적거래대금하한'] = df['누적거래대금하한'][0]
        self.dict_intg['등락율상한'] = df['등락율상한'][0]
        self.dict_intg['고저평균대비등락율하한'] = df['고저평균대비등락율하한'][0]
        con.close()
        while True:
            data = self.stgQ.get()
            if len(data) == 2:
                self.UpdateList(data[0], data[1])
            elif len(data) == 13:
                self.BuyStrategy(data[0], data[1], data[2], data[3], data[4], data[5], data[6],
                                 data[7], data[8], data[9], data[10], data[11], data[12])
            elif len(data) == 5:
                self.SellStrategy(data[0], data[1], data[2], data[3], data[4])

            if now() > self.dict_time['관심종목'] and len(self.dict_gsjm) > 0:
                self.windowQ.put([ui_num['관심종목'], self.dict_gsjm])
                self.dict_time['관심종목'] = timedelta_sec(1)
            if now() > self.dict_time['부가정보']:
                self.UpdateInfo()
                self.dict_time['부가정보'] = timedelta_sec(2)

    def UpdateList(self, gubun, tickers):
        if '관심종목초기화' in gubun:
            self.dict_gsjm = {}
            for ticker in tickers:
                data = np.zeros((self.dict_intg['평균시간'] + 2, len(columns_gj1))).tolist()
                df = pd.DataFrame(data, columns=columns_gj1)
                df['체결시간'] = '20000101090000'
                self.dict_gsjm[ticker] = df.copy()
        elif gubun == '매수완료':
            if tickers in self.list_buy:
                self.list_buy.remove(tickers)
        elif gubun == '매도완료':
            if tickers in self.list_sell:
                self.list_sell.remove(tickers)

    def BuyStrategy(self, ticker, c, h, low, per, dm, bid, ask, d, t, uuidnone, injango, batting):
        if ticker not in self.dict_gsjm.keys():
            return

        hlm = round((h + low) / 2)
        hlmp = round((c / hlm - 1) * 100, 2)
        predm = self.dict_gsjm[ticker]['누적거래대금'][0]
        if predm == 0:
            sm = 0
        else:
            sm = dm - predm
        try:
            ch = round(bid / ask * 100, 2)
        except ZeroDivisionError:
            ch = 500.

        if d + t != self.dict_gsjm[ticker]['체결강도'][self.dict_intg['평균시간'] + 1]:
            self.dict_gsjm[ticker] = self.dict_gsjm[ticker].shift(1)
            if self.dict_gsjm[ticker]['체결강도'][self.dict_intg['평균시간']] != 0.:
                avg_sm = round(self.dict_gsjm[ticker]['거래대금'][1:self.dict_intg['평균시간'] + 1].mean(), 2)
                avg_ch = round(self.dict_gsjm[ticker]['체결강도'][1:self.dict_intg['평균시간'] + 1].mean(), 2)
                high_ch = round(self.dict_gsjm[ticker]['체결강도'][1:self.dict_intg['평균시간'] + 1].max(), 2)
                self.dict_gsjm[ticker].at[self.dict_intg['평균시간'] + 1] = 0, 0., 0., avg_sm, 0, avg_ch, high_ch, d + t
        self.dict_gsjm[ticker].at[0] = c, per, hlmp, sm, dm, ch, 0., d + t

        if self.dict_gsjm[ticker]['체결강도'][self.dict_intg['평균시간']] == 0:
            return
        if ticker in self.list_buy:
            return
        if injango:
            return
        if not uuidnone:
            return

        # 전략 비공개

        oc = int(batting / c)
        if oc > 0:
            self.list_buy.append(ticker)
            self.workerQ.put(['매수', ticker, c, oc])

    def SellStrategy(self, ticker, sp, jc, ch, c):
        if ticker in self.list_sell:
            return

        oc = 0

        # 전략 비공개

        if oc > 0:
            self.list_sell.append(ticker)
            self.workerQ.put(['매도', ticker, c, oc])

    @thread_decorator
    def UpdateInfo(self):
        info = [self.gubun, self.dict_intg['메모리'], self.dict_intg['스레드'], self.dict_intg['시피유']]
        self.windowQ.put(info)
        self.UpdateSysinfo()

    def UpdateSysinfo(self):
        p = psutil.Process(os.getpid())
        self.dict_intg['메모리'] = round(p.memory_info()[0] / 2 ** 20.86, 2)
        self.dict_intg['스레드'] = p.num_threads()
        self.dict_intg['시피유'] = round(p.cpu_percent(interval=2) / 2, 2)
