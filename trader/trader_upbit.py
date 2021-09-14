import os
import sys
import logging
import sqlite3
import pyupbit
import pandas as pd
from PyQt5 import QtCore
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from setting import *
from static import now, timedelta_sec, strf_time, telegram_msg, timedelta_hour, strp_time


class TraderUpbit(QThread):
    data0 = QtCore.pyqtSignal(list)
    data1 = QtCore.pyqtSignal(list)
    data2 = QtCore.pyqtSignal(list)

    def __init__(self, windowQ, workerQ, queryQ, soundQ, stg1Q, stg2Q, stg3Q, stg4Q):
        super().__init__()
        self.log = logging.getLogger('Worker')
        self.log.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{system_path}/log/S{strf_time('%Y%m%d')}.txt", encoding='utf-8')
        self.log.addHandler(filehandler)

        self.windowQ = windowQ
        self.workerQ = workerQ
        self.queryQ = queryQ
        self.soundQ = soundQ
        self.stg1Q = stg1Q
        self.stg2Q = stg2Q
        self.stg3Q = stg3Q
        self.stg4Q = stg4Q

        self.upbit = None                               # 매도수 주문 및 체결 확인용 객체
        self.tickers1 = None                            # 전략 연산 1 프로세스로 보낼 티커 리스트
        self.tickers2 = None                            # 전략 연산 2 프로세스로 보낼 티커 리스트
        self.tickers3 = None                            # 전략 연산 3 프로세스로 보낼 티커 리스트
        self.tickers4 = None                            # 전략 연산 4 프로세스로 보낼 티커 리스트
        self.buy_uuid = None                            # 매수 주문 저장용 list: [티커명, uuid]
        self.sell_uuid = None                           # 매도 주문 저장용 list: [티커명, uuid]
        self.websocketQ = None                          # 실시간데이터 수신용 웹소켓큐

        self.df_cj = pd.DataFrame(columns=columns_cj)   # 체결목록
        self.df_jg = pd.DataFrame(columns=columns_jg)   # 잔고목록
        self.df_tj = pd.DataFrame(columns=columns_tj)   # 잔고평가
        self.df_td = pd.DataFrame(columns=columns_td)   # 거래목록
        self.df_tt = pd.DataFrame(columns=columns_tt)   # 실현손익
        self.str_today = strf_time('%Y%m%d', timedelta_hour(-9))
        self.dict_jcdt = {}                             # 종목별 체결시간 저장용
        self.dict_intg = {
            '예수금': 0,
            '종목당투자금': 0,                            # 종목당 투자금은 int(예수금 / 최대매수종목수)로 계산
            '최대매수종목수': 5,
            '업비트수수료': 0.                            # 0.5% 일경우 0.005로 입력
        }
        self.dict_bool = {
            '모의모드': True                             # 모의모드 False 상태시만 주문 전송
        }
        self.dict_time = {
            '체결확인': now(),                           # 1초 마다 체결 확인용
            '거래정보': now(),                           # UI 갱신용
            '부가정보': now()                            # UI 갱신용
        }

    def run(self):
        self.LoadDatabase()
        self.GetKey()
        self.GetBalances()
        self.Initialization()
        self.EventLoop()

    def LoadDatabase(self):
        """
        프로그램 구동 시 당일 체결목록, 당일 거래목록, 잔고목록을 불러온다.
        체결과 거래목록은 바로 갱신하고 잔고목록은 예수금을 불러온 이후 갱신한다.
        """
        con = sqlite3.connect(db_stg)
        df = pd.read_sql(f"SELECT * FROM chegeollist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_cj = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        df = pd.read_sql(f'SELECT * FROM jangolist', con)
        self.df_jg = df.set_index('index').sort_values(by=['매입금액'], ascending=False)
        df = pd.read_sql(f"SELECT * FROM tradelist WHERE 체결시간 LIKE '{self.str_today}%'", con)
        self.df_td = df.set_index('index').sort_values(by=['체결시간'], ascending=False)
        con.close()

        if len(self.df_cj) > 0:
            self.data0.emit([ui_num['체결목록'], self.df_cj])
        if len(self.df_td) > 0:
            self.data0.emit([ui_num['거래목록'], self.df_td])

    def GetKey(self):
        """
        user.txt 파일에서 업비트 access 키와 secret 키를 읽어 self.upbit 객체 생성
        해당 객체는 매도수 주문 및 체결확인용이다.
        """
        f = open('D:/PythonProjects/MyCoin/trader/user.txt')
        lines = f.readlines()
        access_key = lines[0].strip()
        secret_key = lines[1].strip()
        f.close()
        self.upbit = pyupbit.Upbit(access_key, secret_key)

    def GetBalances(self):
        """ 예수금 조회 및 종목당투자금 계산 """
        if self.dict_bool['모의모드']:
            self.dict_intg['예수금'] = 100000000
        else:
            self.dict_intg['예수금'] = int(float(self.upbit.get_balances()[0][0]['balance']))
        self.dict_intg['종목당투자금'] = int(self.dict_intg['예수금'] / self.dict_intg['최대매수종목수'])

    def Initialization(self, init=False):
        """
        프로그램 구동 시
        - 업비트 원화 시장 티커 리스트 불러오기
        - 전략 연산 프로세스의 관심종목용 딕셔너리 초기화
        날짜 변경 시
        - 기존 웹소켓큐 제거
        - 전일실현손익 저장
        - 체결목록, 거래목록, 실현손익 초기화
        실시간 데이터 수신용 웹소켓큐 생성
        """
        tickers = pyupbit.get_tickers(fiat="KRW")
        self.tickers1 = [ticker for i, ticker in enumerate(tickers) if i % 4 == 0]
        self.tickers2 = [ticker for i, ticker in enumerate(tickers) if i % 4 == 1]
        self.tickers3 = [ticker for i, ticker in enumerate(tickers) if i % 4 == 2]
        self.tickers4 = [ticker for i, ticker in enumerate(tickers) if i % 4 == 3]
        self.stg1Q.put(['관심종목초기화', self.tickers1])
        self.stg2Q.put(['관심종목초기화', self.tickers2])
        self.stg3Q.put(['관심종목초기화', self.tickers3])
        self.stg4Q.put(['관심종목초기화', self.tickers4])
        if init:
            self.websocketQ.terminate()
            self.queryQ.put([self.df_tt, 'totaltradelist', 'append'])
            self.df_cj = pd.DataFrame(columns=columns_cj)
            self.df_td = pd.DataFrame(columns=columns_td)
            self.df_tt = pd.DataFrame(columns=columns_tt)
        self.websocketQ = WebSocketManager('ticker', tickers)

    def EventLoop(self):
        dict_count = 0
        dict_gsjm = {}
        while True:
            """
            UI 갱신용 큐를 감시한다.
            4개의 프로세스로 분리된 관심종목 딕셔너리가 합쳐지면 정렬해서 UI 프로세스로 보낸다.
            리스트 길이 4개짜리는 프로세스 정보 갱신용이다.
            """
            if not self.windowQ.empty():
                data = self.windowQ.get()
                if len(data) == 2:
                    dict_count += 1
                    dict_gsjm.update(data[1])
                    if dict_count == 4:
                        sorted(dict_gsjm)
                        self.data1.emit([data[0], dict_gsjm])
                        dict_gsjm = {}
                        dict_count = 0
                elif len(data) == 4:
                    self.data2.emit([data[0], data[1], data[2], data[3]])

            """
            주문용 큐를 감시한다.
            주문용 큐에 대한 입력은 모두 전략 연산 프로세스에서 이뤄진다.
            """
            if not self.workerQ.empty():
                data = self.workerQ.get()
                if data[0] == '매수':
                    self.Buy(data[1], data[2], data[3])
                elif data[0] == '매도':
                    self.Sell(data[1], data[2], data[3])

            """
            실시간 웹소켓큐로 데이터가 들어오면 우선 티커명, 시간을 뽑아
            티커별 마지막 시간이 저장된 self.dict_jcdt의 시간과 틀리면 전략 연산 프로세스로 데이터를 보낸다. 
            """
            data = self.websocketQ.get()
            ticker = data['code']
            d = data['trade_date']
            t = data['trade_time']
            dt = d + t

            try:
                last_jcdt = self.dict_jcdt[ticker]
            except KeyError:
                last_jcdt = None

            if last_jcdt is None or dt != last_jcdt:
                self.dict_jcdt[ticker] = dt

                c = data['trade_price']
                h = data['high_price']
                low = data['low_price']
                per = round(data['change_rate'] * 100, 2)
                dm = data['acc_trade_price']
                bid = data['acc_bid_volume']
                ask = data['acc_ask_volume']

                uuidnone = self.buy_uuid is None
                injango = ticker in self.df_jg.index
                data = [ticker, c, h, low, per, dm, bid, ask, d, t, uuidnone, injango, self.dict_intg['종목당투자금']]

                if ticker in self.tickers1:
                    self.stg1Q.put(data)
                elif ticker in self.tickers2:
                    self.stg2Q.put(data)
                elif ticker in self.tickers3:
                    self.stg3Q.put(data)
                elif ticker in self.tickers4:
                    self.stg4Q.put(data)

                """ 잔고목록 갱신 및 매도조건 확인 """
                if injango:
                    ch = round(bid / ask * 100, 2)
                    self.UpdateJango(ticker, c, ch)

                """ 날짜 변경시 날짜변수 갱신, 각종목록 및 웹소켓큐 초기화 """
                if d != self.str_today:
                    self.str_today = d
                    self.Initialization(init=True)
                    telegram_msg('관심종목 및 거래정보를 업데이트하였습니다.')

            """
            체결확인, 거래정보, 관심종목 정보는 1초마다 확인 및 갱신되며
            프로세스 정보가 담긴 부가정보는 2초마다 갱신된다.
            """
            if not self.dict_bool['모의모드'] and now() > self.dict_time['체결확인']:
                self.CheckChegeol(ticker, d + t)
                self.dict_time['체결확인'] = timedelta_sec(1)
            if now() > self.dict_time['거래정보']:
                self.UpdateTotaljango()
                self.dict_time['거래정보'] = timedelta_sec(1)
            if now() > self.dict_time['부가정보']:
                self.data2.emit([1, '부가정보업데이트'])
                self.dict_time['부가정보'] = timedelta_sec(2)

    """
    모의모드 시 실제 매도수 주문을 전송하지 않고 바로 체결목록, 잔고목록 등을 갱신한다.
    실매매 시 매도수 아이디 및 티커명을 매도, 매수 구분하여 변수에 저장하고
    해당 변수값이 None이 아닐 경우 get_order 함수로 체결확인을 1초마다 반복실행한다.
    체결이 완료되면 관련목록을 갱신하고 변수값이 다시 None으로 변경된다.
    체결확인 후 잔고목록를 갱신 한 이후에 전략 연산 프로세스로 체결완료 신호를 보낸다.
    모든 목록은 갱신될 때마다 쿼리 프로세스로 보내어 DB에 실시간으로 기록된다.
    매수 주문은 예수금 부족인지 아닌지를 우선 확인하여 예수금 부족일 경우 주문구분을 시드부족으로 체결목록에 기록하고
    전략 연산 프로세스의 주문 리스트 삭제용 매수완료 신호만 보낸다.
    예수금 부족 상태이며 잔고목록에 없는 상태일 경우 전략 프로세스에서 지속적으로 매수 신호가 발생할 수 있다.
    그러므로 재차 시드부족이 발생한 종목은 체결목록에서 마지막 체결시간이 3분이내면 체결목록에 기록하지 않는다.
    """
    def Buy(self, ticker, c, oc):
        if self.buy_uuid is not None:
            self.CompleteSignal('매수완료', ticker)
            return

        if self.dict_intg['예수금'] < c * oc:
            df = self.df_cj[(self.df_cj['주문구분'] == '시드부족') & (self.df_cj.index == ticker)]
            if len(df) == 0 or now() > timedelta_sec(180, strp_time('%Y%m%d%H%M%S%f', df['체결시간'][0])):
                self.UpdateBuy(ticker, c, oc, strf_time('%Y%m%d%H%M%S'), cancle=True)
            self.CompleteSignal('매수완료', ticker)
            return

        dt = strf_time('%Y%m%d%H%M%S')
        if self.dict_bool['모의모드']:
            self.UpdateBuy(ticker, c, oc, dt)
        else:
            ret = self.upbit.buy_market_order(ticker, self.dict_intg['종목당투자금'])
            self.buy_uuid = [ticker, ret[0]['uuid']]
            self.dict_time['체결확인'] = timedelta_sec(1)

    def Sell(self, ticker, c, oc):
        if self.sell_uuid is not None:
            self.CompleteSignal('매도완료', ticker)
            return

        dt = strf_time('%Y%m%d%H%M%S')
        if self.dict_bool['모의모드']:
            self.UpdateSell(ticker, c, oc, dt)
        else:
            ret = self.upbit.sell_market_order(ticker, oc)
            self.sell_uuid = [ticker, ret[0]['uuid']]
            self.dict_time['체결확인'] = timedelta_sec(1)

    def CompleteSignal(self, gubun, ticker):
        if ticker in self.tickers1:
            self.stg1Q.put([gubun, ticker])
        elif ticker in self.tickers2:
            self.stg2Q.put([gubun, ticker])
        elif ticker in self.tickers3:
            self.stg3Q.put([gubun, ticker])
        elif ticker in self.tickers4:
            self.stg4Q.put([gubun, ticker])

    def UpdateJango(self, ticker, c, ch):
        prec = self.df_jg['현재가'][ticker]
        if prec != c:
            bg = self.df_jg['매입금액'][ticker]
            jc = int(self.df_jg['보유수량'][ticker])
            pg, sg, sp = self.GetPgSgSp(bg, jc * c)
            columns = ['현재가', '수익률', '평가손익', '평가금액']
            self.df_jg.at[ticker, columns] = c, sp, sg, pg
            data = [ticker, sp, jc, ch, c]
            if ticker in self.tickers1:
                self.stg1Q.put(data)
            elif ticker in self.tickers2:
                self.stg2Q.put(data)
            elif ticker in self.tickers3:
                self.stg3Q.put(data)
            elif ticker in self.tickers4:
                self.stg4Q.put(data)

    def CheckChegeol(self, ticker, dt):
        if self.buy_uuid is not None and ticker == self.buy_uuid[0]:
            ret = self.upbit.get_order(self.buy_uuid[1])
            if ret is not None and ret['state'] == 'done':
                cp = ret['price']
                cc = ret['executed_volume']
                self.UpdateBuy(ticker, cp, cc, dt)
                self.CompleteSignal('매수완료', ticker)
                self.buy_uuid = None
        if self.sell_uuid is not None and ticker == self.sell_uuid[0]:
            ret = self.upbit.get_order(self.sell_uuid[1])
            if ret is not None and ret['state'] == 'done':
                cp = ret['price']
                cc = ret['executed_volume']
                self.UpdateSell(ticker, cp, cc, dt)
                self.CompleteSignal('매도완료', ticker)
                self.sell_uuid = None

    def UpdateBuy(self, ticker, cp, cc, dt, cancle=False):
        idt = strf_time('%Y%m%d%H%M%S%f')
        order_gubun = '매수' if not cancle else '시드부족'
        self.df_cj.at[dt] = ticker, order_gubun, cc, 0, cp, cp, dt
        self.df_cj.sort_values(by='체결시간', ascending=False, inplace=True)
        if not cancle:
            bg = cp * cc
            pg, sg, sp = self.GetPgSgSp(bg, bg)
            self.dict_intg['예수금'] -= bg
            self.df_jg.at[ticker] = ticker, cp, cp, sp, sg, bg, pg, cc
            self.df_jg.sort_values(by=['매입금액'], ascending=False, inplace=True)
            self.data0.emit([ui_num['체결목록'], self.df_cj])
            self.queryQ.put([self.df_jg, 'jangolist', 'replace'])
            text = f'매매 시스템 체결 알림 - {ticker} {cc}코인 매수'
            self.log.info(f'[{now()}] {text}')
            self.data2.emit([0, text])
            self.soundQ.put(f'{ticker} {cc}코인을 매수하였습니다.')
            telegram_msg(f'매수 알림 - {ticker} {cp} {cc}')
        df = pd.DataFrame([[ticker, order_gubun, cc, 0, cp, cp, dt]], columns=columns_cj, index=[idt])
        self.queryQ.put([df, 'chegeollist', 'append'])

    def UpdateSell(self, ticker, cp, cc, dt):
        bp = self.df_jg['매입가'][ticker]
        bg = bp * cc
        pg, sg, sp = self.GetPgSgSp(bg, cp * cc)
        self.dict_intg['예수금'] += bg + sg
        self.df_jg.drop(index=ticker, inplace=True)
        self.df_cj.at[dt] = ticker, '매도', cc, 0, cp, cp, dt
        self.df_td.at[dt] = ticker, bg, pg, cc, sp, sg, dt
        self.df_td.sort_values(by=['체결시간'], ascending=False, inplace=True)
        tsg = self.df_td['매도금액'].sum()
        tbg = self.df_td['매수금액'].sum()
        tsig = self.df_td[self.df_td['수익금'] > 0]['수익금'].sum()
        tssg = self.df_td[self.df_td['수익금'] < 0]['수익금'].sum()
        sg = self.df_td['수익금'].sum()
        sp = round(sg / tbg * 100, 2)
        tdct = len(self.df_td)
        d = dt[:8]
        self.df_tt = pd.DataFrame([[tdct, tbg, tsg, tsig, tssg, sp, sg]], columns=columns_tt, index=[d])

        self.data0.emit([ui_num['체결목록'], self.df_cj])
        self.data0.emit([ui_num['거래목록'], self.df_td])
        self.data0.emit([ui_num['거래합계'], self.df_tt])
        text = f'매매 시스템 체결 알림 - {ticker} {bp}코인 매도'
        self.log.info(f'[{now()}] {text}')
        self.data2.emit([0, text])
        self.soundQ.put(f'{ticker} {cc}코인을 매도하였습니다.')
        telegram_msg(f'매도 알림 - {ticker} {cp} {cc}')
        telegram_msg(f'손익 알림 - 총매수금액 {tbg}, 총매도금액{tsg}, 수익 {tsig}, 손실 {tssg}, 수익급합계 {sg}')

        idt = strf_time('%Y%m%d%H%M%S%f')
        df = pd.DataFrame([[ticker, '매도', cc, 0, cp, cp, dt]], columns=columns_cj, index=[idt])
        self.queryQ.put([df, 'chegeollist', 'append'])
        df = pd.DataFrame([[ticker, bg, pg, cc, sp, sg, dt]], columns=columns_td, index=[idt])
        self.queryQ.put([df, 'tradelist', 'append'])
        self.queryQ.put([self.df_jg, 'jangolist', 'replace'])

    # noinspection PyMethodMayBeStatic
    def GetPgSgSp(self, bg, cg):
        sfee = cg * self.dict_intg['업비트수수료']
        bfee = bg * self.dict_intg['업비트수수료']
        pg = int(cg - sfee - bfee)
        sg = pg - bg
        sp = round(sg / bg * 100, 2)
        return pg, sg, sp

    def UpdateTotaljango(self):
        if len(self.df_jg) > 0:
            tsg = self.df_jg['평가손익'].sum()
            tbg = self.df_jg['매입금액'].sum()
            tpg = self.df_jg['평가금액'].sum()
            bct = len(self.df_jg)
            tsp = round(tsg / tbg * 100, 2)
            ttg = self.dict_intg['예수금'] + tpg
            self.df_tj.at[self.str_today] = ttg, self.dict_intg['예수금'], bct, tsp, tsg, tbg, tpg
        else:
            self.df_tj.at[self.str_today] = self.dict_intg['예수금'], self.dict_intg['예수금'], 0, 0.0, 0, 0, 0
        self.data0.emit([ui_num['잔고목록'], self.df_jg])
        self.data0.emit([ui_num['잔고평가'], self.df_tj])
