import os
import sys
import pyupbit
from PyQt5 import QtCore
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.static import now, timedelta_sec


class Worker(QThread):
    data = QtCore.pyqtSignal(str)

    def __init__(self, windowQ, tick1Q, tick2Q, tick3Q, tick4Q):
        super().__init__()
        self.windowQ = windowQ
        self.tick1Q = tick1Q
        self.tick2Q = tick2Q
        self.tick3Q = tick3Q
        self.tick4Q = tick4Q
        self.tickers = None
        self.tickers1 = None
        self.tickers2 = None
        self.tickers3 = None
        self.tickers4 = None
        self.websocketQ = None
        self.dict_time = {}
        self.time_info = now()
        self.int_tick = 0

    def run(self):
        self.Initialization()
        self.EventLoop()

    def Initialization(self):
        self.tickers = pyupbit.get_tickers(fiat="KRW")
        self.data.emit('티커 목록 불러오기 완료')
        self.tickers1 = [ticker for i, ticker in enumerate(self.tickers) if i % 4 == 0]
        self.tickers2 = [ticker for i, ticker in enumerate(self.tickers) if i % 4 == 1]
        self.tickers3 = [ticker for i, ticker in enumerate(self.tickers) if i % 4 == 2]
        self.tickers4 = [ticker for i, ticker in enumerate(self.tickers) if i % 4 == 3]
        self.websocketQ = WebSocketManager('ticker', self.tickers)
        self.data.emit('실시간 데이터 수신용 웹소켓큐 생성 완료')

    def EventLoop(self):
        while True:
            if not self.windowQ.empty():
                data = self.windowQ.get()
                self.data.emit(data)

            data = self.websocketQ.get()
            self.int_tick += 1
            ticker = data['code']
            c = data['trade_price']
            h = data['high_price']
            low = data['low_price']
            per = round(data['change_rate'] * 100, 2)
            dm = int(data['acc_trade_price'] / 1000)
            bid = data['acc_bid_volume']
            ask = data['acc_ask_volume']
            d = data['trade_date']
            t = data['trade_time']
            if ticker not in self.dict_time.keys() or t != self.dict_time[ticker]:
                self.dict_time[ticker] = t
                data = [ticker, c, h, low, per, dm, bid, ask, d, t, now()]
                if ticker in self.tickers1:
                    self.tick1Q.put(data)
                elif ticker in self.tickers2:
                    self.tick2Q.put(data)
                elif ticker in self.tickers3:
                    self.tick3Q.put(data)
                elif ticker in self.tickers4:
                    self.tick4Q.put(data)

            if now() > self.time_info:
                self.data.emit(f'부가정보업데이트 {self.int_tick}')
                self.int_tick = 0
                self.time_info = timedelta_sec(+1)
