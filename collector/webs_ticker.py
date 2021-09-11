import os
import sys
import pyupbit
from PyQt5 import QtCore
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.static import now, timedelta_sec


class WebsTicker(QThread):
    data = QtCore.pyqtSignal(str)

    def __init__(self, windowQ, orderQ, tick1Q, tick2Q):
        super().__init__()
        self.windowQ = windowQ
        self.orderQ = orderQ
        self.tick1Q = tick1Q
        self.tick2Q = tick2Q
        self.tickers1 = None
        self.tickers2 = None
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
        tickers = pyupbit.get_tickers(fiat="KRW")
        self.tickers1 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 0]
        self.tickers2 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 1]
        self.data.emit('티커 목록 불러오기 완료')
        self.websocketQ = WebSocketManager('ticker', tickers)
        self.data.emit('실시간 데이터 수신용 웹소켓큐 생성 완료')

    def EventLoop(self):
        while True:
            if not self.windowQ.empty():
                data = self.windowQ.get()
                self.data.emit(data)

            data = self.websocketQ.get()
            self.int_tick += 1
            ticker = data['code']
            self.orderQ.put(['호가업데이트', ticker])
            t = data['trade_time']
            if ticker not in self.dict_time.keys() or t != self.dict_time[ticker]:
                self.dict_time[ticker] = t
                if ticker in self.tickers1:
                    self.tick1Q.put([data, now()])
                elif ticker in self.tickers2:
                    self.tick2Q.put([data, now()])

            if now() > self.time_info:
                self.data.emit(f'부가정보업데이트 {self.int_tick}')
                self.int_tick = 0
                self.time_info = timedelta_sec(+1)
