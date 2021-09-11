import os
import sys
import pyupbit
from PyQt5 import QtCore
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.static import now, timedelta_sec


class WebsOrderbook(QThread):
    data = QtCore.pyqtSignal(str)

    def __init__(self, orderQ, tick1Q, tick2Q):
        super().__init__()
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
        self.time_info = now()
        self.int_tick = 0
        self.dict_push = {}

    def run(self):
        self.Initialization()
        self.EventLoop()

    def Initialization(self):
        tickers = pyupbit.get_tickers(fiat="KRW")
        self.tickers1 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 0]
        self.tickers2 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 1]
        self.websocketQ = WebSocketManager('orderbook', tickers)

    def EventLoop(self):
        while True:
            if not self.orderQ.empty():
                data = self.orderQ.get()
                if data[0] == '호가업데이트':
                    self.dict_push[data[1]] = True

            data = self.websocketQ.get()
            self.int_tick += 1
            ticker = data['code']
            try:
                push = self.dict_push[ticker]
            except KeyError:
                push = False
            if push:
                if ticker in self.tickers1:
                    self.tick1Q.put(data)
                elif ticker in self.tickers2:
                    self.tick2Q.put(data)
                self.dict_push[ticker] = False

            if now() > self.time_info:
                self.data.emit(f'오더북부가정보 {self.int_tick}')
                self.int_tick = 0
                self.time_info = timedelta_sec(+1)
