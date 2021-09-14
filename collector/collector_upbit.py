import os
import sys
import sqlite3
import logging
import pyupbit
import pandas as pd
from PyQt5 import QtCore
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPalette
from PyQt5.QtCore import QThread
from pyupbit import WebSocketManager
from multiprocessing import Process, Queue
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.setting import *
from trader.static import now, strf_time, timedelta_sec


class Query:
    def __init__(self, windowQQ, queryQQ):
        self.windowQ = windowQQ
        self.queryQ = queryQQ
        self.con = sqlite3.connect(db_tick)
        self.Start()

    def __del__(self):
        self.con.close()

    def Start(self):
        while True:
            dict_df = self.queryQ.get()
            for i, ticker in enumerate(list(dict_df.keys())):
                dict_df[ticker].to_sql(ticker, self.con, if_exists='append', chunksize=1000)
                self.windowQ.put(f'시스템 명령 실행 알림 - 틱데이터 저장 중...[{i + 1}/{len(dict_df)}]')


class UpdaterTick:
    def __init__(self, tickQ, queryQQ, windowQQ):
        self.tickQ = tickQ
        self.queryQ = queryQQ
        self.windowQ = windowQQ

        self.dict_df = {}                   # 틱데이터 저장용 딕셔너리 key: ticker, value: datafame
        self.dict_orderbook = {}            # 오더북 저장용 딕셔너리
        self.time_info = timedelta_sec(60)  # 틱데이터 저장주기 확인용
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

        data.update(self.dict_orderbook[ticker])
        dt = data['trade_date'] + data['trade_time']

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
        self.dict_orderbook[ticker] = {
            'total_ask_size': data['total_ask_size'],
            'total_bid_size': data['total_bid_size'],
            'ask_price_10': data['orderbook_units'][9]['ask_price'],
            'ask_price_9': data['orderbook_units'][8]['ask_price'],
            'ask_price_8': data['orderbook_units'][7]['ask_price'],
            'ask_price_7': data['orderbook_units'][6]['ask_price'],
            'ask_price_6': data['orderbook_units'][5]['ask_price'],
            'ask_price_5': data['orderbook_units'][4]['ask_price'],
            'ask_price_4': data['orderbook_units'][3]['ask_price'],
            'ask_price_3': data['orderbook_units'][2]['ask_price'],
            'ask_price_2': data['orderbook_units'][1]['ask_price'],
            'ask_price_1': data['orderbook_units'][0]['ask_price'],
            'bid_price_1': data['orderbook_units'][0]['bid_price'],
            'bid_price_2': data['orderbook_units'][1]['bid_price'],
            'bid_price_3': data['orderbook_units'][2]['bid_price'],
            'bid_price_4': data['orderbook_units'][3]['bid_price'],
            'bid_price_5': data['orderbook_units'][4]['bid_price'],
            'bid_price_6': data['orderbook_units'][5]['bid_price'],
            'bid_price_7': data['orderbook_units'][6]['bid_price'],
            'bid_price_8': data['orderbook_units'][7]['bid_price'],
            'bid_price_9': data['orderbook_units'][8]['bid_price'],
            'bid_price_10': data['orderbook_units'][9]['bid_price'],
            'ask_size_10': data['orderbook_units'][9]['ask_size'],
            'ask_size_9': data['orderbook_units'][8]['ask_size'],
            'ask_size_8': data['orderbook_units'][7]['ask_size'],
            'ask_size_7': data['orderbook_units'][6]['ask_size'],
            'ask_size_6': data['orderbook_units'][5]['ask_size'],
            'ask_size_5': data['orderbook_units'][4]['ask_size'],
            'ask_size_4': data['orderbook_units'][3]['ask_size'],
            'ask_size_3': data['orderbook_units'][2]['ask_size'],
            'ask_size_2': data['orderbook_units'][1]['ask_size'],
            'ask_size_1': data['orderbook_units'][0]['ask_size'],
            'bid_size_1': data['orderbook_units'][0]['bid_size'],
            'bid_size_2': data['orderbook_units'][1]['bid_size'],
            'bid_size_3': data['orderbook_units'][2]['bid_size'],
            'bid_size_4': data['orderbook_units'][3]['bid_size'],
            'bid_size_5': data['orderbook_units'][4]['bid_size'],
            'bid_size_6': data['orderbook_units'][5]['bid_size'],
            'bid_size_7': data['orderbook_units'][6]['bid_size'],
            'bid_size_8': data['orderbook_units'][7]['bid_size'],
            'bid_size_9': data['orderbook_units'][8]['bid_size'],
            'bid_size_10': data['orderbook_units'][9]['bid_size']
        }


class Window(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.log = logging.getLogger('Window')
        self.log.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{system_path}/log/T{strf_time('%Y%m%d')}.txt", encoding='utf-8')
        self.log.addHandler(filehandler)

        def setTextEdit(tab):
            textedit = QtWidgets.QTextEdit(tab)
            textedit.setReadOnly(True)
            textedit.setVerticalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            textedit.setHorizontalScrollBarPolicy(Qt.ScrollBarAlwaysOff)
            textedit.setStyleSheet(style_bc_dk)
            textedit.setFont(qfont12)
            return textedit

        self.setFont(qfont12)
        self.setWindowFlags(Qt.FramelessWindowHint)
        self.setGeometry(0, 293, 692, 292)

        self.lg_tabWidget = QtWidgets.QTabWidget(self)
        self.lg_tabWidget.setGeometry(5, 5, 682, 282)
        self.lg_tab = QtWidgets.QWidget()
        self.lg_textEdit = setTextEdit(self.lg_tab)
        self.lg_textEdit.setGeometry(5, 5, 668, 242)
        self.lg_tabWidget.addTab(self.lg_tab, '틱데이터 저장')
        self.info_label = QtWidgets.QLabel(self)
        self.info_label.setGeometry(105, 1, 500, 30)

        self.int_orderbook = 0

        tickers = pyupbit.get_tickers(fiat="KRW")
        tickers1 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 0]
        tickers2 = [ticker for i, ticker in enumerate(tickers) if i % 2 == 1]

        self.websticker = WebsTicker(tickers, tickers1, tickers2)
        self.websticker.data.connect(self.UpdateTexedit)
        self.websticker.start()

        self.websorderbook = WebsOrderbook(tickers, tickers1, tickers2)
        self.websorderbook.data.connect(self.UpdateTexedit)
        self.websorderbook.start()

    def UpdateTexedit(self, msg):
        if '부가정보업데이트' in msg:
            jcps = msg.split(' ')[1]
            label01text = f'Data Received - RTJC {jcps}TICKps | RTOB {self.int_orderbook}TICKps | ' \
                          f'Queue size - tickQ {tick1Q.qsize() + tick2Q.qsize()}'
            self.info_label.setText(label01text)
        elif '오더북부가정보' in msg:
            self.int_orderbook = int(msg.split(' ')[1])
        else:
            self.lg_textEdit.setTextColor(color_fg_dk)
            self.lg_textEdit.append(f'[{now()}] {msg}')
            self.log.info(f'[{now()}] {msg}')


class WebsTicker(QThread):
    data = QtCore.pyqtSignal(str)

    def __init__(self, tickers, tickers1, tickers2):
        super().__init__()
        self.tickers = tickers
        self.tickers1 = tickers1
        self.tickers2 = tickers2

    def run(self):
        time_info = now()
        dict_time = {}
        int_tick = 0
        websQ_ticker = WebSocketManager('ticker', self.tickers)
        self.data.emit('실시간 데이터 티커 수신용 웹소켓큐 생성 완료')

        while True:
            if not windowQ.empty():
                data = windowQ.get()
                self.data.emit(data)

            data = websQ_ticker.get()
            int_tick += 1
            ticker = data['code']
            orderQ.put(['호가업데이트', ticker])
            t = data['trade_time']

            try:
                pret = dict_time[ticker]
            except KeyError:
                pret = None

            if pret is None or t != pret:
                dict_time[ticker] = t
                if ticker in self.tickers1:
                    tick1Q.put([data, now()])
                elif ticker in self.tickers2:
                    tick2Q.put([data, now()])

            if now() > time_info:
                self.data.emit(f'부가정보업데이트 {int_tick}')
                int_tick = 0
                time_info = timedelta_sec(+1)


class WebsOrderbook(QThread):
    data = QtCore.pyqtSignal(str)

    def __init__(self, tickers, tickers1, tickers2):
        super().__init__()
        self.tickers = tickers
        self.tickers1 = tickers1
        self.tickers2 = tickers2

    def run(self):
        time_info = now()
        dict_push = {}
        int_tick = 0
        websQ_order = WebSocketManager('orderbook', self.tickers)
        self.data.emit('실시간 데이터 오더북 수신용 웹소켓큐 생성 완료')

        while True:
            if not orderQ.empty():
                data = orderQ.get()
                if data[0] == '호가업데이트':
                    dict_push[data[1]] = True

            data = websQ_order.get()
            int_tick += 1
            ticker = data['code']

            try:
                push = dict_push[ticker]
            except KeyError:
                push = False

            if push:
                if ticker in self.tickers1:
                    tick1Q.put(data)
                elif ticker in self.tickers2:
                    tick2Q.put(data)
                dict_push[ticker] = False

            if now() > time_info:
                self.data.emit(f'오더북부가정보 {int_tick}')
                int_tick = 0
                time_info = timedelta_sec(+1)


if __name__ == '__main__':
    windowQ, orderQ, queryQ, tick1Q, tick2Q = Queue(), Queue(), Queue(), Queue(), Queue()

    Process(target=Query, args=(windowQ, queryQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick1Q, queryQ, windowQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick2Q, queryQ, windowQ), daemon=True).start()

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle('fusion')
    palette = QPalette()
    palette.setColor(QPalette.Window, color_bg_bc)
    palette.setColor(QPalette.Background, color_bg_bc)
    palette.setColor(QPalette.WindowText, color_fg_bc)
    palette.setColor(QPalette.Base, color_bg_bc)
    palette.setColor(QPalette.AlternateBase, color_bg_dk)
    palette.setColor(QPalette.Text, color_fg_bc)
    palette.setColor(QPalette.Button, color_bg_bc)
    palette.setColor(QPalette.ButtonText, color_fg_bc)
    palette.setColor(QPalette.Link, color_fg_bk)
    palette.setColor(QPalette.Highlight, color_fg_bk)
    palette.setColor(QPalette.HighlightedText, color_bg_bk)
    app.setPalette(palette)
    window = Window()
    window.show()
    app.exec_()
