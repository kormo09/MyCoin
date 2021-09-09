import os
import sys
import sqlite3
import logging
from worker import Worker
from PyQt5 import QtWidgets
from PyQt5.QtCore import Qt
from PyQt5.QtGui import QPalette
from updater_tick import UpdaterTick
from multiprocessing import Process, Queue
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from trader.setting import *
from trader.static import now, strf_time


class Window(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.log = logging.getLogger('Window')
        self.log.setLevel(logging.INFO)
        filehandler = logging.FileHandler(filename=f"{system_path}/Log/T{strf_time('%Y%m%d')}.txt", encoding='utf-8')
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
        self.setGeometry(0, 0, 692, 292)

        self.lg_tabWidget = QtWidgets.QTabWidget(self)
        self.lg_tabWidget.setGeometry(5, 5, 682, 282)
        self.lg_tab = QtWidgets.QWidget()
        self.lg_textEdit = setTextEdit(self.lg_tab)
        self.lg_textEdit.setGeometry(5, 5, 668, 242)
        self.lg_tabWidget.addTab(self.lg_tab, '틱데이터 저장')
        self.info_label = QtWidgets.QLabel(self)
        self.info_label.setGeometry(105, 1, 500, 30)

        self.worker = Worker(windowQ, tick1Q, tick2Q, tick3Q, tick4Q)
        self.worker.data.connect(self.UpdateTexedit)
        self.worker.start()

    def UpdateTexedit(self, msg):
        if '부가정보업데이트' in msg:
            self.UpdateInfo(msg.split(' ')[1])
        else:
            self.lg_textEdit.setTextColor(color_fg_dk)
            self.lg_textEdit.append(f'[{now()}] {msg}')
            self.log.info(f'[{now()}] {msg}')

    def UpdateInfo(self, jcps):
        tickqsize = tick1Q.qsize() + tick2Q.qsize() + tick3Q.qsize() + tick4Q.qsize()
        label01text = f'Data Received - RTJC {jcps}TICKps, Queue size - tickQ {tickqsize}'
        self.info_label.setText(label01text)


class Query:
    def __init__(self, windowQQ, queryQQ):
        self.windowQ = windowQQ
        self.queryQ = queryQQ
        self.Start()

    def Start(self):
        while True:
            dict_df = self.queryQ.get()
            con = sqlite3.connect(db_tick)
            for i, ticker in enumerate(list(dict_df.keys())):
                columns = ['현재가', '고가', '거래대금', '누적거래대금']
                dict_df[ticker][columns] = dict_df[ticker][columns].astype(int)
                columns = ['등락율', '고저평균대비등락율', '체결강도']
                dict_df[ticker][columns] = dict_df[ticker][columns].astype(float)
                dict_df[ticker].to_sql(ticker, con, if_exists='append', chunksize=1000)
                self.windowQ.put(f'시스템 명령 실행 알림 - 틱데이터 저장 중...[{i + 1}/{len(dict_df)}]')
            con.close()


if __name__ == '__main__':
    windowQ, queryQ, tick1Q, tick2Q, tick3Q, tick4Q = Queue(), Queue(), Queue(), Queue(), Queue(), Queue()

    Process(target=Query, args=(windowQ, queryQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick1Q, queryQ, windowQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick2Q, queryQ, windowQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick3Q, queryQ, windowQ), daemon=True).start()
    Process(target=UpdaterTick, args=(tick4Q, queryQ, windowQ), daemon=True).start()

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
