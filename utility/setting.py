from PyQt5.QtGui import QFont, QColor
import os

#system_path = 'D:/PythonProjects/MyCoin'
system_path = ('/').join((os.getcwd().split('\\'))[:-1])
db_path = f'{system_path}/database'
db_stg = f'{db_path}/stg.db'
db_tick = f'{db_path}/tick1.db'
db_backtest = f'{db_path}/backtest.db'

qfont12 = QFont()
qfont12.setFamily('나눔고딕')
qfont12.setPixelSize(12)

color_fg_bt = QColor(230, 230, 235)
color_fg_bc = QColor(190, 190, 195)
color_fg_dk = QColor(150, 150, 155)
color_fg_bk = QColor(110, 110, 115)

color_bg_bc = QColor(40, 40, 45)
color_bg_dk = QColor(30, 30, 35)
color_bg_bk = QColor(20, 20, 25)

style_bc_bt = 'background-color: rgb(50, 50, 55);'
style_bc_dk = 'background-color: rgb(30, 30, 35);'

ui_num = {'거래합계': 1, '거래목록': 2, '잔고평가': 3, '잔고목록': 4, '체결목록': 5, '관심종목': 6,
          '당일합계': 7, '당일상세': 8, '누적합계': 9, '누적상세': 10}

columns_cj = ['종목명', '주문구분', '주문수량', '미체결수량', '주문가격', '체결가', '체결시간']
columns_jg = ['종목명', '매입가', '현재가', '수익률', '평가손익', '매입금액', '평가금액', '보유수량']
columns_tj = ['추정예탁자산', '추정예수금', '보유종목수', '수익률', '총평가손익', '총매입금액', '총평가금액']
columns_td = ['종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금', '체결시간']
columns_tt = ['거래횟수', '총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']
columns_gj1 = ['현재가', '등락율', '고저평균대비등락율', '거래대금', '누적거래대금', '체결강도', '최고체결강도']
columns_gj2 = ['ticker', 'c', 'per', 'hlmp', 'sm', 'avg_sm', 'dm', 'ch', 'avg_ch', 'hch']

columns_ln = ['기간', '누적매수금액', '누적매도금액', '누적수익금액', '누적손실금액', '수익률', '누적수익금']
columns_lt = ['일자', '총매수금액', '총매도금액', '총수익금액', '총손실금액', '수익률', '수익금합계']
columns_sn = ['거래일자', '누적매수금액', '누적매도금액', '누적수익금액', '누적손실금액', '수익률', '누적수익금']
columns_st = ['체결시간', '종목명', '매수금액', '매도금액', '주문수량', '수익률', '수익금']
