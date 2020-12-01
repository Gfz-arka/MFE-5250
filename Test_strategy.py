import numpy as np
import pandas as pd
import datetime
from event import SignalEvent, MarketEvent
from strategy import Strategy


class TestStrategy(Strategy):

    def __init__(self, bars, events, stock_num, factor='close', layer=1):
        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events
        self.stock_num = stock_num
        self.factor = factor
        self.layer = layer
        self.stock_list = pd.DataFrame()
        self.transferring = False  # 调仓中

        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        # 创建初始的买入情况字典。所有的股票买入情况都为'OUT'，即未买入  
        bought = {}
        for s in self.symbol_list:
            bought[s] = 'OUT'
        return bought

    def calculate_stock_selection(self, cur_layer):
        # 计算月底要买入哪些股票
        n = int(self.stock_num / self.layer)  # 买入股票数量        ！！na值待处理！！
        bars = []
        for s in self.symbol_list:
            bars.append(self.bars.get_latest_bar_value(
                s, self.factor))  # 将每支股票的因子放入列表中
        self.stock_list = pd.DataFrame(bars, self.symbol_list, columns=[self.factor])
        self.stock_list = self.stock_list.sort_values(self.factor, ascending=False)  # 按因子值排序
        self.stock_list = self.stock_list[cur_layer * n: (cur_layer + 1) * n].reset_index()['index']  # 把前n支股票放入dataframe中，股票代码作为index

    def calculate_signals(self, event, cur_layer=0):

        if event.type == 'MARKET':
            self.calculate_stock_selection(cur_layer)

            if not self.transferring:
                for s in self.symbol_list:
                    if self.bought[s] == 'LONG':
                        bar_date = self.bars.get_latest_bar_datetime(s)
                        symbol = s
                        dt = datetime.datetime.utcnow()
                        sig_dir = 'EXIT'
                        order_price = self.bars.get_latest_bars_values(s, 'close')
                        signal = SignalEvent(1, bar_date, symbol, dt, sig_dir, order_price, 1.0)
                        self.events.put(signal)
                        self.bought[s] = 'OUT'
                self.transferring = True

            else:
                for s in self.symbol_list:
                    if s in self.stock_list.values and self.bought[s] == "OUT":
                        bar_date = self.bars.get_latest_bar_datetime(s)
                        symbol = s
                        dt = datetime.datetime.utcnow()
                        sig_dir = 'LONG'
                        order_price = self.bars.get_latest_bars_values(s, 'close')
                        signal = SignalEvent(1, bar_date, symbol, dt, sig_dir, order_price, 1.0)
                        self.events.put(signal)
                        self.bought[s] = "LONG"
                self.transferring = False
