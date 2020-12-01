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
            self.calculate_stock_selection(cur_layer) # 调用计算函数

            if not self.transferring:
                for s in self.symbol_list:  # 遍历所有股票
                    if self.bought[s] == 'LONG':  # 如果持有
                        bar_date = self.bars.get_latest_bar_datetime(s)  # 获取目前日期
                        symbol = s
                        dt = datetime.datetime.utcnow()  # 获取当前现实时间（目前框架里还没用到）
                        sig_dir = 'EXIT'  # 指令为清仓
                        order_price = self.bars.get_latest_bars_values(s, 'close')  # 交易价格，用收盘价表示
                        signal = SignalEvent(1, bar_date, symbol, dt, sig_dir, order_price, 1.0)  # 抛出清仓信号事件
                        self.events.put(signal)  # 将事件放入队列中 （循环后队列中应该多出n个清仓信号）
                        self.bought[s] = 'OUT'  # 买入情况变为'OUT'
                self.transferring = True  # 调仓中，transferring为True时不会进行下一天的循环

            else:
                for s in self.symbol_list:
                    if s in self.stock_list.values and self.bought[s] == "OUT":  # 股票在待买入列表中
                        bar_date = self.bars.get_latest_bar_datetime(s)
                        symbol = s
                        dt = datetime.datetime.utcnow()
                        sig_dir = 'LONG'  # 指令为买入
                        order_price = self.bars.get_latest_bars_values(s, 'close')
                        signal = SignalEvent(1, bar_date, symbol, dt, sig_dir, order_price, 1.0)  # 抛出买入信号事件
                        self.events.put(signal)  # 将事件放入队列中（循环后队列中应该多出n个买入信号）
                        self.bought[s] = "LONG"  # 买入情况变为'LONG'
                self.transferring = False  # 调仓结束，transferring为False，信号处理结束后正常进行下一天的循环
