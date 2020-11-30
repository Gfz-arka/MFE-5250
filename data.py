# data.py

import datetime
import os
import os.path
import math
from abc import ABCMeta, abstractmethod

import numpy as np
import pandas as pd

from event import MarketEvent


class DataHandler(object):

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bar(self, symbol):
        """
        返回最近更新的数据条目
        """

        raise NotImplementedError("Should implement get_latest_bar()")

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        返回最近的N条数据
        """

        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def get_latest_bar_datetime(self, symbol):
        """
        返回最近数据条目对应的Python datetime对象
        """

        raise NotImplementedError("Should implement get_latest_bar_datetime()")

    @abstractmethod
    def get_latest_bar_value(self, symbol, val_type):
        """
        返回最近的数据条目中的Open,High,Low,Close,Volume或者oi的数据
        """

        raise NotImplementedError("Should implement get_latest_bar_value()")

    @abstractmethod
    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        返回最近的N条数据中的相关数值，如果没有那么多数据
        则返回N-k条数据
        """

        raise NotImplementedError("Should implement get_latest_bars_values()")

    @abstractmethod
    def update_bars(self):
        """
        将最近的数据条目放入到数据序列中，采用元组的格式
        (datetime, open, high, low, close, volume, open interest)
        """

        raise NotImplementedError("Should implement update_bars()")


class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler类用来读取请求的代码的CSV文件，这些CSV文件
    存储在磁盘上，提供了一种类似于实际交易的场景的”最近数据“一种概念。
    """

    def __init__(self, events, stock_csv_dir, factor_csv_dir, factor, start_date, symbol_list):

        self.events = events
        self.stock_csv_dir = stock_csv_dir
        self.factor_csv_dir = factor_csv_dir
        self.factor = factor
        self.start_month = start_date.month
        self.start_date = start_date.strftime("%Y-%m-%d")
        self.symbol_list = symbol_list

        self.symbol_data = {}
        self.symbol_factor = {}
        self.factor_na = 0
        self.latest_symbol_data = {}
        self.continue_backtest = True
        self.bar_index = 0
        self.data_generator = {}
        self.next_month_bar = {}
        self.latest_data_month = start_date.month

        self._open_convert_csv_files()

    def _open_convert_csv_files(self, symbol_list=None):  # 多进程实验
        """
        从数据路径中打开CSV文件，将它们转化为pandas的DataFrame。
        """

        if symbol_list is None:
            symbol_list = self.symbol_list

        comb_index = None
        for s in symbol_list:
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.stock_csv_dir, '%s.csv' % s),
                header=0, index_col=0,
                names=['datetime', 'high', 'low', 'open', 'close']
                # names = ['datetime', 'high', 'low', 'open', 'close', 'volume', 'adj_close']
            ).sort_index()

            self.symbol_factor[s] = pd.read_excel(
                os.path.join(self.factor_csv_dir, '%s.xlsx' % s),
                header=0, index_col=0,
            ).sort_index()
            self.symbol_factor[s].index.name = 'datetime'
            self.symbol_factor[s].index = self.symbol_factor[s].index.to_series().apply(lambda x: x.strftime('%Y-%m-%d'))

            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            self.latest_symbol_data[s] = []
            self.next_month_bar[s] = []

        for s in symbol_list:
            self.symbol_data[s] = self.symbol_data[s].reindex(
                index=comb_index, method='pad')
            self.symbol_data[s] = self.symbol_data[s].fillna(0)
            self.symbol_data[s] = pd.merge(self.symbol_data[s], self.symbol_factor[s], how='left', on='datetime')
            self.symbol_data[s]["Pct_change"] = self.symbol_data[s]["close"].pct_change()
            self.symbol_data[s] = self.symbol_data[s].loc[self.start_date:]
            self.data_generator[s] = self.symbol_data[s].iterrows()

    def reset_latest_data(self, symbol_list=None):

        if symbol_list is None:
            symbol_list = self.symbol_list
        for s in symbol_list:
            self.data_generator[s] = self.symbol_data[s].iterrows()
            self.latest_symbol_data[s] = []
            self.next_month_bar[s] = []
        self.factor_na = 0

    def _get_new_bar(self, symbol):
        """
        从数据集返回最新的数据条目
        """

        for b in self.data_generator[symbol]:
            yield b

    def get_latest_bar(self, symbol):
        """
        从最新的symbol_list中返回最新数据条目
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data set.")
            raise
        else:
            return bars_list[-1]

    def get_latest_bars(self, symbol, N=1):
        """
        从最近的数据列表中获取N条数据，如果没有那么多，则返回N-k条数据
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data")
            raise
        else:
            return bars_list[-N:]

    def get_latest_bar_datetime(self, symbol):
        """
        返回最近的数据条目对应的Python datetime
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That symbol is not available in the historical data")
            raise
        else:
            return bars_list[-1][0]

    def get_latest_bar_value(self, symbol, val_type):
        """
        返回最近的数据pandas Series对象中的Open,High,Low,Close,Volume或OI的值
        """

        try:
            bars_list = self.latest_symbol_data[symbol]
        except KeyError:
            print("That Symbol is not available in the historical data")
            raise
        else:
            return getattr(bars_list[-1][1], val_type)

    def get_latest_bars_values(self, symbol, val_type, N=1):
        """
        返回latest_symbol_list中的最近N条数据，如果没有那么多，返回N-k条
        """

        try:
            bars_list = self.get_latest_bars(symbol, N)
        except KeyError:
            print("That Symbol is not available in the historical data")
            raise
        else:
            return np.array([getattr(b[1], val_type) for b in bars_list])

    def update_bars(self):
        """
        将最近的数据条目放入到latest_symbol_data结构中。
        """

        for s in self.symbol_list:
            try:
                bar = next(self._get_new_bar(s))
            except StopIteration:
                self.continue_backtest = False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        self.events.put(MarketEvent())

    def update_bars_monthly(self):

        prev_month = self.latest_data_month
        flag = 0
        for s in self.symbol_list:

            if self.next_month_bar[s]:
                self.latest_symbol_data[s].append(self.next_month_bar[s])
                self.next_month_bar[s] = []
            else:
                try:
                    bar = next(self._get_new_bar(s))
                except StopIteration:
                    self.continue_backtest = False
                else:
                    if bar is not None:
                        cur_month = datetime.datetime.strptime(bar[0], "%Y-%m-%d").month
                        if cur_month != self.latest_data_month:
                            self.next_month_bar[s] = bar
                            flag = 1
                            if math.isnan(self.latest_symbol_data[s][0][1].loc[self.factor]) \
                                    and self.latest_data_month == self.start_month:
                                self.factor_na += 1

                        elif cur_month == self.latest_data_month:
                            self.latest_symbol_data[s].append(bar)
                        prev_month = cur_month

        self.latest_data_month = prev_month
        if flag == 1:
            self.events.put(MarketEvent())

    '''def update_bars_monthly(self):

        prev_month = self.latest_data_month
        while True:
            flag = 0
            for s in self.symbol_list:

                if self.next_month_bar[s]:
                    self.latest_symbol_data[s].append(self.next_month_bar[s])
                    self.next_month_bar[s] = []
                else:
                    try:
                        bar = next(self._get_new_bar(s))
                    except StopIteration:
                        self.continue_backtest = False
                        flag = 1
                    else:
                        if bar is not None:
                            cur_month = datetime.datetime.strptime(bar[0], "%Y-%m-%d").month
                            if cur_month != self.latest_data_month:
                                self.next_month_bar[s] = bar
                                flag = 1
                                if math.isnan(self.latest_symbol_data[s][0][1].loc[self.factor]) \
                                        and self.latest_data_month == self.start_month:
                                    self.factor_na += 1

                            elif cur_month == self.latest_data_month:
                                self.latest_symbol_data[s].append(bar)
                            prev_month = cur_month

            self.latest_data_month = prev_month
            if flag == 1:
                break
        self.events.put(MarketEvent())'''
