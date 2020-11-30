# strategy.py

import datetime
import numpy as np
import pandas as pd
import queue

from abc import ABCMeta, abstractmethod

from event import SignalEvent


class Strategy(object, metaclass=ABCMeta):
    """
    Strategy类是一个抽象类，提供所有后续派生策略处理对象的接口。派生策略类的目标是
    对于给定的代码基于DataHandler对象生成的数据来生成Signal。
    这个类既可以用来处理历史数据，也可以用来处理实际交易数据。只需要将数据存放到
    数据队列当中
    """

    @abstractmethod
    def calculate_signals(self, event):
        """
        提供一种计算信号的机制
        """

        raise NotImplementedError("Should implement calculate_signals()")


class BuyAndHoldStrategy(Strategy):

    def __init__(self, bars, events):

        self.bars = bars
        self.symbol_list = self.bars.symbol_list
        self.events = events

        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):

        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):

        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)
                if bars is not None and bars != []:
                    if self.bought[s] == False:
                        signal = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                        self.events.put(signal)
                        self.bought[s] = True

