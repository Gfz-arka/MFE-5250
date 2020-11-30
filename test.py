import datetime
import math
import os
import matplotlib.pyplot as plt
import time
import pandas as pd

from event import OrderEvent
from backtest import Backtest
from data import HistoricCSVDataHandler
from execution import SimulatedExecutionHandler
from portfolio import Portfolio
from Moving_average_cross_strategy import MovingAverageCrossStrategy
from Test_strategy import TestStrategy
from factor_test import FactorTest


class MyPortfolio(Portfolio):

    def generate_naive_order(self, signal, N=1):

        order = None
        date_time = signal.date_time
        symbol = signal.symbol
        direction = signal.signal_type
        strength = signal.strength
        order_price = signal.order_price
        all_in_cash = self.current_holdings['cash']
        if order_price == 0:
            mkt_quantity = 0
        else:
            mkt_quantity = math.floor((all_in_cash / N) / order_price)  # na值待处理
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'

        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(date_time, symbol, order_type, mkt_quantity, 'BUY', order_price, direction)
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(date_time, symbol, order_type, mkt_quantity, 'SELL', order_price, direction)
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(date_time, symbol, order_type, abs(cur_quantity), 'SELL', order_price, direction)
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(date_time, symbol, order_type, abs(cur_quantity), 'BUY', order_price, direction)

        return order


if __name__ == "__main__":
    start_time = time.process_time()
    path1 = os.path.abspath('.')
    stock_csv_dir = 'stock_price'
    stock_csv_dir = os.path.join(path1, stock_csv_dir)
    factor_csv_dir = 'factors'
    factor_csv_dir = os.path.join(path1, factor_csv_dir)
    symbol_list = os.listdir(stock_csv_dir)
    for i in range(len(symbol_list)):
        symbol_list[i] = symbol_list[i].replace('.csv', '')

    initial_capital = 8000000.0
    heartbeat = 0.0
    stock_num = 80
    start_date = datetime.datetime(2018, 10, 31, 0, 0, 0)
    factor = 'PE'
    layer = 1
    equity_curve = pd.DataFrame
    factortest = FactorTest(stock_csv_dir, factor_csv_dir, symbol_list, initial_capital, stock_num / layer,
                            heartbeat, start_date, factor, layer,  # cur_layer,
                            data_handler_cls=HistoricCSVDataHandler, execution_handler_cls=SimulatedExecutionHandler,
                            portfolio_cls=MyPortfolio, strategy_cls=TestStrategy
                            )
    factortest.run_trading()
    # equity_curve[('layer %s' % cur_layer)] = factortest.factor_equity_curve()

    plt.show()
    end_time = time.process_time()
    print('Running time: %s Seconds' % (end_time - start_time))
