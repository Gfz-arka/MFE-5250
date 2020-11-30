# backtest.py

import datetime
import pprint
import queue
import time

from Test_strategy import TestStrategy
from equity_plot import plot_performance
from event import MarketEvent


class Backtest(object):

    def __init__(
            self, stock_csv_dir, factor_csv_dir, symbol_list, initial_capital, stock_num,
            heartbeat, start_date, data_handler_cls,
            execution_handler_cls, portfolio_cls, strategy_cls
    ):

        self.stock_csv_dir = stock_csv_dir
        self.factor_csv_dir = factor_csv_dir
        self.symbol_list = symbol_list
        self.initial_capital = initial_capital
        self.heartbeat = heartbeat
        self.stock_num = stock_num
        self.start_date = start_date

        self.data_handler_cls = data_handler_cls
        self.execution_handler_cls = execution_handler_cls
        self.portfolio_cls = portfolio_cls
        self.strategy_cls = strategy_cls

        self.events = queue.Queue()

        self.signals = 0
        self.orders = 0
        self.fills = 0
        self.num_strats = 1

        from factor_test import FactorTest
        if not isinstance(self, FactorTest):
            self._generate_trading_instances()

    def _generate_trading_instances(self):

        print(
            "Creating DataHandler, Strategy, Portfolio and ExecutionHandler/n"
        )

        self.data_handler = self.data_handler_cls(self.events, self.stock_csv_dir, self.factor_csv_dir,
                                                  self.start_date, self.symbol_list)
        self.strategy = self.strategy_cls(self.data_handler, self.events)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.start_date,
                                            self.initial_capital, self.stock_num)
        self.execution_handler = self.execution_handler_cls(self.events)

    def _run_backtest(self):

        i = 0
        while True:
            i += 1
            print(i)

            if self.data_handler.continue_backtest:
                self.data_handler.update_bars_monthly()
            else:
                break

            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                else:
                    if event is not None:
                        if event.type == 'MARKET':
                            self.strategy.calculate_signals(event)
                            self.portfolio.update_timeindex()
                        elif event.type == 'SIGNAL':
                            self.signals += 1
                            self.portfolio.update_signal(event)
                        elif event.type == 'ORDER':
                            self.orders += 1
                            self.execution_handler.execute_order(event)
                        elif event.type == 'FILL':
                            self.fills += 1
                            self.portfolio.update_fill(event)

                if self.events.empty() and self.strategy.transferring == True:
                    self.events.put(MarketEvent())

            time.sleep(self.heartbeat)

    def _output_performance(self):

        self.portfolio.create_equity_curve_dataframe()

        print("Creating summary stats...")
        stats = self.portfolio.output_summary_stats()

        print("Creating equity curve...")
        print(self.portfolio.equity_curve.tail(10))
        pprint.pprint(stats)

        print("Signals: %s" % self.signals)
        print("Orders: %s" % self.orders)
        print("Fills: %s" % self.fills)
        self.portfolio.equity_curve.to_csv('equity.csv')
        self.execution_handler.execution_records.to_csv('Execution_summary.csv')

    def run_trading(self):

        self._run_backtest()
        self._output_performance()
        my_plot = plot_performance(self.portfolio.equity_curve,
                                   self.data_handler.symbol_data[self.symbol_list[0]],
                                   self.execution_handler.execution_records)
        my_plot.plot_equity_curve()
        # my_plot.plot_stock_curve()
        # my_plot.show_all_plot()
