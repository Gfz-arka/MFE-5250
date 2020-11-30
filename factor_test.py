import queue
import time

from backtest import Backtest
from equity_plot import plot_performance
from event import MarketEvent
import matplotlib.pyplot as plt

'''如果拆成 backtest 和 factortest 两套，需要加入判断功能的参数'''


class FactorTest(Backtest):

    def __init__(
            self, stock_csv_dir, factor_csv_dir, symbol_list, initial_capital, stock_num,
            heartbeat, start_date, factor, layer,
            data_handler_cls, execution_handler_cls, portfolio_cls, strategy_cls
    ):
        super().__init__(stock_csv_dir, factor_csv_dir, symbol_list, initial_capital, stock_num,
                         heartbeat, start_date, data_handler_cls,
                         execution_handler_cls, portfolio_cls, strategy_cls)
        self.factor = factor
        self.layer = layer
        # self.cur_layer = cur_layer
        self._generate_trading_instances()

    def _generate_trading_instances(self):

        print(
            "Creating DataHandler, Strategy, Portfolio and ExecutionHandler/n"
        )

        self.data_handler = self.data_handler_cls(self.events, self.stock_csv_dir, self.factor_csv_dir, self.factor,
                                                  self.start_date, self.symbol_list)
        self.strategy = self.strategy_cls(self.data_handler, self.events, self.stock_num, self.factor, self.layer)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.start_date,
                                            self.initial_capital, self.stock_num)
        self.execution_handler = self.execution_handler_cls(self.events)

    def _reset_class(self):

        self.strategy = self.strategy_cls(self.data_handler, self.events, self.stock_num, self.factor, self.layer)
        self.portfolio = self.portfolio_cls(self.data_handler, self.events, self.start_date,
                                            self.initial_capital, self.stock_num)
        self.execution_handler = self.execution_handler_cls(self.events)

    def _run_factortest(self, cur_layer):

        i = 0
        while True:
            i += 1
            print(i)

            if self.data_handler.continue_backtest:
                self.data_handler.update_bars_monthly()
                self.portfolio.update_timeindex()
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
                            self.strategy.calculate_signals(event, cur_layer)
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

    def run_trading(self):

        for cur_layer in range(self.layer):
            self._run_factortest(cur_layer)
            self._output_performance()
            my_plot = plot_performance(self.portfolio.equity_curve,
                                       self.data_handler.symbol_data[self.symbol_list[0]],
                                       self.execution_handler.execution_records)
            my_plot.plot_equity_curve()
            self.data_handler.reset_latest_data()
            self._reset_class()
            self.data_handler.continue_backtest = True

    def factor_equity_curve(self):

        return self.portfolio.equity_curve['equity_curve']

    '''def run_trading(self):

        self._run_factortest()
        self._output_performance()
        my_plot = plot_performance(self.portfolio.equity_curve,
                                   self.data_handler.symbol_data[self.symbol_list[0]],
                                   self.execution_handler.execution_records)
        my_plot.plot_equity_curve()'''
