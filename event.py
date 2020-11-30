# event.py


class Event(object):

    pass


class MarketEvent(Event):

    def __init__(self):

        self.type = "MARKET"


class SignalEvent(Event):

    def __init__(self, strategy_id, date_time, symbol, datetime, signal_type, order_price, strength):

        self.strategy_id = strategy_id
        self.date_time = date_time
        self.type = "SIGNAL"
        self.symbol = symbol
        self.datetime = datetime
        self.signal_type = signal_type
        self.strength = strength
        self.order_price = order_price


class OrderEvent(Event):

    def __init__(self, date_time, symbol, order_type, quantity, buy_or_sell, order_price, direction):

        self.date_time = date_time
        self.type = "ORDER"
        self.symbol = symbol
        self.order_type = order_type
        self.quantity = quantity
        self.buy_or_sell = buy_or_sell
        self.direction = direction
        self.order_price = order_price

    def print_order(self):

        print("Order: Symbol:%s, Type=%s, Quantity=%s, Direction=%s, Order_price=%s" %
              (self.symbol, self.order_type, self.quantity, self.direction, self.order_price))


class FillEvent(Event):

    def __init__(self, date_time, symbol, quantity, buy_or_sell,
                 fill_cost, commission=None):

        self.type = "FILL"
        self.date_time = date_time
        self.symbol = symbol
        # self.exchange = exchange
        self.quantity = quantity
        self.buy_or_sell = buy_or_sell
        self.fill_cost = fill_cost

        if commission is None:
            self.commission = self.calculate_ib_commission()
        else:
            self.commission = commission

    def calculate_ib_commission(self):

        full_cost = 1.3
        if self.quantity <= 500:
            full_cost = max(1.3, 0.013 * self.quantity)
        else:
            full_cost = max(1.3, 0.008 * self.quantity)
        # full_cost = min(full_cost, 0.5 / 100.0 * self.quantity * self.fill_cost)
        return full_cost


