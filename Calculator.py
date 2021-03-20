import pandas as pd

class SendBox:    # 클래스
    def __init__(self):
        self.balances = None
        self.coins = None
        self.series = None

    def set_balances(self, balances):
        self.balances = balances

    def set_coins(self, coins):
        self.coins = coins

    def set_series(self, series):
        self.series = series

    def get_goldend_cross_buy_signal(self, market, short_term, long_term, short_term_momentum_threshold, long_term_momentum_threshold, volume_momentum_threshold):
        price = self.series.tail(1)['close'].values[0]
        short_avg_price = self.series.tail(short_term)['close'].mean()
        long_avg_price = self.series.tail(long_term)['close'].mean()

        volume = self.series.tail(1)['volume'].values[0]
        short_avg_volume = self.series.tail(short_term)['volume'].mean()
        long_avg_volume = self.series.tail(long_term)['volume'].mean()

        if price > short_avg_price * short_term_momentum_threshold and short_avg_price > long_avg_price * long_term_momentum_threshold \
                and short_avg_volume > long_avg_volume * volume_momentum_threshold:
            print("Market:" + market + ", Price: " + str(price)
                  + ", Short Momentum:" + str(round(price / short_avg_price, 4))
                  + ", Long Momentum:" + str(round(short_avg_price / long_avg_price, 4))
                  + ", Volume Momentum:" + str(round(short_avg_volume / long_avg_volume, 4)))