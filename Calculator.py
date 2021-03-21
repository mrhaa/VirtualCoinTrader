#_*_ coding: utf-8 _*_

import pandas as pd

class SendBox:    # 클래스
    def __init__(self):
        self.balances = None
        self.coins = None
        self.series = None

    def set_balances(self, balances, max_balances_num=10):
        self.balances = balances
        self.balances_num = len(self.balances.index)
        self.balances_list = list(self.balances.index)
        self.max_balances_num = max_balances_num

    def set_coins(self, coins):
        self.coins = coins
        self.coins_num = len(self.coins.index)
        self.coins_list = list(self.coins.index)

    def set_series(self, series):
        self.series = series
        self.series_len = len(self.series.index)

    def update_balances(self, market, type):
        if type == 'bid':
            self.balances = self.balances.append(pd.DataFrame(columns=self.balances.columns, index=[market]))
            self.balances_list.append(market)
            self.balances_num += 1
        elif type == 'ask':
            self.balances.drop(index=market, inplace=True)
            self.balances_list.remove(market)
            self.balances_num -= 1


    def get_golden_cross_buy_signal(self, short_term, long_term, short_term_momentum_threshold, long_term_momentum_threshold, volume_momentum_threshold):

        # short term & long term 확인
        short_len = min(short_term, self.series_len)
        long_len = min(long_term, self.series_len)

        if short_len == long_len:
            return False

        price = self.series.tail(1)['close'].values[0]
        short_avg_price = self.series.tail(short_len)['close'].mean()
        long_avg_price = self.series.tail(long_len)['close'].mean()

        volume = self.series.tail(1)['volume'].values[0]
        short_avg_volume = self.series.tail(short_len)['volume'].mean()
        long_avg_volume = self.series.tail(long_len)['volume'].mean()

        if price > short_avg_price * short_term_momentum_threshold \
                and short_avg_price > long_avg_price * long_term_momentum_threshold \
                and short_avg_volume > long_avg_volume * volume_momentum_threshold:

            return 100

        else:
            return False

    def get_dead_cross_sell_signal(self, short_term, long_term):

        # short term & long term 확인
        short_len = min(short_term, self.series_len)
        long_len = min(long_term, self.series_len)

        if short_len == long_len:
            return False

        price = self.series.tail(1)['close'].values[0]
        short_avg_price = self.series.tail(short_len)['close'].mean()
        long_avg_price = self.series.tail(long_len)['close'].mean()

        volume = self.series.tail(1)['volume'].values[0]
        short_avg_volume = self.series.tail(short_len)['volume'].mean()
        long_avg_volume = self.series.tail(long_len)['volume'].mean()

        if price < short_avg_price and short_avg_price < long_avg_price and short_avg_volume > long_avg_volume:

            return -100

        else:
            return False