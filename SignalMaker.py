#_*_ coding: utf-8 _*_

import pandas as pd

class SignalMaker:    # 클래스
    def __init__(self):
        print("Generate SignalMaker.")

    def __del__(self):
        print("Destroy SignalMaker.")

    def get_golden_cross_buy_signal(self, dm, short_term, long_term, short_term_momentum_threshold, long_term_momentum_threshold, volume_momentum_threshold):

        # short term & long term 확인
        short_len = min(short_term, dm.series_num)
        long_len = min(long_term, dm.series_num)

        if short_len == long_len:
            return False

        price = dm.series.tail(1)['close'].values[0]
        short_avg_price = dm.series.tail(short_len)['close'].mean()
        long_avg_price = dm.series.tail(long_len)['close'].mean()

        volume = dm.series.tail(1)['volume'].values[0]
        short_avg_volume = dm.series.tail(short_len)['volume'].mean()
        long_avg_volume = dm.series.tail(long_len)['volume'].mean()

        if price > short_avg_price * short_term_momentum_threshold \
                and short_avg_price > long_avg_price * long_term_momentum_threshold \
                and short_avg_volume > long_avg_volume * volume_momentum_threshold:

            return 'BUY'

        else:
            return False

    def get_dead_cross_sell_signal(self, dm, short_term, long_term):

        # short term & long term 확인
        short_len = min(short_term, dm.series_num)
        long_len = min(long_term, dm.series_num)

        if short_len == long_len:
            return False

        price = dm.series.tail(1)['close'].values[0]
        short_avg_price = dm.series.tail(short_len)['close'].mean()
        long_avg_price = dm.series.tail(long_len)['close'].mean()

        volume = dm.series.tail(1)['volume'].values[0]
        short_avg_volume = dm.series.tail(short_len)['volume'].mean()
        long_avg_volume = dm.series.tail(long_len)['volume'].mean()

        if price < short_avg_price and short_avg_price < long_avg_price and short_avg_volume > long_avg_volume:

            return 'SELL'

        else:
            return False