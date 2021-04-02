#_*_ coding: utf-8 _*_

import pandas as pd

class SignalMaker:    # 클래스
    def __init__(self):
        print("Generate SignalMaker.")

    def __del__(self):
        print("Destroy SignalMaker.")

    def get_golden_cross_buy_signal(self, series, series_num, short_term, long_term, short_term_momentum_threshold, long_term_momentum_threshold, volume_momentum_threshold=None, direction=1):

        # short term & long term 확인
        short_len = min(short_term, series_num)
        long_len = min(long_term, series_num)

        if short_len == long_len:
            return False

        price = series.tail(1)['close'].values[0]
        short_avg_price = series.tail(short_len)['close'].mean()
        long_avg_price = series.tail(long_len)['close'].mean()

        volume = series.tail(1)['volume'].values[0]
        short_avg_volume = series.tail(short_len)['volume'].mean()
        long_avg_volume = series.tail(long_len)['volume'].mean()

        # 신규 포지션을 위해 골든 크로스 확인하는 경우: 1 or 이익 실현을 위해 골든 크로스 확인하는 경우: -1
        # 두 경우를 반대로 적용하면 더 빠르게 반응할 수 있다.
        short_multiple = short_term_momentum_threshold if direction == 1 else 1/short_term_momentum_threshold
        long_multiple = long_term_momentum_threshold if direction == 1 else 1/long_term_momentum_threshold
        if price > short_avg_price*short_multiple and short_avg_price > long_avg_price*long_multiple:
            if volume_momentum_threshold is None or short_avg_volume > long_avg_volume*volume_momentum_threshold:
                return 'BUY'
            else:
                return False
        else:
            return False

    def get_dead_cross_sell_signal(self, series, series_num, short_term, long_term):

        # short term & long term 확인
        short_len = min(short_term, series_num)
        long_len = min(long_term, series_num)

        if short_len == long_len:
            return False

        price = series.tail(1)['close'].values[0]
        short_avg_price = series.tail(short_len)['close'].mean()
        long_avg_price = series.tail(long_len)['close'].mean()

        volume = series.tail(1)['volume'].values[0]
        short_avg_volume = series.tail(short_len)['volume'].mean()
        long_avg_volume = series.tail(long_len)['volume'].mean()

        if price < short_avg_price and short_avg_price < long_avg_price and short_avg_volume > long_avg_volume:
            return 'SELL'
        else:
            return False
