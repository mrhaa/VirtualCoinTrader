#_*_ coding: utf-8 _*_

import time
from timeit import default_timer as timer

import sys
import multiprocessing as mp

from API import UPbit
import BalanceManager
import MarketManager
import DataManager
import SignalMaker
import TradeManager
import Learner
import DBManager


class BatchManager():
    def __init__(self, PRINT_BALANCE_STATUS_LOG, PRINT_TRADABLE_MARKET_LOG, PRINT_DATA_LOG, PROCEDURE_ERR_LOG, API_ERR_LOG):
        print("Generate BatchManager.")

        self.PRINT_BALANCE_STATUS_LOG = PRINT_BALANCE_STATUS_LOG
        self.PRINT_TRADABLE_MARKET_LOG = PRINT_TRADABLE_MARKET_LOG
        self.PRINT_DATA_LOG = PRINT_DATA_LOG
        self.PROCEDURE_ERR_LOG = PROCEDURE_ERR_LOG
        self.API_ERR_LOG = API_ERR_LOG


    def __del__(self):
        print("Destroy BatchManager.")

    def test(self, TEST_LOGIC):

        ############################################################################
        server_url = "https://api.upbit.com"
        api = UPbit.UPbit(server_url=server_url, API_PRINT_ERR=self.API_ERR_LOG)
        (access_key, secret_key) = api.get_key()

        ############################################################################
        # 기준 통화(매수에 사용)
        currency = 'KRW'

        ############################################################################
        # balance 정보에서 인덱스로 사용할 컬럼명
        balance_idx_nm1 = 'unit_currency'
        balance_idx_nm2 = 'currency'
        max_balance_num = 10

        bm = BalanceManager.BalanceManager(self.PRINT_BALANCE_STATUS_LOG)
        bm.set_api(api)
        bm.set_parameters(currency, balance_idx_nm1, balance_idx_nm2, max_balance_num)
        (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info()

        ############################################################################
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG)
        mm.set_api(api)
        mm.set_parameters(currency, market_idx_nm)
        (markets, markets_num, markets_list) = mm.get_markets_info()

        ############################################################################
        # Learner를 sub-process로 생성
        nn = Learner.NeuralNet()
        p = mp.Process(target=Learner.LearnerStart, args=(nn,))
        p.start()

        # Learner가 정상적으로 생성되었는지 확인 후 끝냄
        if p.is_alive():
            print("Learner is alive")
            p.terminate()

        ############################################################################
        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        interval_unit = 'minutes'
        interval_val = '1'
        count = 200  # 최대 200개

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api)
        dm.set_parameters_for_series(interval_unit, interval_val, count, series_idx_nm)

        if TEST_LOGIC:
            (series, series_num) = dm.get_series_info('KRW-BTC')

        ############################################################################
        short_term = 5
        long_term = 20
        short_term_momentum_threshold = 1.005
        long_term_momentum_threshold = 1.002
        volume_momentum_threshold = 1.01

        sm = SignalMaker.SignalMaker()

        if TEST_LOGIC:
            signal = sm.get_golden_cross_buy_signal(series, series_num, short_term, long_term, short_term_momentum_threshold,
                                                    long_term_momentum_threshold, volume_momentum_threshold)
            print("golden_cross_gignal: ", signal)
            signal = sm.get_dead_cross_sell_signal(series, series_num, short_term, long_term)
            print("dead_cross_gignal: ", signal)

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_unit = 10000

        tm = TradeManager.TradeManager()
        tm.set_api(api)
        tm.set_parameters(buy_amount_unit, position_idx_nm)

        if TEST_LOGIC:
            tm.set_balance(balance)
            ret = tm.execute_at_market_price('KRW-BTC', 'BUY')
            (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info()
            tm.update_balance(balance)
            print("BUY return: ", ret)
            ret = tm.execute_at_market_price('KRW-BTC', 'SELL')
            print("SELL return: ", ret)

    def make_db_for_learner(self):

        db = DBManager.DBManager()
        db.connet(host="127.0.0.1", port=3306, database="upbit", user="root", password="ryumaria")

        db.disconnect()

    def loop_procedures(self, READ_BALANCE=True, READ_MARKET=True, READ_DATA=True, ANALYZE_DATA=True, TRADE_COIN=False
                        , EMPTY_ALL_POSITION=False, CALL_TERM_APPLY=False, loop_num=float('inf')):

        ############################################################################
        server_url = "https://api.upbit.com"
        api = UPbit.UPbit(server_url=server_url, API_PRINT_ERR=self.API_ERR_LOG)
        (access_key, secret_key) = api.get_key()

        ############################################################################
        # 기준 통화(매수에 사용)
        currency = 'KRW'

        ############################################################################
        # balance 정보에서 인덱스로 사용할 컬럼명
        balance_idx_nm1 = 'unit_currency'
        balance_idx_nm2 = 'currency'
        max_balance_num = 10

        bm = BalanceManager.BalanceManager(self.PRINT_BALANCE_STATUS_LOG)
        bm.set_api(api)
        bm.set_parameters(currency, balance_idx_nm1, balance_idx_nm2, max_balance_num)

        ############################################################################
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG)
        mm.set_api(api)
        mm.set_parameters(currency, market_idx_nm)

        ############################################################################
        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        interval_unit = 'minutes'
        interval_val = '1'
        count = 200  # 최대 200개

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api)
        dm.set_parameters_for_series(interval_unit, interval_val, count, series_idx_nm)

        ############################################################################
        short_term = 5
        long_term = 20
        short_term_momentum_threshold = 1.005
        long_term_momentum_threshold = 1.002
        volume_momentum_threshold = 1.01

        sm = SignalMaker.SignalMaker()

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_unit = 10000

        tm = TradeManager.TradeManager()
        tm.set_api(api)
        tm.set_parameters(buy_amount_unit, position_idx_nm)

        target_profit = 1.015

        ############################################################################
        if CALL_TERM_APPLY:
            call_term = 0.05
            call_err_score = 0.0
            call_err_pos_score_threshold = 10
            call_err_neg_score_threshold = -10

        ############################################################################

        loop_cnt = 0
        while loop_cnt < loop_num:

            start_tm = timer()

            if READ_BALANCE:

                (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info()

                if READ_MARKET:

                    (markets, markets_num, markets_list) = mm.get_markets_info()

                    # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                    for market in markets.index:

                        try:
                            if READ_DATA:

                                (series, series_num) = dm.get_series_info(market)

                                if series is False:
                                    if CALL_TERM_APPLY:
                                        # 너무 자주 call error 발생 시 주기 확대
                                        call_err_score = call_err_score - 1.0
                                        if call_err_score < call_err_neg_score_threshold:
                                            # 최대 0.1초까지 증가 시킬 수 있음
                                            call_term = min(call_term * 1.1, 0.1)
                                            call_err_score = 0
                                            print("API call term extended to %s" % (round(call_term, 4)))
                                    continue
                                else:
                                    if CALL_TERM_APPLY:
                                        # call error가 자주 발생하지 않으면 주기 축소
                                        call_err_score = call_err_score + 0.1
                                        if call_err_score > call_err_pos_score_threshold:
                                            call_term = call_term * 0.9
                                            call_err_score = 0
                                            print("API call term reduced to %s" % (round(call_term, 4)))

                                if ANALYZE_DATA:
                                    # BUY, SELL
                                    signal = False

                                    # 강제로 모든 포지션을 비우는 경우
                                    if EMPTY_ALL_POSITION:
                                        if market in balance_list:
                                            signal = 'SELL'
                                    else:
                                        # 현금이 최소 단위의 금액 이상 있는 경우 & 해당 코인을 보유하고 있지 않은 경우 BUY 할 수 있음
                                        if market not in balance_list:
                                            if float(balance[position_idx_nm][currency+'-'+currency]) > buy_amount_unit:

                                                # 최대 보유 가능 종류 수량을 넘는 경우
                                                if balance_num > max_balance_num:
                                                    print("현재 %s/%s 포지션 보유중으로 %s 추가 매수 불가"%(balance_num, max_balance_num, market))
                                                    continue

                                                # 골든 크로스 BUY 시그널 계산
                                                signal = sm.get_golden_cross_buy_signal(series, series_num, short_term=short_term, long_term=long_term
                                                    , short_term_momentum_threshold=short_term_momentum_threshold
                                                    , long_term_momentum_threshold=long_term_momentum_threshold
                                                    , volume_momentum_threshold=volume_momentum_threshold)
                                                if signal is not False:
                                                    print("golden_cross_signal of %s: %s"%(market, signal))

                                        # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                        else:
                                            # 목표한 수익률 달성 시 매도
                                            if series.tail(1)['close'][0] / float(balance['avg_price'][market]) > target_profit and signal != 100:
                                                signal = 'SELL'
                                                print("target profit(%s) of %s reached."%(market, target_profit))
                                            else:
                                                signal = sm.get_dead_cross_sell_signal(series, series_num, short_term=short_term, long_term=long_term)
                                                if signal is not False:
                                                    print("dead_cross_signal of %s: %s"%(market, signal))

                                    if TRADE_COIN:
                                        if signal is not False:
                                            tm.set_balance(balance)
                                            ret = tm.execute_at_market_price(market, signal)
                                            print("execute return: %s"%(ret))
                                            (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info()
                                            tm.update_balance(balance)

                                        # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                        if EMPTY_ALL_POSITION and balance_num == 1:
                                            sys.exit()

                        except Exception as x:
                            if self.PROCEDURE_ERR_LOG:
                                print(market, ": ", x.__class__.__name__)

                        if CALL_TERM_APPLY:
                            time.sleep(call_term)

            end_tm = timer()
            # 1 Cycle Finished
            loop_cnt += 1
            print("Finished %s Loop: %s seconds elapsed" % (loop_cnt, round(end_tm - start_tm, 2)))

