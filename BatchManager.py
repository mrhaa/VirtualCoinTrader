#_*_ coding: utf-8 _*_

import time
import datetime
from timeit import default_timer as timer
import operator
import numpy as np
import pandas as pd

import sys
import multiprocessing as mp

from API import UPbit
from API import Telegram
import BalanceManager
import MarketManager
import DataManager
import SignalMaker
import TradeManager
import Learner
import DBManager


class BatchManager():
    def __init__(self, PRINT_BALANCE_STATUS_LOG=True, PRINT_TRADABLE_MARKET_LOG=True, PRINT_DATA_LOG=False
                 , PROCEDURE_ERR_LOG=True, API_ERR_LOG=False):
        print("Generate BatchManager.")

        self.PRINT_BALANCE_STATUS_LOG = PRINT_BALANCE_STATUS_LOG
        self.PRINT_TRADABLE_MARKET_LOG = PRINT_TRADABLE_MARKET_LOG
        self.PRINT_DATA_LOG = PRINT_DATA_LOG
        self.PROCEDURE_ERR_LOG = PROCEDURE_ERR_LOG
        self.API_ERR_LOG = API_ERR_LOG

    def __del__(self):
        print("Destroy BatchManager.")

    def test(self, TEST_LOGIC=False):

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
        bm.set_api(api=api)
        bm.set_parameters(currency=currency, balance_idx_nm1=balance_idx_nm1, balance_idx_nm2=balance_idx_nm2, max_balance_num=max_balance_num)
        (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info()

        ############################################################################
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG)
        mm.set_api(api=api)
        mm.set_parameters(currency=currency, market_idx_nm=market_idx_nm)
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
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit, interval_val=interval_val, count=count, series_idx_nm=series_idx_nm)

        if TEST_LOGIC:
            (series, series_num) = dm.get_series_info(market='KRW-BTC')

        ############################################################################
        short_term = 5
        long_term = 20
        short_term_momentum_threshold = 1.005
        long_term_momentum_threshold = 1.002
        volume_momentum_threshold = 1.01

        sm = SignalMaker.SignalMaker()

        if TEST_LOGIC:
            signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=short_term, long_term=long_term
                                                    , short_term_momentum_threshold=short_term_momentum_threshold
                                                    , long_term_momentum_threshold=long_term_momentum_threshold
                                                    , volume_momentum_threshold=volume_momentum_threshold)
            print("golden_cross_gignal: ", signal)
            signal = sm.get_dead_cross_sell_signal(series=series, series_num=series_num, short_term=short_term, long_term=long_term)
            print("dead_cross_gignal: ", signal)

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_unit = 10000

        tm = TradeManager.TradeManager()
        tm.set_api(api=api)
        tm.set_parameters(buy_amount_unit=buy_amount_unit, position_idx_nm=position_idx_nm)

        if TEST_LOGIC:
            tm.set_balance(balance=balance)
            ret = tm.execute_at_market_price(market='KRW-BTC', signal='BUY')
            (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info()
            tm.update_balance(balance=balance)
            print("BUY return: ", ret)
            ret = tm.execute_at_market_price(market='KRW-BTC', signal='SELL')
            print("SELL return: ", ret)

        ############################################################################
        bot = Telegram.Telegram()
        bot.get_bot()
        bot.send_message("msg")


    def make_db_for_learner(self, READ_MARKET=True, READ_DATA=True, loop_num=float('inf')):

        ############################################################################
        db = DBManager.DBManager()
        db.connet(host="127.0.0.1", port=3306, database="upbit", user="root", password="ryumaria")

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
        max_balance_num = 200

        bm = BalanceManager.BalanceManager(self.PRINT_BALANCE_STATUS_LOG)
        bm.set_api(api=api)
        bm.set_parameters(currency=currency, balance_idx_nm1=balance_idx_nm1, balance_idx_nm2=balance_idx_nm2, max_balance_num=max_balance_num)

        ############################################################################
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG)
        mm.set_api(api=api)
        mm.set_parameters(currency=currency, market_idx_nm=market_idx_nm)

        ############################################################################
        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        interval_unit1 = 'minutes'
        interval_val1 = '10'
        interval_unit2 = 'minutes'
        interval_val2 = '1'
        count = 100

        last_idx_nm1 = 'trade_date_kst'
        last_idx_nm2 = 'trade_time_kst'

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit1, interval_val=interval_val1, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        dm2 = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm2.set_api(api=api)
        dm2.set_parameters_for_series(interval_unit=interval_unit2, interval_val=interval_val2, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        ############################################################################

        max_no = db.max_no()
        if max_no is None:
            no = 0
        else:
            no = max_no + 1

        loop_cnt = 0
        while loop_cnt < loop_num:

            start_tm = timer()

            if READ_MARKET:

                (markets, markets_num, markets_list) = mm.get_markets_info()

                # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                for market in markets.index:
                    try:
                        if READ_DATA:

                            # 시장가 취득
                            last = dm.get_last_info(market=market)
                            if last is False:
                                print("No last data.")
                                continue

                            (series, series_num) = dm.get_series_info(market=market)
                            (series2, series_num2) = dm2.get_series_info(market=market)

                            new_idx = last['trade_date'][0][:4]+'-'+last['trade_date'][0][4:6]+'-'+last['trade_date'][0][-2:]+'T'+last['trade_time_kst'][0][:2]+':'+last['trade_time_kst'][0][2:4]+':'+last['trade_time_kst'][0][-2:]
                            new_low = pd.DataFrame({'market': market, 'open': last['open'][0], 'close': last['close'][0], 'low': last['low'][0], 'high': last['high'][0], 'volume': last['trade_volume'][0]}, columns=series.columns, index=[new_idx])

                            if series is False:
                                print("No data series.")
                                continue
                            else:
                                db.update_prices(table_nm='price_spot', no=no, seq=loop_cnt, market=market, series=new_low, columns=('open', 'close', 'low', 'high', 'volume'))
                                if loop_cnt % 1000 == 0:
                                    db.update_prices(table_nm='price_hist', no=no, market=market, interval_unit=interval_unit1, interval_val=interval_val1, series=series, columns=('open', 'close', 'low', 'high', 'volume'))
                                if loop_cnt % 100 == 0:
                                    db.update_prices(table_nm='price_hist', no=no, market=market, interval_unit=interval_unit2, interval_val=interval_val2, series=series2, columns=('open', 'close', 'low', 'high', 'volume'))


                    # 일시적으로 거래가 정지된 마켓은 예외 대상으로 등록
                    except UnboundLocalError:
                        #except_market_list.append(market)
                        pass
                    except Exception as x:
                        if self.PROCEDURE_ERR_LOG:
                            print(market, ": ", x.__class__.__name__)

            end_tm = timer()
            # 1 Cycle Finished
            loop_cnt += 1

            if loop_cnt % 100 == 0:
                print("Finished %s Loop: %s seconds elapsed"%(loop_cnt, round(end_tm-start_tm,2)))

            time.sleep(10)

        ############################################################################
        db.disconnect()

    def loop_procedures(self, SIMULATION=False, READ_BALANCE=True, READ_MARKET=True, READ_DATA=True, ANALYZE_DATA=True, TRADE_COIN=False
                        , EMPTY_ALL_POSITION=False, CALL_TERM_APPLY=False, SELL_SIGNAL=False, RE_BID_TYPE='PRICE'
                        , PARAMETERS=None
                        , TEST_MARKET=None, loop_num=float('inf')):

        # 알고리즘을 위한 파라미터가 넘어오지 않는 경우
        if PARAMETERS is None:
            return False

        ############################################################################
        db = DBManager.DBManager()
        db.connet(host="127.0.0.1", port=3306, database="upbit", user="root", password="ryumaria")

        ############################################################################
        if SIMULATION == False:
            server_url = "https://api.upbit.com"
            api = UPbit.UPbit(server_url=server_url, API_PRINT_ERR=self.API_ERR_LOG)
            (access_key, secret_key) = api.get_key()
        else:
            api = None

        ############################################################################
        # 기준 통화(매수에 사용)
        currency = 'KRW'

        ############################################################################
        # balance 정보에서 인덱스로 사용할 컬럼명
        balance_idx_nm1 = 'unit_currency'
        balance_idx_nm2 = 'currency'
        max_balance_num = PARAMETERS['BM']['max_balance_num'] # 200

        bm = BalanceManager.BalanceManager(self.PRINT_BALANCE_STATUS_LOG, SIMULATION)
        bm.set_api(api=api)
        bm.set_db(db=db)
        bm.set_parameters(currency=currency, balance_idx_nm1=balance_idx_nm1, balance_idx_nm2=balance_idx_nm2, max_balance_num=max_balance_num)

        ############################################################################
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG, SIMULATION)
        mm.set_api(api=api)
        mm.set_db(db=db)
        mm.set_parameters(currency=currency, market_idx_nm=market_idx_nm)

        ############################################################################
        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        interval_unit_long = 'minutes'
        interval_val_long = '10'
        interval_unit_short = 'minutes'
        interval_val_short = '1'
        count = PARAMETERS['DM']['count'] # 100  # 최대 200개

        last_idx_nm1 = 'trade_date_kst'
        last_idx_nm2 = 'trade_time_kst'

        dm_long = DataManager.DataManager(self.PRINT_DATA_LOG, SIMULATION)
        dm_long.set_api(api=api)
        dm_long.set_db(db=db)
        dm_long.set_parameters_for_series(interval_unit=interval_unit_long, interval_val=interval_val_long, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        dm_short = DataManager.DataManager(self.PRINT_DATA_LOG, SIMULATION)
        dm_short.set_api(api=api)
        dm_short.set_db(db=db)
        dm_short.set_parameters_for_series(interval_unit=interval_unit_short, interval_val=interval_val_short, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        ############################################################################
        short_term = PARAMETERS['SM']['short_term'] # 5
        long_term = PARAMETERS['SM']['long_term'] # 7
        short_term_momentum_threshold = 0.98 # 값이 작을 수록 빠르게 진입 & 빠르게 탈출
        long_term_momentum_threshold = 0.97 # 값이 작을 수록 빠르게 진입 & 빠르게 탈출
        volume_momentum_threshold = None # 1.0

        sell_short_term = PARAMETERS['SM']['sell_short_term'] # 5
        sell_long_term = PARAMETERS['SM']['sell_long_term'] # 10

        sm = SignalMaker.SignalMaker()

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_multiple = PARAMETERS['TM']['buy_amount_multiple'] # 10
        buy_amount_unit = 10000.0*buy_amount_multiple
        target_profit = PARAMETERS['TM']['target_profit'] # 0.039
        additional_position_threshold = PARAMETERS['TM']['additional_position_threshold'] # -0.145

        tm = TradeManager.TradeManager(SIMULATION)
        tm.set_api(api=api)
        tm.set_db(db=db)
        tm.set_parameters(buy_amount_unit=buy_amount_unit, position_idx_nm=position_idx_nm)

        ############################################################################
        bot = Telegram.Telegram(SIMULATION)
        bot.get_bot()

        ############################################################################
        if CALL_TERM_APPLY:
            call_term = 0.05
            call_err_score = 0.0
            call_err_pos_score_threshold = 10.0
            call_err_neg_score_threshold = -10.0

        ############################################################################
        market_shock = False
        playable_markets_amount = {}
        playable_markets_rate = {}
        max_playable_market = PARAMETERS['ETC']['max_playable_market'] # 40
        current_period = PARAMETERS['ETC']['current_period'] # 3
        (markets, markets_num, markets_list) = mm.get_markets_info()
        for market in markets.index:
            (series, series_num) = dm_short.get_series_info(market=market)
            playable_markets_amount[market] = (series['volume'][-current_period:] * series['close'][-current_period:]).sum()
            playable_markets_rate[market] = series['close'][-1] / series['close'][-current_period] - 1.0

        market_shock_threshold = PARAMETERS['ETC']['market_shock_threshold'] # 0.1
        minimum_price = PARAMETERS['ETC']['minimum_price'] # 300

        ############################################################################

        max_no = 0
        if SIMULATION == True:
            max_no = db.max_no()


        except_market_list = []
        recently_sold_list = {}
        for n in range(max_no + 1):

            if SIMULATION == True:
                loop_num = db.max_seq(no=n)

            loop_cnt = 0
            while loop_cnt <= loop_num:

                start_tm = timer()

                if READ_BALANCE:

                    (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info(True if loop_cnt == 0 else False)

                    if READ_MARKET:

                        (markets, markets_num, markets_list) = mm.get_markets_info(no=n)

                        # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                        for idx_mrk, market in enumerate(markets.index):

                            # 특정 코인으로 테스트하기 위함
                            # 테스트 마켓을 파라미터로 받는 경우는 해당 마켓을 제외하고는 패스
                            if TEST_MARKET is not None:
                                if market != TEST_MARKET:
                                    continue

                            # 매매 성공 시 Telegram 메세지
                            msg = ""
                            trade_cd = 0

                            try:
                                if READ_DATA:

                                    # 시장가 취득
                                    last = dm_short.get_last_info(market=market, no=n, seq=loop_cnt)
                                    if last is False:
                                        print("No last data.")
                                        continue

                                    new_idx = last['trade_date_kst'][0][:4]+'-'+last['trade_date_kst'][0][4:6]+'-'+last['trade_date_kst'][0][-2:]+'T'+last['trade_time_kst'][0][:2]+':'+last['trade_time_kst'][0][2:4]+':'+last['trade_time_kst'][0][-2:]
                                    (series, series_num) = dm_short.get_series_info(market=market, no=n, curr=new_idx)
                                    if loop_cnt % 10 == 0:
                                        # 최근 특정 기간 거래금액 * 수익률이 높은 상위 가상화폐만 거래
                                        playable_markets_amount[market] = (series['volume'][-current_period:] * series['close'][-current_period:]).sum()
                                        playable_markets_rate[market] = series['close'][-1] / series['close'][-current_period] - 1.0

                                        playable_markets_amount = sorted(playable_markets_amount.items(), key=lambda item: item[1], reverse=True)
                                        playable_markets_amount = {k: v for k, v in playable_markets_amount}

                                        # 마켓 쇼크가 발생한 경우 일단 포지션 정리
                                        market_shock_ratio = sum([1 if playable_markets_amount[key]*playable_markets_rate[key] > 0.0 else 0 for key in playable_markets_amount.keys()]) / len(playable_markets_amount)
                                        if market_shock_ratio < market_shock_threshold:
                                            market_shock = True

                                        if idx_mrk == 0:
                                            print('Market Shock Ratio: %s / %s'%(round(market_shock_ratio, 2), market_shock_threshold))

                                    # 최신 데이터에 시장가 적용
                                    if 0:
                                        series['close'][-1] = last['close'][0]
                                    else:
                                        if datetime.datetime.strptime(new_idx, "%Y-%m-%dT%H:%M:%S") > datetime.datetime.strptime(series.index[-1], "%Y-%m-%dT%H:%M:%S"):
                                            new_low = pd.DataFrame({'market': market, 'close': last['close'][0]}, columns=series.columns, index=[new_idx])
                                            (series, series_num) = dm_short.append_new_data(market=market, data=new_low)

                                    if 0:
                                        Y = list(series['close'][-5:].values)
                                        X = [x for x in range(5)]
                                        print(np.polyfit(X, Y, 1)[0])

                                    # 최근 매도한 코인 재매수할 지 판단, 급등 이후 재매수를 통해 물리는 경우 방지
                                    if market in recently_sold_list.keys():
                                        # 최근 매도한 마켓 리스트 중 일정 수준 이상 오르지 않았으면 재매수할 수 있음
                                        if RE_BID_TYPE == 'PRICE':

                                            price_lag = 5
                                            surge_rate_limit = 0.10

                                            surge_rate = series['close'][-1] / min(series['close'][-price_lag:]) - 1.0
                                            if surge_rate < surge_rate_limit:
                                                print("%s은 최근 매도 리스트에서 제외(PRICE, %s pro)."%(market, round(surge_rate*100, 2)))
                                                recently_sold_list.pop(market)

                                        # 최근 매도한 마켓 리스트 중 일정 시간이 지나면 재매수할 수 있음
                                        time_lag = 1200
                                        if RE_BID_TYPE == 'TIME':
                                            time_lag = 600

                                        if market in recently_sold_list.keys():
                                            if timer()-recently_sold_list[market]['TIME'] > time_lag:
                                                print(market + "은 최근 매도 리스트에서 제외(TIME, %s)."%(time_lag))
                                                recently_sold_list.pop(market)

                                    if series is False:
                                        if CALL_TERM_APPLY:
                                            # 너무 자주 call error 발생 시 주기 확대
                                            call_err_score = call_err_score - 1.0
                                            if call_err_score < call_err_neg_score_threshold:
                                                # 최대 0.1초까지 증가 시킬 수 있음
                                                call_term = min(call_term * 1.1, 0.1)
                                                call_err_score = 0
                                                print("API call term extended to %s"%(round(call_term, 4)))
                                        print("No data series.")
                                        continue

                                    else:
                                        if CALL_TERM_APPLY:
                                            # call error가 자주 발생하지 않으면 주기 축소
                                            call_err_score = call_err_score + 0.1
                                            if call_err_score > call_err_pos_score_threshold:
                                                call_term = call_term * 0.9
                                                call_err_score = 0
                                                print("API call term reduced to %s"%(round(call_term, 4)))

                                    if ANALYZE_DATA:
                                        # BUY, SELL
                                        signal = False

                                        # 강제로 모든 포지션을 비우는 경우
                                        if EMPTY_ALL_POSITION or market_shock == True:

                                            if market in balance_list:
                                                signal = 'SELL'

                                                if EMPTY_ALL_POSITION:
                                                    msg = 'EMPTY_ALL_POSITION(%s)'%(market)
                                                elif market_shock == True:
                                                    msg = 'MARKET_SHOCK HAPPENED(%s, %s/%s)'%(market, round(market_shock_ratio, 2), market_shock_threshold)

                                            if market_shock == True:
                                                market_shock = False

                                        else:
                                            # 현금이 최소 단위의 금액 이상 있는 경우 BUY 할 수 있음
                                            if float(balance[position_idx_nm][currency+'-'+currency]) > buy_amount_unit:

                                                # 거래량이 많은 약 상위 30%이고 최근 상승 구간에 있는 코인 분 최근 상승 추세인 경우 매수 시도
                                                if market in list(playable_markets_amount.keys())[:max_playable_market]\
                                                        and playable_markets_rate[market] > 0.0:
                                                        #and playable_markets_amount[market] > 100000000.0:

                                                    # 최대 보유 가능 종류 수량을 넘는 경우
                                                    if balance_num > max_balance_num:

                                                        if loop_cnt % 10 == 0:
                                                            print("현재 %s/%s 포지션 보유중으로 %s 추가 매수 불가"%(balance_num, max_balance_num, market))
                                                        pass

                                                    else:
                                                        # signal이 발생하거나 매매 처리 예외 리스트에 없는 경우
                                                        if market not in except_market_list:

                                                            if 0:
                                                                # 골든 크로스 BUY 시그널 계산
                                                                signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=short_term, long_term=long_term
                                                                                                        , short_term_momentum_threshold=short_term_momentum_threshold, long_term_momentum_threshold=long_term_momentum_threshold
                                                                                                        , volume_momentum_threshold=volume_momentum_threshold, direction=1)
                                                            else:
                                                                signal = sm.get_momentum_z_buy_signal(series=series, series_num=series_num
                                                                                                      , short_term=short_term, long_term=long_term
                                                                                                      , base=0.1)

                                                            # 해당 코인을 보유하고 있지 않은 경우 매수
                                                            if market not in balance_list:

                                                                if signal == 'BUY':
                                                                    # 최근 매도한 마켓의 경우 잠시 동안 매수하지 않음
                                                                    if market in list(recently_sold_list.keys()):
                                                                        signal = False
                                                                    else:
                                                                        msg = "BUY: golden_cross of %s pro"%(market)
                                                                        trade_cd = 1

                                                            else:
                                                                # 손실률이 기준 이하인 경우 추가 매수
                                                                expected_loss = series['close'][-1] / float(balance['avg_price'][market]) - 1.0
                                                                if expected_loss < additional_position_threshold:
                                                                    #print('추가 매수 시도:', market, signal, round(expected_loss*100,2), round(series['close'][-1],2), round(series['close'][-short_term:].mean(),2), round(series['close'][-long_term:].mean(),2))
                                                                    # 시장 조정 후 골든 크로스로 변경되는 경우 물타기
                                                                    if signal == 'BUY':
                                                                        msg = "BUY: loss of %s is %s pro. buy addtional position."%(market, round(expected_loss*100, 2))
                                                                        trade_cd = 2

                                                                # 단위 금액보다 적은 금액이 매수된 경우 추가 매수
                                                                if round(buy_amount_unit - float(balance['avg_price'][market])*float(balance['balance'][market]), -4) / 10000.0 > 0:
                                                                    if signal == 'BUY':
                                                                        msg = "BUY: less of %s is %s won. buy addtional position."%(market, round(buy_amount_unit-series['close'][-1] * float(balance['balance'][market]), -4))
                                                                        trade_cd = 3

                                            # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                            if market in balance_list:

                                                # signal이 발생하거나 매매 처리 예외 리스트에 없는 경우
                                                if market not in except_market_list:

                                                    # 물렸던 경우 목표 수익률 보다 낮은 수준에서 차익 실현
                                                    profit_multiple = 1.0
                                                    if series['close'][-1] * float(balance['balance'][market]) > buy_amount_unit * 2:
                                                        profit_multiple = 1.5

                                                    # 목표한 수익률 달성 시 매도
                                                    expected_profit = (series['close'][-1] / float(balance['avg_price'][market]) - 1.0) * profit_multiple
                                                    if expected_profit > target_profit:

                                                        # 골든 크로스 BUY 시그널 계산
                                                        if 0:
                                                            signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term
                                                                                                    , short_term_momentum_threshold=short_term_momentum_threshold, long_term_momentum_threshold=long_term_momentum_threshold
                                                                                                    , volume_momentum_threshold=volume_momentum_threshold, direction=-1)
                                                        else:
                                                            signal = sm.get_momentum_z_buy_signal(series=series, series_num=series_num
                                                                                                  , short_term=sell_short_term, long_term=sell_long_term
                                                                                                  , base=0.1)
                                                        #print('수익 실현 시도:', market, signal, round(expected_profit/profit_multiple*100,2), round(series['close'][-1],2), round(series['close'][-sell_short_term:].mean(),2), round(series['close'][-sell_long_term:].mean(),2))

                                                        # 골든 크로스 해지, 정배열이 없어지면 모멘텀이 사라졌다고 판단
                                                        if signal != 'BUY':
                                                            signal = 'SELL'
                                                            msg = "SELL: target profit(%s/%s pro, %s won) of %s is reached."%(round(expected_profit/profit_multiple*100, 2), round(target_profit*100,2), round(float(balance['balance'][market]) * series['close'][-1]), market)
                                                            trade_cd = -2
                                                        else:
                                                            signal = False

                                                    if SELL_SIGNAL:
                                                        if 0:
                                                            signal = sm.get_dead_cross_sell_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term)
                                                        else:
                                                            signal = sm.get_momentum_z_sell_signal(series=series, series_num=series_num
                                                                                                  , short_term=sell_short_term, long_term=sell_long_term
                                                                                                  , base=0.0)
                                                        if signal == 'SELL':
                                                            expected_profit = float(balance['avg_price'][market]) / series['close'][-1] - 1.0
                                                            msg = "SELL: dead_cross of %s(%s pro)"%(market, round(expected_profit*100, 2))
                                                            trade_cd = -1

                                            # 특정 가격 보다 작은 종목은 보유하지 않음
                                            if series['close'][-1] < minimum_price:
                                                signal = False

                                        if TRADE_COIN:
                                            if signal == 'BUY' or signal == 'SELL':

                                                tm.set_balance(balance)
                                                ret = tm.execute_at_market_price(market=market, signal=signal, trade_cd=trade_cd)
                                                #print("execute return: %s"%(ret))
                                                (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info(info=ret, curr_price=series['close'][-1])
                                                tm.update_balance(balance=balance)

                                                # 매매 내역을 DB에 저장(디버그를 위함)
                                                if SIMULATION == False:
                                                    db.save_signal(market=market, date=series.index[-1][:10], time=series.index[-1][-8:], signal=signal, trade_cd=trade_cd, price=series['close'][-1])

                                                    # 매매 성공
                                                    # print(market, ret.text)
                                                    if ret.status_code == 201:
                                                        bot.send_message(msg)
                                                    elif ret.status_code == 400:
                                                        pass

                                                if signal == 'SELL':
                                                    recently_sold_list[market] = {'TIME':timer(), 'PRICE':series['close'][-1]}

                                            # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                            if EMPTY_ALL_POSITION and balance_num == 1:
                                                #sys.exit()
                                                break

                                            if SIMULATION == True and idx_mrk == 0 and loop_cnt % 10 == 0:
                                                print("-----------------------My Balance Status (time: %s, loop_cnt: %s, balance_num: %s)----------------------"%(new_idx, loop_cnt, balance_num))
                                                cash_amount = 0.0
                                                asset_amount = 0.0
                                                for idx, row in enumerate(balance.iterrows()):
                                                    #print(str(idx) + " " + row[0] + ", balacne: " + str(row[1]['balance']) + ", avg_price: " + str(row[1]['avg_price']))
                                                    if row[0] == currency+'-'+currency:
                                                        cash_amount += row[1]['balance']
                                                    else:
                                                        asset_amount += row[1]['balance']*db.get_ticker(row[0], loop_cnt)['close'][0]
                                                print("-----------------------Total Amount: %s (Cash: %s, Asset: %s) -------------------------"%(format(round(cash_amount+asset_amount), ','), format(round(cash_amount), ','), format(round(asset_amount), ',')))

                                                if loop_cnt == loop_num:
                                                    #sys.exit()
                                                    break


                            # 일시적으로 거래가 정지된 마켓은 예외 대상으로 등록
                            except UnboundLocalError:
                                #except_market_list.append(market)
                                pass
                            except Exception as x:
                                if self.PROCEDURE_ERR_LOG:
                                    print(market, ": ", x.__class__.__name__)

                            if CALL_TERM_APPLY:
                                time.sleep(call_term)

                end_tm = timer()
                # 1 Cycle Finished

                if SIMULATION == False:
                    if loop_cnt % 10 == 0:
                        print("Finished %s Loop: %s seconds elapsed"%(loop_cnt, round(end_tm-start_tm, 2)))
                        print("current balance num: %s(%s)" % (balance_num, balance_list))

                loop_cnt += 1

        ############################################################################
        db.disconnect()

        return True
