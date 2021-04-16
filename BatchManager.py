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
    def __init__(self, PRINT_BALANCE_STATUS_LOG=True, PRINT_TRADABLE_MARKET_LOG=True, PRINT_DATA_LOG=False, PROCEDURE_ERR_LOG=True, API_ERR_LOG=False):
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
        interval_unit = 'minutes'
        interval_val = '10'
        count = 10

        last_idx_nm1 = 'trade_date_kst'
        last_idx_nm2 = 'trade_time_kst'

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit, interval_val=interval_val, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        ############################################################################


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

                            new_idx = last['trade_date'][0][:4]+'-'+last['trade_date'][0][4:6]+'-'+last['trade_date'][0][-2:]+'T'+last['trade_time_kst'][0][:2]+':'+last['trade_time_kst'][0][2:4]+':'+last['trade_time_kst'][0][-2:]
                            new_low = pd.DataFrame({'market': market, 'open': last['open'][0], 'close': last['close'][0], 'low': last['low'][0], 'high': last['high'][0], 'volume': last['trade_volume'][0]}, columns=series.columns, index=[new_idx])

                            if series is False:
                                print("No data series.")
                                continue
                            else:
                                db.update_prices(market=market, interval_unit=interval_unit, interval_val=interval_val, table_nm='price_spot', seq=loop_cnt, series=new_low, columns=('open', 'close', 'low', 'high', 'volume'))

                                if loop_cnt % 100 == 0:
                                    db.update_prices(market=market, interval_unit=interval_unit, interval_val=interval_val, table_nm='price_hist', seq=loop_cnt, series=series, columns=('open', 'close', 'low', 'high', 'volume'))


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

            #time.sleep(10)

        ############################################################################
        db.disconnect()

    def loop_procedures(self, READ_BALANCE=True, READ_MARKET=True, READ_DATA=True, ANALYZE_DATA=True, TRADE_COIN=False
                        , EMPTY_ALL_POSITION=False, CALL_TERM_APPLY=False, SELL_SIGNAL=False, RE_BID_TYPE='PRICE'
                        , TEST_MARKET=None, loop_num=float('inf')):

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
        interval_unit = 'minutes'
        interval_val = '10'
        count = 200  # 최대 200개

        last_idx_nm1 = 'trade_date_kst'
        last_idx_nm2 = 'trade_time_kst'

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit, interval_val=interval_val, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        ############################################################################
        short_term = 5
        long_term = 7
        short_term_momentum_threshold = 0.98 # 값이 작을 수록 빠르게 진입 & 빠르게 탈출
        long_term_momentum_threshold = 0.97 # 값이 작을 수록 빠르게 진입 & 빠르게 탈출
        volume_momentum_threshold = None # 1.0

        sell_short_term = 5
        sell_long_term = 10

        sm = SignalMaker.SignalMaker()

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_multiple = 5
        buy_amount_unit = 10000.0*buy_amount_multiple
        additional_position_threshold = -0.145

        tm = TradeManager.TradeManager()
        tm.set_api(api=api)
        tm.set_parameters(buy_amount_unit=buy_amount_unit, position_idx_nm=position_idx_nm)

        target_profit = 0.039

        ############################################################################
        bot = Telegram.Telegram()
        bot.get_bot()

        ############################################################################
        if CALL_TERM_APPLY:
            call_term = 0.05
            call_err_score = 0.0
            call_err_pos_score_threshold = 10.0
            call_err_neg_score_threshold = -10.0

        ############################################################################
        playable_market_list = {}
        (markets, markets_num, markets_list) = mm.get_markets_info()
        for market in markets.index:
            (series, series_num) = dm.get_series_info(market=market)
            playable_market_list[market] = (series['volume'][-10:]*series['close'][-10:]).sum()

        ############################################################################


        except_market_list = []
        recently_sold_list = {}
        loop_cnt = 0
        while loop_cnt < loop_num:

            start_tm = timer()

            if READ_BALANCE:

                (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info()

                if READ_MARKET:

                    (markets, markets_num, markets_list) = mm.get_markets_info()

                    # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                    for market in markets.index:

                        # 특정 코인으로 테스트하기 위함
                        if TEST_MARKET is None:
                            pass
                        else:
                            if market == TEST_MARKET:
                                pass
                            else:
                                continue

                        # 매매 성공 시 Telegram 메세지
                        msg = ""
                        trade_cd = 0

                        try:
                            if READ_DATA:

                                # 시장가 취득
                                last = dm.get_last_info(market=market)
                                if last is False:
                                    print("No last data.")
                                    continue

                                (series, series_num) = dm.get_series_info(market=market)
                                playable_market_list = sorted(playable_market_list.items(), key=lambda item: item[1], reverse=True)
                                playable_market_list = {k: v for k, v in playable_market_list}

                                # 최신 데이터에 시장가 적용
                                if 0:
                                    series['close'][-1] = last['close'][0]
                                else:
                                    new_idx = last['trade_date'][0][:4]+'-'+last['trade_date'][0][4:6]+'-'+last['trade_date'][0][-2:]+'T'+last['trade_time_kst'][0][:2]+':'+last['trade_time_kst'][0][2:4]+':'+last['trade_time_kst'][0][-2:]
                                    if datetime.datetime.strptime(new_idx, "%Y-%m-%dT%H:%M:%S") > datetime.datetime.strptime(series.index[-1], "%Y-%m-%dT%H:%M:%S"):
                                        new_low = pd.DataFrame({'market': market, 'close': last['close'][0]}, columns=series.columns, index=[new_idx])
                                        (series, series_num) = dm.append_new_data(market=market, data=new_low)

                                if 0:
                                    Y = list(series['close'][-5:].values)
                                    X = [x for x in range(5)]
                                    print(np.polyfit(X, Y, 1)[0])

                                # 최근 매도한 코인 재매수할 지 판단, 급등 이후 재매수를 통해 물리는 경우 방지
                                if market in recently_sold_list.keys():
                                    # 최근 매도한 마켓 리스트 중 일정 수준 이상 오르지 않았으면 재매수할 수 있음
                                    if RE_BID_TYPE == 'PRICE':

                                        price_lag = 3
                                        surge_rate_limit = 0.10

                                        surge_rate = max(series['close'][-price_lag:])/min(series['close'][-price_lag:])-1
                                        if surge_rate < surge_rate_limit:
                                            print(market + "은 최근 매도 리스트에서 제외(PRICE, %s pro)." % (round(surge_rate*100,2)))
                                            recently_sold_list.pop(market)

                                    # 최근 매도한 마켓 리스트 중 일정 시간이 지나면 재매수할 수 있음
                                    time_lag = 600
                                    if RE_BID_TYPE == 'TIME':
                                        time_lag = 300

                                    if market in recently_sold_list.keys():
                                        if timer()-recently_sold_list[market]['TIME'] > time_lag:
                                            print(market + "은 최근 매도 리스트에서 제외(TIME, %s)."%(time_lag))
                                            recently_sold_list.pop(market)

                                if series is False:
                                    if CALL_TERM_APPLY:
                                        # 너무 자주 call error 발생 시 주기 확대
                                        call_err_score = call_err_score-1.0
                                        if call_err_score < call_err_neg_score_threshold:
                                            # 최대 0.1초까지 증가 시킬 수 있음
                                            call_term = min(call_term*1.1,0.1)
                                            call_err_score = 0
                                            print("API call term extended to %s"%(round(call_term,4)))
                                    print("No data series.")
                                    continue

                                else:
                                    if CALL_TERM_APPLY:
                                        # call error가 자주 발생하지 않으면 주기 축소
                                        call_err_score = call_err_score + 0.1
                                        if call_err_score > call_err_pos_score_threshold:
                                            call_term = call_term * 0.9
                                            call_err_score = 0
                                            print("API call term reduced to %s"%(round(call_term,4)))

                                if ANALYZE_DATA:
                                    # BUY, SELL
                                    signal = False

                                    # 강제로 모든 포지션을 비우는 경우
                                    if EMPTY_ALL_POSITION:
                                        if market in balance_list:
                                            signal = 'SELL'
                                            msg = 'EMPTY_ALL_POSITION %s'%(market)
                                    else:
                                        # 현금이 최소 단위의 금액 이상 있는 경우 BUY 할 수 있음
                                        if float(balance[position_idx_nm][currency+'-'+currency]) > buy_amount_unit:

                                            # 거래량이 많은 약 상위 30% 만 매수 시도
                                            if market in list(playable_market_list.keys())[:40]:

                                                # 최대 보유 가능 종류 수량을 넘는 경우
                                                if balance_num > max_balance_num:

                                                    if loop_cnt%100 == 0:
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
                                                                                                  , base=-0.1)

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
                                                            expected_loss = series['close'][-1]/float(balance['avg_price'][market])-1
                                                            if expected_loss < additional_position_threshold:
                                                                #print('추가 매수 시도:', market, signal, round(expected_loss*100,2), round(series['close'][-1],2), round(series['close'][-short_term:].mean(),2), round(series['close'][-long_term:].mean(),2))
                                                                # 시장 조정 후 골든 크로스로 변경되는 경우 물타기
                                                                if signal == 'BUY':
                                                                    msg = "BUY: loss of %s is %s pro. buy addtional position."%(market, round(expected_loss*100,2))
                                                                    trade_cd = 2

                                                            # 단위 금액보다 적은 금액이 매수된 경우 추가 매수
                                                            if round(buy_amount_unit-float(balance['avg_price'][market])*float(balance['balance'][market]),-4)/10000.0 > 0:
                                                                if signal == 'BUY':
                                                                    msg = "BUY: less of %s is %s won. buy addtional position."%(market, round(buy_amount_unit-series['close'][-1]*float(balance['balance'][market]),-4))
                                                                    trade_cd = 3

                                        # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                        if market in balance_list:

                                            # signal이 발생하거나 매매 처리 예외 리스트에 없는 경우
                                            if market not in except_market_list:

                                                # 물렸던 경우 목표 수익률 보다 낮은 수준에서 차익 실현
                                                profit_multiple = 1.0
                                                if series['close'][-1]*float(balance['balance'][market]) > buy_amount_unit*2:
                                                    profit_multiple = 1.5

                                                # 목표한 수익률 달성 시 매도
                                                expected_profit = (series['close'][-1]/float(balance['avg_price'][market])-1)*profit_multiple
                                                if expected_profit > target_profit:

                                                    # 골든 크로스 BUY 시그널 계산
                                                    if 0:
                                                        signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term
                                                                                                , short_term_momentum_threshold=short_term_momentum_threshold, long_term_momentum_threshold=long_term_momentum_threshold
                                                                                                , volume_momentum_threshold=volume_momentum_threshold, direction=-1)
                                                    else:
                                                        signal = sm.get_momentum_z_buy_signal(series=series, series_num=series_num
                                                                                              , short_term=sell_short_term, long_term=sell_long_term
                                                                                              , base=-0.1)
                                                    #print('수익 실현 시도:', market, signal, round(expected_profit/profit_multiple*100,2), round(series['close'][-1],2), round(series['close'][-sell_short_term:].mean(),2), round(series['close'][-sell_long_term:].mean(),2))

                                                    # 골든 크로스 해지, 정배열이 없어지면 모멘텀이 사라졌다고 판단
                                                    if signal != 'BUY':
                                                        signal = 'SELL'
                                                        msg = "SELL: target profit(%s/%s pro, %s won) of %s is reached."%(round(expected_profit/profit_multiple*100,2), round(target_profit*100,2), round(float(balance['balance'][market])*series['close'][-1]), market)
                                                        trade_cd = -2
                                                    else:
                                                        signal = False

                                                if SELL_SIGNAL:

                                                    signal = sm.get_dead_cross_sell_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term)
                                                    if signal == 'SELL':
                                                        expected_profit = float(balance['avg_price'][market])/series['close'][-1]-1
                                                        msg = "SELL: dead_cross of %s(%s pro)"%(market, round(expected_profit*100,2))
                                                        trade_cd = -1

                                        if 0:
                                            # 특정 가격 보다 작은 종목은 보유하지 않음
                                            if series['close'][-1] < 300.0:
                                                signal = False

                                    if TRADE_COIN:
                                        if signal == 'BUY' or signal == 'SELL':
                                            tm.set_balance(balance)
                                            ret = tm.execute_at_market_price(market=market, signal=signal, trade_cd=trade_cd)
                                            #print("execute return: %s"%(ret))
                                            (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info()
                                            tm.update_balance(balance=balance)

                                            # 매매 내역을 DB에 저장(디버그를 위함)
                                            db.save_signal(market=market, date=series.index[-1][:10], time=series.index[-1][-8:], signal=signal, trade_cd=trade_cd, price=series['close'][-1])

                                            if signal == 'SELL':
                                                recently_sold_list[market] = {'TIME':timer(), 'PRICE':series['close'][-1]}

                                            # 매매 성공
                                            #print(market, ret.text)
                                            if ret.status_code == 201:
                                                bot.send_message(msg)
                                            elif ret.status_code == 400:
                                                pass

                                        # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                        if EMPTY_ALL_POSITION and balance_num == 1:
                                            sys.exit()

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
            loop_cnt += 1

            if loop_cnt % 10 == 0:
                print("Finished %s Loop: %s seconds elapsed"%(loop_cnt, round(end_tm-start_tm,2)))
                print("recently_sold_list: ", recently_sold_list)
                print("playable_market_list(40개): ", list(playable_market_list.keys())[:40])

        ############################################################################
        db.disconnect()

    def algorithm_test(self, algorithm='golden_cross'):

        ############################################################################
        db = DBManager.DBManager()
        db.connet(host="127.0.0.1", port=3306, database="upbit", user="root", password="ryumaria")

        total_profit = 0

        interval_unit = 'minutes'
        interval_val = '1'
        market_list = db.get_market_list()
        for market in market_list:
            df_series = db.get_data_series(market, interval_unit, interval_val)
            #print(df_series)

            df_series['pct'] = 0.0
            df_series['pct_acc'] = 0.0
            for idx in range(len(df_series.index)):
                curr_value = df_series.iloc[idx]['close']
                if idx == 0:
                    first_value = curr_value
                else:
                    df_series['pct'][idx] = curr_value/prev_value-1
                    df_series['pct_acc'][idx] = curr_value/first_value-1
                prev_value = curr_value
                #df_series_transpose = df_series.transpose()

            target_profit = 0.05
            signal = False
            in_value = 0
            if algorithm == 'golden_cross':
                short_term = 5
                long_term = 20
                for idx in range(len(df_series.index) - long_term):
                    curr_value = df_series.iloc[idx+long_term]['close']
                    short_avg = df_series.iloc[idx+(long_term-short_term):idx+long_term]['close'].mean()
                    long_avg = df_series.iloc[idx:idx+long_term]['close'].mean()

                    if curr_value > short_avg and short_avg > long_avg and signal == False:
                        signal = True
                        in_value = curr_value

                    profit = curr_value/in_value-1
                    if profit > target_profit and signal == True:
                        #print(market, 'profit', round(min(profit, target_profit),4))
                        total_profit += min(profit, target_profit)

                        signal = False
                        in_value = 0

                    if idx == len(df_series.index) - long_term - 1 and signal == True:
                        #print(market, 'loss', round(profit-1,4))
                        total_profit += profit-1
        print('total_profit: ', round(total_profit,4))
        """ 
        else:
            cnt = 0
            prev_max_idx = -1
            prev_min_idx = -1
            window_size = 120
            for idx in range(len(df_series.index)-window_size):
                clip = df_series.iloc[idx:idx+window_size]
                max_idx = clip['pct_acc'].idxmax()
                min_idx = clip['pct_acc'].idxmin()

                start_data = df_series.iloc[idx]
                max_data = df_series.iloc[max_idx]
                min_data = df_series.iloc[min_idx]
                end_data = df_series.iloc[idx+window_size]

                max_profit = (1+max_data['pct_acc'])/(1+start_data['pct_acc'])
                if max_profit > target_profit:
                    if prev_max_idx != max_idx:
                        cnt += 1

                        msg = str(cnt)+"\t"+market+"\t"+str(max_idx-idx)+"\t"+start_data['date']+"\t"+start_data['time']+"\t"+max_data['date']+"\t"+max_data['time']+"\t"+end_data['date']+"\t"+end_data['time']+"\t"+str(round(max_profit,4))
                        for row in clip.iterrows():
                            msg = msg+"\t"+str(round(row[1]['pct_acc'],4))
                        #print(msg)
                        prev_max_idx = max_idx
        """

        ############################################################################
        db.disconnect()