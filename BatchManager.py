#_*_ coding: utf-8 _*_

import time
import datetime
from timeit import default_timer as timer

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
        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        market_idx_nm = 'market'

        mm = MarketManager.MarketManager(self.PRINT_TRADABLE_MARKET_LOG)
        mm.set_api(api=api)
        mm.set_parameters(currency=currency, market_idx_nm=market_idx_nm)

        ############################################################################
        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        interval_unit = 'minutes'
        interval_val = '1'
        count = 200  # 최대 200개

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit, interval_val=interval_val, count=count, series_idx_nm=series_idx_nm)

        ############################################################################
        if READ_MARKET:

            (markets, markets_num, markets_list) = mm.get_markets_info()
            db.update_markets(markets, ('kr_nm', 'us_nm'))

            loop_cnt = 0
            while loop_cnt < loop_num:

                start_tm = timer()

                # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                for market in markets.index:

                    try:
                        if READ_DATA:
                            # 데이터의 시점을 정하고 데이터 요청하는 로직은 동작하지 않는 것으로 테스트 됨
                            if 0:
                                first_point = db.get_first_point(market=market, interval_unit=interval_unit, interval_val=interval_val)
                                (series, series_num) = dm.get_series_info(market=market, to=first_point)
                            else:
                                (series, series_num) = dm.get_series_info(market)
                            db.update_series(market, interval_unit, interval_val, series, ('open', 'close', 'low', 'high', 'volume'))

                    except Exception as x:
                        if self.PROCEDURE_ERR_LOG:
                            print(market, ": ", x.__class__.__name__)

                end_tm = timer()
                elapsed_time = end_tm-start_tm
                sleep_time = int(count*0.9*60.0-elapsed_time)
                # 1 Cycle Finished
                loop_cnt += 1
                print("Finished %s's %s Loop: %s seconds elapsed & sleep %s seconds" % (market, loop_cnt, round(elapsed_time, 2), sleep_time))

                time.sleep(sleep_time)

        ############################################################################
        db.disconnect()

    def loop_procedures(self, READ_BALANCE=True, READ_MARKET=True, READ_DATA=True, ANALYZE_DATA=True, TRADE_COIN=False
                        , EMPTY_ALL_POSITION=False, CALL_TERM_APPLY=False, SELL_SIGNAL=False, loop_num=float('inf')):

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
        interval_val = '1'
        count = 20  # 최대 200개

        last_idx_nm1 = 'trade_date_kst'
        last_idx_nm2 = 'trade_time_kst'

        dm = DataManager.DataManager(self.PRINT_DATA_LOG)
        dm.set_api(api=api)
        dm.set_parameters_for_series(interval_unit=interval_unit, interval_val=interval_val, count=count, series_idx_nm=series_idx_nm, last_idx_nm1=last_idx_nm1, last_idx_nm2=last_idx_nm2)

        ############################################################################
        short_term = 5
        long_term = 20
        short_term_momentum_threshold = 1.0 #1.005
        long_term_momentum_threshold = 1.0 #1.002
        volume_momentum_threshold = None #1.01

        sell_short_term = 3
        sell_long_term = 10

        sm = SignalMaker.SignalMaker()

        ############################################################################
        # 매매 시 사용 정보
        position_idx_nm = 'balance'
        buy_amount_unit = 10000
        additional_position_threshold = 0.85

        tm = TradeManager.TradeManager()
        tm.set_api(api=api)
        tm.set_parameters(buy_amount_unit=buy_amount_unit, position_idx_nm=position_idx_nm)

        target_profit = 1.04

        ############################################################################
        bot = Telegram.Telegram()
        bot.get_bot()

        ############################################################################
        if CALL_TERM_APPLY:
            call_term = 0.05
            call_err_score = 0.0
            call_err_pos_score_threshold = 10
            call_err_neg_score_threshold = -10

        ############################################################################

        except_market_list = []
        loop_cnt = 0
        while loop_cnt < loop_num:

            start_tm = timer()

            if READ_BALANCE:

                (balance, balance_num, balance_list, max_balance_num) = bm.get_balance_info()

                if READ_MARKET:

                    (markets, markets_num, markets_list) = mm.get_markets_info()

                    # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                    for market in markets.index:

                        # 매매 성공 시 Telegram 메세지
                        msg = ""
                        trade_cd = 0

                        try:
                            if READ_DATA:

                                # 시장가 취득
                                last = dm.get_last_info(market=market)
                                if last is False:
                                    continue

                                (series, series_num) = dm.get_series_info(market=market)
                                # 최신 데이터에 시장가 적
                                series['close'][-1] = last['close'][0]

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
                                        # 현금이 최소 단위의 금액 이상 있는 경우 BUY 할 수 있음
                                        if float(balance[position_idx_nm][currency+'-'+currency]) > buy_amount_unit:

                                            # 최대 보유 가능 종류 수량을 넘는 경우
                                            if balance_num > max_balance_num:
                                                #print("현재 %s/%s 포지션 보유중으로 %s 추가 매수 불가"%(balance_num, max_balance_num, market))
                                                pass
                                            else:
                                                # signal이 발생하거나 매매 처리 예외 리스트에 없는 경우
                                                if market not in except_market_list:
                                                    # 골든 크로스 BUY 시그널 계산
                                                    signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=short_term, long_term=long_term
                                                                                            , short_term_momentum_threshold=short_term_momentum_threshold, long_term_momentum_threshold=long_term_momentum_threshold
                                                                                            , volume_momentum_threshold=volume_momentum_threshold)

                                                    # 해당 코인을 보유하고 있지 않은 경우 매수
                                                    if market not in balance_list:
                                                        if signal == 'BUY':
                                                            msg = "BUY, golden_cross of %s"%(market)
                                                            trade_cd = 1
                                                    else:
                                                        # 손실률이 기준 이하인 경우 추가 매수
                                                        expected_loss = series.tail(1)['close'][0]/float(balance['avg_price'][market])
                                                        if expected_loss < additional_position_threshold:
                                                            # 시장 조정 후 골든 크로스로 변경되는 경우 물타기
                                                            if signal == 'BUY':
                                                                msg = "BUY, loss of %s is %s" % (market, round(expected_loss*100,2))
                                                                trade_cd = 2

                                        # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                        if market in balance_list:

                                            # signal이 발생하거나 매매 처리 예외 리스트에 없는 경우
                                            if market not in except_market_list:
                                                # 목표한 수익률 달성 시 매도
                                                if series.tail(1)['close'][0] / float(balance['avg_price'][market]) > target_profit:

                                                    # 골든 크로스 BUY 시그널 계산
                                                    signal = sm.get_golden_cross_buy_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term
                                                                                            , short_term_momentum_threshold=short_term_momentum_threshold, long_term_momentum_threshold=long_term_momentum_threshold
                                                                                            , volume_momentum_threshold=volume_momentum_threshold)

                                                    # 골든 크로스 해지, 정배열이 없어지면 모멘텀이 사라졌다고 판단
                                                    if signal != 'BUY':
                                                        signal = 'SELL'
                                                        msg = "SELL, target profit(%s) of %s is reached."%(target_profit, market)
                                                        trade_cd = -2
                                                    else:
                                                        signal = False

                                                if SELL_SIGNAL:
                                                    signal = sm.get_dead_cross_sell_signal(series=series, series_num=series_num, short_term=sell_short_term, long_term=sell_long_term)

                                                    if signal == 'SELL':
                                                        expected_profit = float(balance['avg_price'][market])/series.tail(1)['close'][0]-1
                                                        msg = "SELL, dead_cross of %s(%s)"%(market, round(expected_profit*100,2))
                                                        trade_cd = -1

                                    if TRADE_COIN:
                                        if signal == 'BUY' or signal == 'SELL':
                                            tm.set_balance(balance)
                                            ret = tm.execute_at_market_price(market=market, signal=signal, trade_cd=trade_cd)
                                            #print("execute return: %s"%(ret))
                                            (balance, balance_num, balance_list, max_balance_num) = bm.update_balance_info()
                                            tm.update_balance(balance=balance)

                                            # 매매 성공
                                            if ret.status_code == 201:
                                                bot.send_message(msg)
                                            elif ret.status_code == 400:
                                            #    print(market, except_market_list, ret.text)
                                                pass

                                        # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                        if EMPTY_ALL_POSITION and balance_num == 1:
                                            sys.exit()

                        # 일시적으로 거래가 정지된 마켓은 예외 대상으로 등록
                        except UnboundLocalError:
                            except_market_list.append(market)
                        except Exception as x:
                            if self.PROCEDURE_ERR_LOG:
                                print(market, ": ", x.__class__.__name__)

                        if CALL_TERM_APPLY:
                            time.sleep(call_term)

            end_tm = timer()
            # 1 Cycle Finished
            loop_cnt += 1
            print("Finished %s Loop: %s seconds elapsed" % (loop_cnt, round(end_tm - start_tm, 2)))

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

            target_profit = 1.05
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

                    profit = curr_value / in_value
                    if profit > target_profit and signal == True:
                        #print(market, 'profit', round(min(profit-1, target_profit-1),4))
                        total_profit += min(profit-1, target_profit-1)

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