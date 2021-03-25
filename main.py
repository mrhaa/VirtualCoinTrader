#_*_ coding: utf-8 _*_

import sys
import time
import multiprocessing as mp
from timeit import default_timer as timer

from UPbit import API
import Calculator
import Learner

# 로직별 프린트가 필요한 영역 설정
PRINT_BALANCE_STATUS_LOG = True
PRINT_TRADABLE_COINS_LOG = True
TIME_SERIES_DATAS_LOG = False
PROCEDURE_ERR_LOG = True # 메인 프로시저 동작 시 오류
API_ERR_LOG = False # 비스업 API 호출 시 오류

# 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
READ_BALANCES_ONLINE = True # 1. 비트업에서 계좌 정보를 읽음
READ_MARKETS_ONLINE = True # 2. 비트업에서 거래 가능한 종목 리스트업
READ_TIME_SERIES_ONLINE = True # 3. 비트업에서 시계열 데이터 읽음
ANALYZE_DATAS = True # 4. 시그널 생성
TRADE_COIN = False # 5. 시그널에 맞춰 매매

# 옵션한 기능 활성화 설정
EMPTY_ALL_POSITION = False # 모든 포지션 매도 후 프로그램 종료
CALL_TERM_APPLY = False # API 오류 빈도에 따라 루프 주기를 자동 조절
SAVE_SERIES_DATA = False # 학습(머신러닝 or 통계적 모수)을 위해 데이터 파일 저장

TEST = False

LOOP_MAX_NUM = float('inf')

if __name__ == '__main__':

    ############################################################################
    server_url = "https://api.upbit.com"
    upbit = API.UPbitObject(server_url=server_url, SAVE_SERIES_DATA=SAVE_SERIES_DATA, API_PRINT_ERR=API_ERR_LOG)

    (access_key, secret_key) = upbit.set_key()

    sb = Calculator.SendBox()

    ############################################################################
    # 기준 통화(매수에 사용)
    currency = 'KRW'

    # balance 정보에서 인덱스로 사용할 컬럼명
    balances_idx_nm1 = 'unit_currency'
    balances_idx_nm2 = 'currency'
    balances_idx_nm = balances_idx_nm1+'-'+balances_idx_nm2
    max_balances_num = 10

    # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
    coins_idx_nm = 'market'

    # 시계열 정보를 받는 기준
    interval_unit = 'minutes'
    interval_val = '1'
    count = 50 # 최대 200개

    if CALL_TERM_APPLY:
        call_term = 0.05
        call_err_score = 0.0
        call_err_pos_score_threshold = 10
        call_err_neg_score_threshold = -10

    # series 정보에서 인덱스로 사용할 컬럼명
    series_idx_nm = 'candle_date_time_kst'
    short_term = 5
    long_term = 20
    short_term_momentum_threshold = 1.005
    long_term_momentum_threshold = 1.002
    volume_momentum_threshold = 1.01

    # 매매 시 사용 정보
    position_idx_nm = 'balance'
    buy_amount_unit = 10000
    target_profit = 1.015

    ############################################################################
    # Learner를 sub-process로 생성
    nn = Learner.NeuralNet()
    p = mp.Process(target=Learner.LearnerStart, args=(nn,))
    p.start()

    # Learner가 정상적으로 생성되었는지 확인
    if p.is_alive():
        print("Learner is alive")

    ############################################################################
    loop_cnt = 0
    while loop_cnt < LOOP_MAX_NUM:

        start_tm = timer()

        if READ_BALANCES_ONLINE:
            balances = upbit.get_balance_info()
            if balances is False:
                continue
            balances[balances_idx_nm] = balances[[balances_idx_nm1, balances_idx_nm2]].apply('-'.join, axis=1)
            balances.rename(columns={'avg_buy_price': 'avg_price'}, inplace=True)
            balances.set_index(balances_idx_nm, inplace=True)
            sb.set_balances(balances, max_balances_num)
            if PRINT_BALANCE_STATUS_LOG and loop_cnt % 100 == 0:
                print("--------------------- My Balance Status: %s ---------------------"%(loop_cnt))
                for idx, row in enumerate(balances.iterrows()):
                    print(str(idx) + " " + row[0] + ", balacne: "+ row[1]['balance'] + ", avg_price: " + row[1]['avg_price'])
                print("----------------------------------------------------------------")

            if READ_MARKETS_ONLINE:
                coins = upbit.look_up_all_coins()
                coins = coins.loc[[True if currency in market else False for market in coins[coins_idx_nm]]]
                coins.rename(columns={'korean_name': 'kr_nm', 'english_name': 'us_nm'}, inplace=True)
                coins.set_index(coins_idx_nm, inplace=True)
                sb.set_coins(coins)
                if PRINT_TRADABLE_COINS_LOG and loop_cnt % 1000 == 0:
                    print("--------------------- Tradable Coins: %s ---------------------" % (loop_cnt))
                    for idx, row in enumerate(coins.iterrows()):
                        print(str(idx) + " " + row[0] + ", name: " + row[1]['kr_nm'])
                    print("-------------------------------------------------------------")

                # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                for market in coins.index:
                    try:
                        if READ_TIME_SERIES_ONLINE:
                            #last_tick = upbit.get_ticker(market=market)
                            series = upbit.get_candles(market=market, interval_unit=interval_unit, interval_val=interval_val, count=count)
                            if series is False:
                                if CALL_TERM_APPLY:
                                    # 너무 자주 call error 발생 시 주기 확대
                                    call_err_score = call_err_score - 1.0
                                    if call_err_score < call_err_neg_score_threshold:
                                        # 최대 0.1초까지 증가 시킬 수 있음
                                        call_term = min(call_term * 1.1, 0.1)
                                        call_err_score = 0
                                        print("API call term extended to %s"%(round(call_term, 4)))
                                continue
                            else:
                                if CALL_TERM_APPLY:
                                    # call error가 자주 발생하지 않으면 주기 축소
                                    call_err_score = call_err_score + 0.1
                                    if call_err_score > call_err_pos_score_threshold:
                                        call_term = call_term * 0.9
                                        call_err_score = 0
                                        print("API call term reduced to %s" % (round(call_term, 4)))

                            #series[series_idx_nm].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
                            series.rename(columns={'opening_price': 'open', 'high_price': 'high', 'low_price': 'low', 'trade_price': 'close', 'candle_acc_trade_volume': 'volume'}, inplace=True)
                            series.set_index(series_idx_nm, inplace=True)
                            series = series.sort_index()
                            sb.set_series(series)
                            if TIME_SERIES_DATAS_LOG:
                                print(series)
                                for row in series.iterrows():
                                    tm = row[0]
                                    datas = row[1]
                                    print(tm, datas)

                            if ANALYZE_DATAS:
                                # 100: BUY, -100: SELL
                                signal = False

                                # 강제로 모든 포지션을 비우는 경우
                                if EMPTY_ALL_POSITION:
                                    signal = -100
                                else:
                                    # 현금이 최소 단위의 금액 이상 있는 경우 & 해당 코인을 보유하고 있지 않은 경우 BUY 할 수 있음
                                    if market not in sb.balances_list:
                                        if float(sb.balances[position_idx_nm][currency+'-'+currency]) > buy_amount_unit:

                                            # 최대 보유 가능 종류 수량을 넘는 경우
                                            if sb.balances_num > sb.max_balances_num:
                                                #print("현재 %s/%s 포지션 보유중으로 %s 추가 매수 불가"%(sb.balances_num, sb.max_balances_num, market))
                                                continue

                                            # 골든 크로스 BUY 시그널 계산
                                            signal = sb.get_golden_cross_buy_signal(short_term=short_term, long_term=long_term
                                                                            , short_term_momentum_threshold=short_term_momentum_threshold
                                                                            , long_term_momentum_threshold=long_term_momentum_threshold
                                                                            , volume_momentum_threshold=volume_momentum_threshold)

                                    # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                    if market in sb.balances_list:
                                        # 목표한 수익률 달성 시 매도
                                        if sb.series.tail(1)['close'][0]/float(sb.balances['avg_price'][market]) > target_profit and signal != 100:
                                            signal = -100
                                        #else:
                                        #    signal = sb.get_dead_cross_sell_signal(short_term=short_term, long_term=long_term)

                                if TRADE_COIN:
                                    # 매수 시 side='bid', price=매수금액, ord_type='price'
                                    # 매도 시 side='ask', volume=매도수량, ord_type='market'
                                    if signal == 100:
                                        ret = upbit.order(market=market, side='bid', volume=None, price=str(buy_amount_unit), ord_type='price')
                                        sb.update_balances(market, 'bid')
                                        print(market + " 매수 성공") if ret.status_code == 201 else False

                                    elif signal == -100:
                                        # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                        if market in sb.balances_list:
                                            # 매도할 수량 계산(전체)
                                            sell_balance = sb.balances[position_idx_nm][market]
                                            ret = upbit.order(market=market, side='ask', volume=str(sell_balance), price=None, ord_type='market')
                                            sb.update_balances(market, 'ask')
                                            print(market + " 매도 성공") if ret.status_code == 201 else False

                                    # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                    if EMPTY_ALL_POSITION and sb.balances_num == 1:
                                        # Leanrner 프로세스를 끝냄
                                        if p.is_alive():
                                            p.terminate()
                                        sys.exit()

                    except Exception as x:
                        if PROCEDURE_ERR_LOG:
                            print(market, ": ", x.__class__.__name__)

                    if CALL_TERM_APPLY:
                        time.sleep(call_term)

        end_tm = timer()
        # 1 Cycle Finished
        loop_cnt += 1
        print("Finished %s Loop: %s seconds elapsed"%(loop_cnt, round(end_tm-start_tm, 2)))

        if TEST:
            time.sleep(5.0)

    # Leanrner 프로세스를 끝냄
    if p.is_alive():
        p.terminate()
