#_*_ coding: utf-8 _*_

from timeit import default_timer as timer

class BatchManager():
    def __init__(self):
        print("Generate BatchManager.")

        self.balance = None
        self.markets = None
        self.series = None

    def __del__(self):
        print("Destroy BatchManager.")

    def loop_procedures(self, loop_num=float('inf')):

        loop_cnt = 0
        while loop_cnt < loop_num:

            start_tm = timer()

            if READ_BALANCE_ONLINE:

                if READ_MARKET_ONLINE:

                    # 거래가 가능한 종목들을 순차적으로 돌아가며 처리
                    for market in mm.index:
                        try:
                            if READ_TIME_SERIES_ONLINE:
                                # last_tick = api.get_ticker(market=market)

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

                                if SAVE_SERIES_DATA:
                                    Util.save_pickle(file="%s%s_%s_%s_0.pickle" % (
                                    learning_data_dir, series.index[0], series.index[-1], market), obj=series)

                                if ANALYZE_DATAS:
                                    # 100: BUY, -100: SELL
                                    signal = False

                                    # 강제로 모든 포지션을 비우는 경우
                                    if EMPTY_ALL_POSITION:
                                        signal = -100
                                    else:
                                        # 현금이 최소 단위의 금액 이상 있는 경우 & 해당 코인을 보유하고 있지 않은 경우 BUY 할 수 있음
                                        if market not in calculator.balances_list:
                                            if float(calculator.balances[position_idx_nm][
                                                         currency + '-' + currency]) > buy_amount_unit:

                                                # 최대 보유 가능 종류 수량을 넘는 경우
                                                if calculator.balances_num > calculator.max_balances_num:
                                                    # print("현재 %s/%s 포지션 보유중으로 %s 추가 매수 불가"%(calculator.balances_num, calculator.max_balances_num, market))
                                                    continue

                                                # 골든 크로스 BUY 시그널 계산
                                                signal = calculator.get_golden_cross_buy_signal(
                                                    short_term=short_term, long_term=long_term
                                                    , short_term_momentum_threshold=short_term_momentum_threshold
                                                    , long_term_momentum_threshold=long_term_momentum_threshold
                                                    , volume_momentum_threshold=volume_momentum_threshold)

                                        # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
                                        if market in calculator.balances_list:
                                            # 목표한 수익률 달성 시 매도
                                            if calculator.series.tail(1)['close'][0] / float(calculator.balances['avg_price'][market]) > target_profit and signal != 100:
                                                signal = -100
                                            # else:
                                            #    signal = calculator.get_dead_cross_sell_signal(short_term=short_term, long_term=long_term)

                                    if TRADE_COIN:


                                        # 강제로 모든 포지션을 비우고 현금만 남으면 시스템 다운 시킴
                                        if EMPTY_ALL_POSITION and calculator.balances_num == 1:
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
            print("Finished %s Loop: %s seconds elapsed" % (loop_cnt, round(end_tm - start_tm, 2)))

