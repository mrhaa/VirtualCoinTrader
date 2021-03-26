#_*_ coding: utf-8 _*_

import os
import sys
import platform
import multiprocessing as mp
import time


from API import UPbit
import SignalMaker
import Learner
import Util

import BalanceManager
import MarketManager
import DataManager
import TradeManager

# 로직별 프린트가 필요한 영역 설정
PRINT_BALANCE_STATUS_LOG = True
PRINT_TRADABLE_MARKET_LOG = True
PRINT_DATA_LOG = True
PROCEDURE_ERR_LOG = True # 메인 프로시저 동작 시 오류
API_ERR_LOG = False # 비스업 API 호출 시 오류

# 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
READ_BALANCE_ONLINE = True # 1. 비트업에서 계좌 정보를 읽음
READ_MARKET_ONLINE = True # 2. 비트업에서 거래 가능한 종목 리스트업
READ_TIME_SERIES_ONLINE = True # 3. 비트업에서 시계열 데이터 읽음
ANALYZE_DATAS = True # 4. 시그널 생성
TRADE_COIN = False # 5. 시그널에 맞춰 매매

# 옵션한 기능 활성화 설정
EMPTY_ALL_POSITION = False # 모든 포지션 매도 후 프로그램 종료
CALL_TERM_APPLY = False # API 오류 빈도에 따라 루프 주기를 자동 조절
SAVE_SERIES_DATA = True # 학습(머신러닝 or 통계적 모수)을 위해 데이터 파일 저장

TEST = True


base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
if platform.system() == 'Windows':
    key_dir =  '%s\\'%(base_dir)
else:
    key_dir = '%s/'%(base_dir)
learning_data_dir = '%s/LearningDatas/' % (base_dir)

if __name__ == '__main__':

    ############################################################################
    server_url = "https://api.upbit.com"
    api = UPbit.UPbit(server_url=server_url, API_PRINT_ERR=API_ERR_LOG)
    (access_key, secret_key) = api.get_key()

    ############################################################################
    # 기준 통화(매수에 사용)
    currency = 'KRW'

    ############################################################################
    # balance 정보에서 인덱스로 사용할 컬럼명
    balance_idx_nm1 = 'unit_currency'
    balance_idx_nm2 = 'currency'
    balance_idx_nm = balance_idx_nm1+'-'+balance_idx_nm2
    max_balance_num = 10

    bm = BalanceManager.BalanceManager(PRINT_BALANCE_STATUS_LOG)
    bm.set_api(api)
    bm.set_parameters(currency, balance_idx_nm1, balance_idx_nm2, max_balance_num)
    balance = bm.get_balance()

    ############################################################################
    # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
    market_idx_nm = 'market'

    mm = MarketManager.MarketManager(PRINT_TRADABLE_MARKET_LOG)
    mm.set_api(api)
    mm.set_parameters(currency, market_idx_nm)
    markets = mm.get_markets()

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
    count = 200 # 최대 200개

    dm = DataManager.DataManager(PRINT_DATA_LOG)
    dm.set_api(api)
    series = dm.set_parameters_for_series(interval_unit, interval_val, count, series_idx_nm)

    if TEST:
        series = dm.get_series('KRW-BTC')

    ############################################################################
    short_term = 5
    long_term = 20
    short_term_momentum_threshold = 1.005
    long_term_momentum_threshold = 1.002
    volume_momentum_threshold = 1.01

    sm = SignalMaker.SignalMaker()

    if TEST:
        signal = sm.get_golden_cross_buy_signal(dm, short_term, long_term, short_term_momentum_threshold, long_term_momentum_threshold, volume_momentum_threshold)
        print("golden_cross_gignal: ", signal)
        signal = sm.get_dead_cross_sell_signal(dm, short_term, long_term)
        print("dead_cross_gignal: ", signal)

    ############################################################################
    # 매매 시 사용 정보
    position_idx_nm = 'balance'
    buy_amount_unit = 10000

    tm = TradeManager.TradeManager()
    tm.set_api(api)
    tm.set_balance(bm)
    tm.set_parameters(buy_amount_unit, position_idx_nm)
    if TEST:
        ret = tm.execute_at_market_price('KRW-BTC', 'BUY')
        print("BUY return: ", ret)
        ret = tm.execute_at_market_price('KRW-BTC', 'SELL')
        print("SELL return: ", ret)

    exit()










    if CALL_TERM_APPLY:
        call_term = 0.05
        call_err_score = 0.0
        call_err_pos_score_threshold = 10
        call_err_neg_score_threshold = -10

    target_profit = 1.015
