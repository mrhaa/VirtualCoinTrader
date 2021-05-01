#_*_ coding: utf-8 _*_

import BatchManager
import pyupbit

if __name__ == '__main__':

    # 로직별 프린트가 필요한 영역 설정
    PRINT_BALANCE_STATUS_LOG = False
    PRINT_TRADABLE_MARKET_LOG = False
    PRINT_DATA_LOG = False
    PROCEDURE_ERR_LOG = True  # 메인 프로시저 동작 시 오류
    API_ERR_LOG = False  # 비스업 API 호출 시 오류

    batch = BatchManager.BatchManager(PRINT_BALANCE_STATUS_LOG=PRINT_BALANCE_STATUS_LOG, PRINT_TRADABLE_MARKET_LOG=PRINT_TRADABLE_MARKET_LOG, PRINT_DATA_LOG=PRINT_DATA_LOG
                                      , PROCEDURE_ERR_LOG=PROCEDURE_ERR_LOG, API_ERR_LOG=API_ERR_LOG)

    if 0:
        TEST_LOGIC = False
        batch.test(TEST_LOGIC)

    if 1:
        SIMULATION = False  # 시뮬레이션 여부

        # 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
        READ_BALANCE = True  # 1. 비트업에서 계좌 정보를 읽음
        READ_MARKET = True  # 2. 비트업에서 거래 가능한 종목 리스트업
        READ_DATA = True  # 3. 비트업에서 시계열 데이터 읽음
        ANALYZE_DATA = True  # 4. 시그널 생성
        TRADE_COIN = True  # 5. 시그널에 맞춰 매매


        # 옵션한 기능 활성화 설정
        EMPTY_ALL_POSITION = False  # 모든 포지션 매도 후 프로그램 종료
        CALL_TERM_APPLY = False  # API 오류 빈도에 따라 루프 주기를 자동 조절
        SELL_SIGNAL = False
        RE_BID_TYPE = 'PRICE' # 'PRICE' or 'TIME'

        PARAMETERS = {'BM': {}, 'DM': {}, 'SM': {}, 'TM': {}, 'ETC': {}}
        PARAMETERS['BM'] = {'max_balance_num': 200}
        PARAMETERS['DM'] = {'count': 100}
        PARAMETERS['SM'] = {'algorithm': 'z_value', 'base_z_value': 0.0, 'short_term': 10, 'long_term': 20, 'sell_short_term': 7, 'sell_long_term': 15}
        PARAMETERS['TM'] = {'buy_amount_multiple': 10, 'target_profit': 0.025, 'additional_position_threshold': -0.145}
        PARAMETERS['ETC'] = {'max_playable_market': 40, 'market_shock_base_rate': -0.02, 'minimum_price': 500.0, 'current_period': 5, 'market_shock_threshold': 0.2}

        batch.loop_procedures(SIMULATION=SIMULATION, READ_BALANCE=READ_BALANCE, READ_MARKET=READ_MARKET, READ_DATA=READ_DATA, ANALYZE_DATA=ANALYZE_DATA, TRADE_COIN=TRADE_COIN
                              , EMPTY_ALL_POSITION=EMPTY_ALL_POSITION, CALL_TERM_APPLY=CALL_TERM_APPLY, SELL_SIGNAL=SELL_SIGNAL, RE_BID_TYPE=RE_BID_TYPE
                              , PARAMETERS=PARAMETERS
                              )#, TEST_MARKET='KRW-ADA')

    if 0:
        # 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
        READ_MARKET = True  # 1. 비트업에서 거래 가능한 종목 리스트업
        READ_DATA = True  # 2. 비트업에서 시계열 데이터 읽음

        batch.make_db_for_learner(READ_MARKET, READ_DATA)


