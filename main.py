#_*_ coding: utf-8 _*_

import BatchManager

if __name__ == '__main__':

    # 로직별 프린트가 필요한 영역 설정
    PRINT_BALANCE_STATUS_LOG = True
    PRINT_TRADABLE_MARKET_LOG = False
    PRINT_DATA_LOG = False
    PROCEDURE_ERR_LOG = True  # 메인 프로시저 동작 시 오류
    API_ERR_LOG = False  # 비스업 API 호출 시 오류

    batch = BatchManager.BatchManager(PRINT_BALANCE_STATUS_LOG, PRINT_TRADABLE_MARKET_LOG, PRINT_DATA_LOG, PROCEDURE_ERR_LOG, API_ERR_LOG)


    TEST_LOGIC = False
    batch.test(TEST_LOGIC)


    batch.make_db_for_learner()

    # 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
    READ_BALANCE = True  # 1. 비트업에서 계좌 정보를 읽음
    READ_MARKET = True  # 2. 비트업에서 거래 가능한 종목 리스트업
    READ_DATA = True  # 3. 비트업에서 시계열 데이터 읽음
    ANALYZE_DATA = True  # 4. 시그널 생성
    TRADE_COIN = False  # 5. 시그널에 맞춰 매매

    # 옵션한 기능 활성화 설정
    EMPTY_ALL_POSITION = False  # 모든 포지션 매도 후 프로그램 종료
    CALL_TERM_APPLY = False  # API 오류 빈도에 따라 루프 주기를 자동 조절

    batch.loop_procedures(READ_BALANCE, READ_MARKET, READ_DATA, ANALYZE_DATA, TRADE_COIN
                          , EMPTY_ALL_POSITION, CALL_TERM_APPLY)


