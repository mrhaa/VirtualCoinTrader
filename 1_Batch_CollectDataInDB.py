#_*_ coding: utf-8 _*_

import BatchManager

if __name__ == '__main__':
    # 로직별 프린트가 필요한 영역 설정
    PRINT_BALANCE_STATUS_LOG = True
    PRINT_TRADABLE_MARKET_LOG = False
    PRINT_DATA_LOG = False
    PROCEDURE_ERR_LOG = True  # 메인 프로시저 동작 시 오류
    API_ERR_LOG = False  # 비스업 API 호출 시 오류
    SIMULATION = False  # 시뮬레이션 여부

    batch = BatchManager.BatchManager(PRINT_BALANCE_STATUS_LOG=PRINT_BALANCE_STATUS_LOG, PRINT_TRADABLE_MARKET_LOG=PRINT_TRADABLE_MARKET_LOG, PRINT_DATA_LOG=PRINT_DATA_LOG
                                      , PROCEDURE_ERR_LOG=PROCEDURE_ERR_LOG, API_ERR_LOG=API_ERR_LOG, SIMULATION=SIMULATION)

    # 프로시저 작업 활성화 단계 설정(전 단계가 True인 경우 다음 단계에가 활성화될 수 있음)
    READ_MARKET = True  # 1. 비트업에서 거래 가능한 종목 리스트업
    READ_DATA = True  # 2. 비트업에서 시계열 데이터 읽음

    batch.make_db_for_learner(READ_MARKET, READ_DATA)
