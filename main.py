import time
from UPbit import API

PRINT_BALANCE_STATUS_LOG = True
PRINT_TRADABLE_COINS_LOG = True
TIME_SERIES_DATAS_LOG = False
ERR_LOG = True

CHECK_BALANCE_INFO = True
ANALYZE_DATA = True
TRADE_COIN = False

LOOP_MAX_NUM = float('inf')

if __name__ == '__main__':

    server_url = "https://api.upbit.com"
    upbit = API.UPbitObject(server_url=server_url, print_err=ERR_LOG)

    (access_key, secret_key) = upbit.set_key()

    if CHECK_BALANCE_INFO:

        # 기준 통화(매수에 사용)
        currency = 'KRW'

        # balance 정보에서 인덱스로 사용할 컬럼명
        balance_idx_nm = 'currency'

        # 거래 가능한 coin 정보에서 인덱스로 사용할 컬럼명
        coins_idx_nm = 'market'

        # 시계열 정보를 받는 기준
        interval_unit = 'minutes'
        interval_val = '1'
        count = 50 # 최대 200개
        call_term = 0.05

        # series 정보에서 인덱스로 사용할 컬럼명
        series_idx_nm = 'candle_date_time_kst'
        short_term_momentum_threshold = 1.05
        long_term_momentum_threshold = 1.02
        volume_momentum_threshold = 1.05

        # 매매 시 사용 정보
        buy_amount_unit = 50000

        loop_cnt = 0
        while loop_cnt < LOOP_MAX_NUM:
            balances = upbit.get_balance_info()
            balances.rename(columns={'avg_buy_price': 'avg_price'}, inplace=True)
            balances.set_index(balance_idx_nm, inplace=True)
            if PRINT_BALANCE_STATUS_LOG and loop_cnt % 100 == 0:
                print("--------------------- My Balance Status: %s ---------------------"%(loop_cnt))
                for idx, row in enumerate(balances.iterrows()):
                    print(str(idx) + " " + row[0] + ", balacne: "+ row[1]['balance'] + ", avg_price: " + row[1]['avg_price'])
                print("----------------------------------------------------------------")

            if ANALYZE_DATA:
                coins = upbit.look_up_all_coins()
                coins = coins.loc[[True if currency in market else False for market in coins[coins_idx_nm]]]
                coins.rename(columns={'korean_name': 'kr_nm', 'english_name': 'us_nm'}, inplace=True)
                coins.set_index(coins_idx_nm, inplace=True)

                if PRINT_TRADABLE_COINS_LOG and loop_cnt % 1000 == 0:
                    print("--------------------- Tradable Coins: %s ---------------------" % (loop_cnt))
                    for idx, row in enumerate(coins.iterrows()):
                        print(str(idx) + " " + row[0] + ", name: " + row[1]['kr_nm'])
                    print("-------------------------------------------------------------")

                for market in coins.index:
                    try:
                        series = upbit.get_candles(market=market, interval_unit=interval_unit, interval_val=interval_val, count=count)
                        if series is False:
                            continue
                        #series[series_idx_nm].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
                        series.rename(columns={'opening_price': 'open', 'high_price': 'high', 'low_price': 'low', 'trade_price': 'close', 'candle_acc_trade_volume': 'volume'}, inplace=True)
                        series.set_index(series_idx_nm, inplace=True)
                        series = series.sort_index()

                        if TIME_SERIES_DATAS_LOG:
                            print(series)
                            for row in series.iterrows():
                                tm = row[0]
                                datas = row[1]
                                print(tm, datas)

                        price = series.tail(1)['close'].values[0]
                        price5 = series.tail(5)['close'].mean()
                        price20 = series.tail(20)['close'].mean()

                        volume = series.tail(1)['volume'].values[0]
                        volume5 = series.tail(5)['volume'].mean()
                        volume20 = series.tail(20)['volume'].mean()

                        if price > price5*short_term_momentum_threshold and price5 > price20*long_term_momentum_threshold\
                                and volume5 > volume20*volume_momentum_threshold:
                            print("Market:" + market + ", Price: " + str(price)
                                  + ", Short Momentum:" + str(round(price/price5, 4))
                                  + ", Long Momentum:" + str(round(price5/price20, 4))
                                  + ", Volume Momentum:" + str(round(volume5/volume20, 4)))

                    except Exception as x:
                        if ERR_LOG:
                            print(market, ": ", x.__class__.__name__)

                    time.sleep(call_term)

                if TRADE_COIN:
                    # 매수 시 side='bid', price=매수금액, ord_type='price'
                    # 매도 시 side='ask', volume=매도수량, ord_type='market'
                    ret = upbit.order(market=market, side='bid', volume=None, price=str(buy_amount_unit), ord_type='price')
                    print(ret)
                    """
                    ret_code: 201
                    message: 주문 성공
                    
                    ret_code: 400
                    message: 최소주문금액 이하 주문
                    
                    ret_code: 401
                    message: 쿼리 오류
                    
                    ret_code: 500
                    message: 알수 없는 오류
                    """

            # 1 Cycle Finished
            loop_cnt += 1