import requests
import pyupbit
import time
import pprint


def test_order():
    f = open("upbit.txt")
    lines = f.readlines()
    access = lines[0].strip()
    secret = lines[1].strip()
    f.close()

    # print(access)
    # print(secret)

    if 0:
        upbit = pyupbit.Upbit(access, secret)
        balances = upbit.get_balances()
        print(balances)
        #pprint.pprint(balances[0])
    else:
        import os
        import jwt
        import uuid
        import hashlib
        from urllib.parse import urlencode

        import requests

        access_key = access #os.environ['UPBIT_OPEN_API_ACCESS_KEY']
        secret_key = secret #os.environ['UPBIT_OPEN_API_SECRET_KEY']
        server_url = "https://api.upbit.com" # os.environ['UPBIT_OPEN_API_SERVER_URL']

        payload = {
            'access_key': access_key,
            'nonce': str(uuid.uuid4()),
        }

        jwt_token = jwt.encode(payload, secret_key)
        authorize_token = 'Bearer {}'.format(jwt_token)
        headers = {"Authorization": authorize_token}

        res = requests.get(server_url + "/v1/accounts", headers=headers)

        print(res.json())

def read_tick():
    print("Start")

    url = "https://api.upbit.com/v1/market/all"
    params = {
        "isDetails": "false"
    }

    resp = requests.get(url, params=params)
    datas = resp.json()

    tickers = pyupbit.get_tickers(fiat="KRW")
    # tickers = ['KRW-BTC', 'KRW-DOGE']

    a = 0
    while True:
        a = a + 1
        print(a)
        for ticker in tickers:
            # price = pyupbit.get_current_price(ticker)
            df_prc = pyupbit.get_ohlcv(ticker, "minute1", "10")
            df_vol = pyupbit.get_ohlcv(ticker, "minute1", "10")

            price = df_prc.tail(1)['close'].values[0]
            MA05prc = df_prc.tail(5)['close'].mean()
            MA20prc = df_prc['close'].mean()

            volume = df_vol.tail(1)['volume'].values[0]
            MA05vol = df_vol.tail(5)['volume'].sum()
            MA20vol = df_vol['volume'].sum()

            if price > MA05prc * 1.002 and MA05prc > MA20prc * 1.005:  # and volume*5 > MA05vol and MA05vol*4 > MA20vol:
                for data in datas:
                    if data['market'] == ticker:
                        print(data['korean_name'], end=" ")
                # print(ticker, end=" ")
                print("MTM=", price, end=" ")
                print("5MAprc=", round(price / MA05prc, 4), end=" ")
                print("20MAprc=", round(price / MA20prc, 4), end=" ")
                print("Vol=", volume, end=" ")
                print("5MAvol=", round(volume * 5 / MA05vol, 4), end=" ")
                print("20MAvol=", round(volume * 20 / MA20vol, 4))

            time.sleep(0.14)


if __name__ == '__main__':

    test_order()