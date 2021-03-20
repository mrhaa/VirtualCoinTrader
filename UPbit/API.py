import time
import requests
import jwt
import uuid
import hashlib
from urllib.parse import urlencode
import pandas as pd

import pyupbit

class UPbitObject:    # 클래스
    def __init__(self, server_url, print_err=True):

        self.access_key = ""
        self.secret_key = ""
        self.server_url = server_url

        self.PRINT_ERR = print_err

    def set_key(self):

        f = open("UPbit/upbit.txt")
        lines = f.readlines()
        self.access_key = lines[0].strip()
        self.secret_key = lines[1].strip()
        f.close()

        return (self.access_key, self.secret_key)

    def get_balance_info(self):

        try:
            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
            }

            jwt_token = jwt.encode(payload, self.secret_key)
            authorize_token = 'Bearer {}'.format(jwt_token)
            headers = {"Authorization": authorize_token}

            return pd.DataFrame(requests.get(self.server_url + "/v1/accounts", headers=headers).json())

        except Exception as x:
            if self.PRINT_ERR:
                print(self.get_balance_info.__name__, x.__class__.__name__)

            return False

    def order(self, market, side, volume, price, ord_type):

        try:
            query = {}

            if market is not None:
                query['market'] = market

            # 매수 = bid, 매도 = ask
            if side is not None:
                query['side'] = side

            if volume is not None:
                query['volume'] = volume

            if price is not None:
                query['price'] = price

            # 지정가 = limit, 시장가 매수 = price, 시장가 매도 = market
            if ord_type is not None:
                query['ord_type'] = ord_type

                # 시장가 매수 시
                if side == 'bid' and ord_type == 'price':
                    del query['volume']

                # 시장가 매도 시
                if side == 'ask' and ord_type == 'market':
                    del query['price']

            query_string = urlencode(query).encode()

            m = hashlib.sha512()
            m.update(query_string)
            query_hash = m.hexdigest()

            payload = {
                'access_key': self.access_key,
                'nonce': str(uuid.uuid4()),
                'query_hash': query_hash,
                'query_hash_alg': 'SHA512',
            }

            jwt_token = jwt.encode(payload, self.secret_key)
            authorize_token = 'Bearer {}'.format(jwt_token)
            headers = {"Authorization": authorize_token}

            return requests.post(self.server_url + "/v1/orders", params=query, headers=headers)

        except Exception as x:
            if self.PRINT_ERR:
                print(self.order.__name__, x.__class__.__name__)

            return False

    def look_up_all_coins(self):
        querystring = {"isDetails": "false"}

        try:
            return pd.DataFrame(requests.request("GET", self.server_url + "/v1/market/all", params=querystring).json())

        except Exception as x:
            if self.PRINT_ERR:
                print(self.look_up_all_coins.__name__, x.__class__.__name__)

            return False

    def get_candles(self, market, interval_unit='minutes', interval_val='1', count=200):

        querystring = {}
        querystring['market'] = market
        querystring['count'] = count

        try:
            return pd.DataFrame(requests.request("GET", self.server_url + "/v1/candles/%s/%s" % (interval_unit, interval_val), params=querystring).json())

        except Exception as x:
            if self.PRINT_ERR:
                print(self.get_candles.__name__, market, x.__class__.__name__)

            return False

        """
        ret_code: 200
        message: API 요청 성공

        ret_code: 429
        message: 너무 많은 API 요청
        """