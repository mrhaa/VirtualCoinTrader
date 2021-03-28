#_*_ coding: utf-8 _*_

import os
import platform
import time
import requests
import jwt
import uuid
import hashlib
from urllib.parse import urlencode
import pandas as pd
import pickle

base_dir = (os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))
if platform.system() == 'Windows':
    key_dir =  '%s\\'%(base_dir)
else:
    key_dir = '%s/'%(base_dir)

class UPbit:    # 클래스
    def __init__(self, server_url, API_PRINT_ERR=True):
        print("Generate UPbit.")

        self.access_key = ""
        self.secret_key = ""
        self.server_url = server_url

        self.API_PRINT_ERR = API_PRINT_ERR

    def __del__(self):
        print("Destroy UPbit.")

    def get_key(self):

        f = open("%supbit.txt"%(key_dir))
        lines = f.readlines()
        self.access_key = lines[0].strip()
        self.secret_key = lines[1].strip()
        f.close()

        return (self.access_key, self.secret_key)

    def get_balance_info(self):

        ret = None
        for i in range(10):
            try:
                payload = {
                    'access_key': self.access_key,
                    'nonce': str(uuid.uuid4()),
                }

                jwt_token = jwt.encode(payload, self.secret_key)
                authorize_token = 'Bearer {}'.format(jwt_token)
                headers = {"Authorization": authorize_token}

                ret = pd.DataFrame(requests.get(self.server_url + "/v1/accounts", headers=headers).json())

            except Exception as x:
                if self.API_PRINT_ERR:
                    print(self.get_balance_info.__name__, x.__class__.__name__)

                time.sleep(0.1)

        if ret is not None:
            return ret
        else:
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
                if side == 'bid' and ord_type == 'price' and 'volume' in query.keys():
                    del query['volume']

                # 시장가 매도 시
                if side == 'ask' and ord_type == 'market' and 'price' in query.keys():
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
            if self.API_PRINT_ERR:
                print(self.order.__name__, x.__class__.__name__)

            return False

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

    def look_up_all_coins(self):
        querystring = {"isDetails": "false"}

        ret = None
        for i in range(10):
            try:
                ret = pd.DataFrame(requests.request("GET", self.server_url + "/v1/market/all", params=querystring).json())
                break

            except Exception as x:
                if self.API_PRINT_ERR:
                    print(self.look_up_all_coins.__name__, x.__class__.__name__)

                time.sleep(0.1)

        if ret is not None:
            return ret
        else:
            return False

    def get_ticker(self, market):

        querystring = {}
        querystring['markets'] = market

        ret = None
        for i in range(10):
            try:
                ret = pd.DataFrame(
                    requests.request("GET", self.server_url + "/v1/ticker", params=querystring).json())
                break

            except Exception as x:
                if self.API_PRINT_ERR:
                    print(i, self.get_ticker.__name__, market, x.__class__.__name__)

                time.sleep(0.1)

        if ret is not None:
            return ret
        else:
            return False

    def get_candles(self, market, to, interval_unit='minutes', interval_val='1', count=200):

        querystring = {}
        querystring['market'] = market
        if to is not None:
            querystring['to'] = to
        querystring['count'] = count

        ret = None
        for i in range(10):
            try:
                ret = pd.DataFrame(requests.request("GET", self.server_url + "/v1/candles/%s/%s" % (interval_unit, interval_val), params=querystring).json())
                break

            except Exception as x:
                if self.API_PRINT_ERR:
                    print(i, self.get_candles.__name__, market, x.__class__.__name__)

                time.sleep(0.1)

        if ret is not None:
            return ret
        else:
            return False

        """
        ret_code: 200
        message: API 요청 성공

        ret_code: 429
        message: 너무 많은 API 요청
        """

