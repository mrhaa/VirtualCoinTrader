#_*_ coding: utf-8 _*_

import mysql.connector
from mysql.connector import errorcode
import pandas as pd

import time


# DB 접속 정보를 dict type으로 준비한다.
config = {
    "host": "127.0.0.1",
    "port": 3306,
    "database": "DB",
    "user": "root",
    "password": "maria"
}

class DBManager():
    def __init__(self):
        print("Generate DBManager.")

    def __del__(self):
        print("Destroy DBManager.")

    def connet(self, host="127.0.0.1", port=3306, database="DB", user="root", password="maria"):
        try:
            #DB연결 설정
            config["host"] = host
            config["port"] = port
            config["database"] = database
            config["user"] = user
            config["password"] = password

            # DB 연결객체
            # config dict type 매칭
            self.conn = mysql.connector.connect(**config)
            print("DB connect")

            # DB 작업객체
            self.cursor = self.conn.cursor()
            print("DB obj. open")

            return True

        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("아이디 혹은 비밀번호 오류")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("DB 오류")
            else:
                print("기타 오류")

            # cursor 닫기
            if self.cursor:
                self.cursor.close()
                print("DB obj. close")

            # 연결 객체 닫기
            if self.conn:
                self.conn.close()
                print("DB disconnect")

            return False

    def disconnect(self):

        try:
            # cursor 닫기
            if self.cursor:
                self.cursor.close()
                print("DB obj. close")

            # 연결 객체 닫기
            if self.conn:
                self.conn.close()
                print("DB disconnect")

            return True

        except:

            return False

    def select_query(self, query, columns=None):
        sql = query
        sql_arg = None

        self.cursor.execute(sql, sql_arg)

        data = self.cursor.fetchall()
        df = pd.DataFrame(data, columns=columns)

        return df

    def execute_query(self, sql, sql_arg):

        try:
            # 수행
            #print(sql % sql_arg)
            self.cursor.execute(sql % sql_arg)

            # DB 반영
            self.conn.commit()

            return True

        except:
            self.conn.rollback()

            return False

    def update_markets(self, markets, columns):

        for row in markets.iterrows():
            cd = row[0]
            nm_kr = row[1][columns[0]]
            nm_us = row[1][columns[1]]

            sql = "INSERT INTO item (cd, kr_nm, us_nm, create_time, update_time) " \
                  "VALUES ('%s', '%s', '%s', now(), now()) ON DUPLICATE KEY UPDATE kr_nm = '%s', us_nm = '%s', update_time = now()"
            sql_arg = (cd, nm_kr, nm_us, nm_kr, nm_us)
            self.execute_query(sql, sql_arg)

    def update_prices(self, table_nm, no=None, seq=None, market=None, interval_unit=None, interval_val=None, series=None, columns=None):

        cd = market

        for row in series.iterrows():
            date = row[0][:10]
            time = row[0][-8:]
            open = row[1][columns[0]]
            close = row[1][columns[1]]
            low = row[1][columns[2]]
            high = row[1][columns[3]]
            volume = row[1][columns[4]]

            if table_nm == 'price_spot':
                sql = "INSERT INTO %s (no, seq, cd, date, time, open, close, low, high, volume, create_time, update_time) " \
                      "VALUES (%s, %s, '%s', '%s', '%s', %s, %s, %s, %s, %s, now(), now()) " \
                      "ON DUPLICATE KEY UPDATE open = %s, close = %s, low = %s, high = %s, volume = %s, update_time = now()"
                sql_arg = (table_nm, no, seq, cd, date, time, open, close, low, high, volume, open, close, low, high, volume)

            elif table_nm == 'price_hist':
                sql = "INSERT INTO %s (no, cd, interval_unit, interval_val, date, time, open, close, low, high, volume, create_time, update_time) " \
                      "VALUES (%s, '%s', '%s', '%s', '%s', '%s', %s, %s, %s, %s, %s, now(), now()) " \
                      "ON DUPLICATE KEY UPDATE open = %s, close = %s, low = %s, high = %s, volume = %s, update_time = now()"
                sql_arg = (table_nm, no, cd, interval_unit, interval_val, date, time, open, close, low, high, volume, open, close, low, high, volume)

            self.execute_query(sql, sql_arg)

    def save_signal(self, market, date, time, signal, trade_cd, price):

        sql = "INSERT INTO transaction (cd, date, time, signals, trade_cd, price, create_time) " \
              "VALUES ('%s', '%s', '%s', '%s', %s, %s, now())"
        sql_arg = (market, date, time, signal, trade_cd, price)
        #print(sql % sql_arg)
        self.execute_query(sql, sql_arg)

    def get_first_point(self, market, interval_unit, interval_val):
        sql = "SELECT date, time" \
              "  FROM price" \
              " WHERE cd = '%s'" \
              "   AND interval_unit = '%s'" \
              "   AND interval_val = '%s'" \
              " ORDER BY date, time" \
              " LIMIT 1" %(market, interval_unit, interval_val)
        #print(sql)
        ret = self.select_query(sql, columns=('date', 'time'))

        if len(ret) == 0:
            return None
        else:
            date = ret['date'][0]
            time = ret['time'][0]

            return date+' '+time

    def get_market_list(self):
        sql = "SELECT DISTINCT cd" \
              "  FROM item"
        # print(sql)
        ret = self.select_query(sql, columns=('cd',))

        if len(ret) == 0:
            return None
        else:
            return list(ret['cd'])

    def get_candles(self, market, no=0, curr=None, interval_unit='minutes', interval_val='1', count=200):

        if curr == None:
            ret = self.get_ticker(market=market, no=no, seq=0)
            curr = ret['date'].iloc[0]+'T'+ret['time'].iloc[0]

        sql = "SELECT cd, date, time, open, close, low, high, volume " \
              "  FROM price_hist" \
              " WHERE no = %s" \
              "   AND cd = '%s'" \
              "   AND interval_unit = '%s'" \
              "   AND interval_val = '%s'" \
              "   AND concat(date, 'T', time) < '%s'"%(no, market, interval_unit, interval_val, curr)
        #print(sql)
        ret = self.select_query(sql, columns=('cd', 'date', 'time', 'open', 'close', 'low', 'high', 'volume'))

        if len(ret) == 0:
            return None
        else:
            return ret

    def get_ticker(self, market, no=0, seq=0):

        sql = "SELECT cd, date, time, open, close, low, high, volume " \
              "  FROM price_spot" \
              " WHERE cd = '%s'" \
              "   AND no = %s" \
              "   AND seq = %s" \
              " ORDER BY date, time" % (market, no, seq)
        #print(sql)
        ret = self.select_query(sql, columns=('cd', 'date', 'time', 'open', 'close', 'low', 'high', 'volume'))

        if len(ret) == 0:
            return None
        else:
            return ret

    def look_up_all_coins(self, no=0):

        sql = "SELECT i.cd, i.kr_nm, i.us_nm" \
              "  FROM item i, price_hist ph" \
              " WHERE i.cd = ph.cd" \
              "   AND ph.no = %s" \
              " GROUP BY i.cd" % (no)

        #print(sql)
        ret = self.select_query(sql, columns=('market', 'kr_nm', 'us_nm'))

        if len(ret) == 0:
            return None
        else:
            return ret

    def max_no(self):

        sql = "SELECT max(no)" \
              "  FROM price_spot" \

        #print(sql)
        ret = self.select_query(sql).iloc[0, 0]

        return ret

    def max_seq(self, no=0):

        sql = "SELECT max(seq)" \
              "  FROM price_spot" \
              " WHERE no= %s" % (no)

        #print(sql)
        ret = self.select_query(sql).iloc[0, 0]

        return ret

    def save_simulation_result(self, no, seq, algorithm, base_z_value, short_term, long_term, sell_short_term, sell_long_term
                               , buy_amount_multiple, target_profit, additional_position_threshold
                               , max_playable_market, market_shock_base_rate, minimum_price, current_period, market_shock_threshold
                               , max_value, min_value, value):

        sql = "INSERT INTO simulation_result (no, seq, algorithm, base_z_value, short_term, long_term, sell_short_term, sell_long_term, buy_amount_multiple, target_profit, additional_position_threshold, max_playable_market, market_shock_base_rate, minimum_price, current_period, market_shock_threshold, max_value, min_value, value, create_time, update_time) " \
              "VALUES (%s, %s, '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now(), now()) " \
              "ON DUPLICATE KEY UPDATE max_value = %s, min_value = %s, value = %s, update_time = now()"
        sql_arg = (no, seq, algorithm, base_z_value, short_term, long_term, sell_short_term, sell_long_term, buy_amount_multiple, target_profit, additional_position_threshold, max_playable_market, market_shock_base_rate, minimum_price, current_period, market_shock_threshold, max_value, min_value, value, max_value, min_value, value)

        self.execute_query(sql, sql_arg)




