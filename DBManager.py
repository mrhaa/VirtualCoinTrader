#_*_ coding: utf-8 _*_

import mysql.connector
from mysql.connector import errorcode
import pandas as pd


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

    def update_series(self, series, columns):
        for row in series.iterrows():
            date = row[0][:10]
            time = row[0][-8:]
            cd = row[1][columns[0]]
            open = row[1][columns[1]]
            close = row[1][columns[2]]
            low = row[1][columns[3]]
            high = row[1][columns[4]]
            volume = row[1][columns[5]]

