#_*_ coding: utf-8 _*_

class DataManager():
    def __init__(self, PRINT_DATA_LOG, SIMULATION=False):
        print("Generate DataManager.")

        self.SIMULATION = SIMULATION

        self.interval_unit = None
        self.interval_val = None
        self.count = None
        self.series_idx_nm = None

        self.last_idx_nm1 = None
        self.last_idx_nm2 = None

        self.series = None
        self.series_num = None

        self.last = None

        self.api = None
        self.db = None

        self.PRINT_DATA_LOG = PRINT_DATA_LOG

    def __del__(self):
        print("Destroy DataManager.")

    def set_parameters_for_series(self, interval_unit, interval_val, count, series_idx_nm, last_idx_nm1, last_idx_nm2):

        self.interval_unit = interval_unit
        self.interval_val = interval_val
        self.count = count
        self.series_idx_nm = series_idx_nm

        self.last_idx_nm1 = last_idx_nm1
        self.last_idx_nm2 = last_idx_nm2

    def set_api(self, api):

        self.api = api

    def set_db(self, db):

        self.db = db

    def get_series_info(self, market, no=0, curr=None, to=None):

        # 시뮬레이션인 경우 DB에 있는 데이터를 사용
        if self.SIMULATION == False:
            self.series = self.api.get_candles(market=market, to=to, interval_unit=self.interval_unit, interval_val=self.interval_val, count=self.count)
        else:
            self.series = self.db.get_candles(market=market, no=no, curr=curr, interval_unit=self.interval_unit, interval_val=self.interval_val, count=self.count)

        if self.series is not False:
            #
            if self.SIMULATION == False:
                self.series.rename(columns={'opening_price': 'open', 'high_price': 'high', 'low_price': 'low', 'trade_price': 'close', 'candle_acc_trade_volume': 'volume'}, inplace=True)
            else:
                self.series[self.series_idx_nm] = self.series['date']+'T'+self.series['time']
            self.series.set_index(self.series_idx_nm, inplace=True)

            self.series = self.series.sort_index()

            self.series_num = len(self.series.index)

            if self.PRINT_DATA_LOG:
                print(self.series)
                for row in self.series.iterrows():
                    tm = row[0]
                    datas = row[1]
                    print(tm, datas)

        return (self.series, self.series_num)

    def append_new_data(self, market, data=None):

        if data is not None:
            self.series = self.series.append(data)
            self.series_num = len(self.series.index)

        return (self.series, self.series_num)


    def get_last_info(self, market, no=0, seq=0):

        # 시뮬레이션인 경우 DB에 있는 데이터를 사용
        if self.SIMULATION == False:
            self.last = self.api.get_ticker(market=market)
        else:
            self.last = self.db.get_ticker(market=market, no=no, seq=seq)

        if self.last is not False:
            if self.SIMULATION == False:
                self.last.rename(columns={'opening_price': 'open', 'high_price': 'high', 'low_price': 'low', 'trade_price': 'close', 'candle_acc_trade_volume': 'volume'}, inplace=True)
            else:
                self.last.rename(columns={'date': 'trade_date_kst', 'time': 'trade_time_kst'}, inplace=True)
                #self.last[[self.last_idx_nm1, self.last_idx_nm2]].apply('-'.join, axis=1)
                self.last['trade_date_kst'] = self.last['trade_date_kst'].apply(lambda x: x.replace('-',''))
                self.last['trade_time_kst'] = self.last['trade_time_kst'].apply(lambda x: x.replace(':', ''))

            if self.PRINT_DATA_LOG:
                print(self.last)

        return self.last

