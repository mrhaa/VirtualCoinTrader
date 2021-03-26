#_*_ coding: utf-8 _*_

class DataManager():
    def __init__(self, PRINT_DATA_LOG):
        print("Generate DataManager.")

        self.interval_unit = None
        self.interval_val = None
        self.count = None
        self.series_idx_nm = None

        self.series = None
        self.series_num = None

        self.api = None

        self.PRINT_DATA_LOG = PRINT_DATA_LOG

    def __del__(self):
        print("Destroy DataManager.")

    def set_parameters_for_series(self, interval_unit, interval_val, count, series_idx_nm):

        self.interval_unit = interval_unit
        self.interval_val = interval_val
        self.count = count
        self.series_idx_nm = series_idx_nm

    def set_api(self, api):

        self.api = api

    def get_series(self, market):

        self.series = self.api.get_candles(market, self.interval_unit, self.interval_val, self.count)
        if self.series is not False:
            # series[series_idx_nm].apply(lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%S"))
            self.series.rename(columns={'opening_price': 'open', 'high_price': 'high', 'low_price': 'low', 'trade_price': 'close', 'candle_acc_trade_volume': 'volume'}, inplace=True)
            self.series.set_index(self.series_idx_nm, inplace=True)
            series = self.series.sort_index()

            self.series_num = len(self.series.index)

            if self.PRINT_DATA_LOG:
                print(series)
                for row in series.iterrows():
                    tm = row[0]
                    datas = row[1]
                    print(tm, datas)

        return self.series

