#_*_ coding: utf-8 _*_

class MarketManager():
    def __init__(self, PRINT_TRADABLE_MARKET_LOG, SIMULATION=False):
        print("Generate MarketManager.")

        self.SIMULATION = SIMULATION

        self.currency = None
        self.market_idx_nm = None

        self.markets = None
        self.markets_num = None
        self.markets_list = None

        self.api = None
        self.db = None

        self.PRINT_TRADABLE_MARKET_LOG = PRINT_TRADABLE_MARKET_LOG

    def __del__(self):
        print("Destroy MarketManager.")

    def set_parameters(self, currency, market_idx_nm):

        self.currency = currency
        self.market_idx_nm = market_idx_nm

    def set_api(self, api):

        self.api = api

    def set_db(self, db):

        self.db = db

    def get_markets_info(self):

        # 시뮬레이션인 경우 DB에 있는 데이터를 사용
        if self.SIMULATION == False:
            self.markets = self.api.look_up_all_coins()
        else:
            self.markets = self.db.look_up_all_coins()

        if self.markets is not False:
            #self.btc_markets = self.markets.loc[[True if 'BTC-' in market else False for market in self.markets[self.market_idx_nm]]]
            #self.usdt_markets = self.markets.loc[[True if 'USDT-' in market else False for market in self.markets[self.market_idx_nm]]]
            self.markets = self.markets.loc[[True if self.currency+'-' in market else False for market in self.markets[self.market_idx_nm]]]
            self.markets.rename(columns={'korean_name': 'kr_nm', 'english_name': 'us_nm'}, inplace=True)
            self.markets.set_index(self.market_idx_nm, inplace=True)

            self.markets_num = len(self.markets.index)
            self.markets_list = list(self.markets.index)

            if self.PRINT_TRADABLE_MARKET_LOG:
                print("----------------------- Tradable Coins -----------------------")
                for idx, row in enumerate(self.markets.iterrows()):
                    print(str(idx) + " " + row[0] + ", name: " + row[1]['kr_nm'])
                print("--------------------------------------------------------------")

        return (self.markets, self.markets_num, self.markets_list)