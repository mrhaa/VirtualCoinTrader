#_*_ coding: utf-8 _*_

class BalanceManager():
    def __init__(self, PRINT_BALANCE_STATUS_LOG):
        print("Generate BalanceManager.")

        self.currency = None
        self.balance_idx_nm1 = None
        self.balance_idx_nm2 = None
        self.balance_idx_nm = None
        self.max_balance_num = None

        self.balance = None
        self.balance_num = None
        self.balance_list = None
        self.max_balance_num = None

        self.api = None

        self.PRINT_BALANCE_STATUS_LOG = PRINT_BALANCE_STATUS_LOG

    def __del__(self):
        print("Destroy BalanceManager.")

    def set_parameters(self, currency, balance_idx_nm1, balance_idx_nm2, max_balance_num):

        self.currency = currency
        self.balance_idx_nm1 = balance_idx_nm1
        self.balance_idx_nm2 = balance_idx_nm2
        self.balance_idx_nm = self.balance_idx_nm1+'-'+self.balance_idx_nm2
        self.max_balance_num = max_balance_num

    def set_api(self, api):

        self.api = api

    def get_balance(self):

        self.balance = self.api.get_balance_info()
        if self.balance is not False:
            self.balance[self.balance_idx_nm] = self.balance[[self.balance_idx_nm1, self.balance_idx_nm2]].apply('-'.join, axis=1)
            self.balance.rename(columns={'avg_buy_price': 'avg_price'}, inplace=True)
            self.balance.set_index(self.balance_idx_nm, inplace=True)

            self.balance_num = len(self.balance.index)
            self.balance_list = list(self.balance.index)
            if self.balance_num > self.max_balance_num:
                self.max_balance_num = self.balance_num

            if self.PRINT_BALANCE_STATUS_LOG:
                print("----------------------- My Balance Status -----------------------")
                for idx, row in enumerate(self.balance.iterrows()):
                    print(str(idx) + " " + row[0] + ", balacne: " + row[1]['balance'] + ", avg_price: " + row[1]['avg_price'])
                print("-----------------------------------------------------------------")

        return self.balance

    def updata_balance(self):

        return self.get_balance()
