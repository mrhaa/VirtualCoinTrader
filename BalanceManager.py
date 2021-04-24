#_*_ coding: utf-8 _*_

import pandas as pd

class BalanceManager():
    def __init__(self, PRINT_BALANCE_STATUS_LOG, SIMULATION=False):
        print("Generate BalanceManager.")

        self.SIMULATION = SIMULATION

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
        self.db = None

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

    def set_db(self, db):

        self.db = db

    def get_balance_info(self, initial=False):

        # 시뮬레이션인 경우 DB에 있는 데이터를 사용
        if self.SIMULATION == False:
            self.balance = self.api.get_balance_info()
        else:
            if initial == True:
                self.balance = pd.DataFrame({'currency': [self.currency], 'balance': [10000000.0], 'avg_buy_price': [0.0], 'unit_currency': [self.currency]})

        if self.balance is not False:
            self.balance[self.balance_idx_nm] = self.balance[[self.balance_idx_nm1, self.balance_idx_nm2]].apply('-'.join, axis=1)
            self.balance.rename(columns={'avg_buy_price': 'avg_price'}, inplace=True)
            self.balance.set_index(self.balance_idx_nm, inplace=True)

            self.balance_num = len(self.balance.index)
            self.balance_list = list(self.balance.index)

            if self.PRINT_BALANCE_STATUS_LOG:
                print("----------------------- My Balance Status -----------------------")
                for idx, row in enumerate(self.balance.iterrows()):
                    print(str(idx) + " " + row[0] + ", balacne: " + row[1]['balance'] + ", avg_price: " + row[1]['avg_price'])
                print("-----------------------------------------------------------------")

        return (self.balance, self.balance_num, self.balance_list, self.max_balance_num)

    def update_balance_info(self, info=None, curr_price=None):

        if self.SIMULATION == False:
            return self.get_balance_info()
        else:
            if info['side'] == 'bid':
                if self.balance['balance'][self.currency+'-'+self.currency] > info['price']:
                    self.balance['balance'][self.currency+'-'+self.currency] -= info['price']
                    if info['trade_cd'] == 1:
                        new_balace = pd.DataFrame({'currency': info['market'].replace('KRW-', ''), 'balance': info['price']/curr_price, 'avg_price': curr_price, 'unit_currency': self.currency}, index=[info['market']])
                        self.balance = self.balance.append(new_balace)
                    else:
                        total_price = self.balance['balance'][info['market']]*self.balance['avg_price'][info['market']]+info['price']
                        total_balance = self.balance['balance'][info['market']]+info['price']/curr_price
                        self.balance['balance'][info['market']] = total_balance
                        self.balance['avg_price'][info['market']] = total_price/total_balance
            elif info['side'] == 'ask':
                self.balance['balance'][self.currency+'-'+self.currency] += info['volume']*curr_price
                self.balance = self.balance.drop(index=info['market'])

            self.balance_num = len(self.balance.index)
            self.balance_list = list(self.balance.index)

            return (self.balance, self.balance_num, self.balance_list, self.max_balance_num)

