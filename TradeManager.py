#_*_ coding: utf-8 _*_



class TradeManager():
    def __init__(self):
        print("Generate TradeManager.")

        self.buy_amount_unit = None
        self.position_idx_nm = None

        self.balance = None
        self.balance_list = None

        self.api = None

    def __del__(self):
        print("Destroy TradeManager.")

    def set_api(self, api):

        self.api = api

    def set_balance(self, balance):

        self.balance = balance
        self.balance_list = self.balance.index

    def update_balance(self, balance):

        self.set_balance(balance=balance)

    def set_parameters(self, buy_amount_unit, position_idx_nm):

        self.buy_amount_unit = buy_amount_unit
        self.position_idx_nm = position_idx_nm

    def execute_at_market_price(self, market, signal, trade_cd):
        # 매수 시 side='bid', price=매수금액, ord_type='price'
        # 매도 시 side='ask', volume=매도수량, ord_type='market'
        ret = False
        if signal == 'BUY':
            # 해당 마켓 처음 매수
            if trade_cd == 1:
                buy_amount = self.buy_amount_unit
            # 시장 조정으로 추가 매수
            elif trade_cd == 2:
                buy_amount = float(self.balance['avg_price'][market])*float(self.balance['balance'][market])
            ret = self.api.order(market=market, side='bid', volume=None, price=str(buy_amount), ord_type='price')
            print(market + " 매수 성공") if ret.status_code == 201 else False

        elif signal == 'SELL':
            # 해당 코인을 보유하고 있는 경우 SELL 할 수 있음
            if market in self.balance_list:
                # 매도할 수량 계산(전체)
                sell_balance = self.balance[self.position_idx_nm][market]
                ret = self.api.order(market=market, side='ask', volume=str(sell_balance), price=None, ord_type='market')
                print(market + " 매도 성공") if ret.status_code == 201 else False

        return ret





