#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/24 12:22
@Author  : petrus
@Project : vnpy-master
@File    : monthly_volatility_strategy.py
"""
import traceback
from datetime import datetime

from tianyan.bt.bt_portfoliostrategy import StrategyTemplate, StrategyEngine
from tianyan.bt.trader.object import BarData
from tianyan.bt.trader.utility import ArrayManager

class MonthlyVolatilityPortfolioStrategy(StrategyTemplate):
    author = "wb-zengshuai"

    def __init__(self, strategy_engine: StrategyEngine, strategy_name: str, vt_symbols: list[str], setting: dict):
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)
        self.ams = {symbol: ArrayManager() for symbol in vt_symbols}
        self.trading_calendar = []

    def set_trading_calendar(self, trading_calendar: list[datetime]):
        self.trading_calendar = [dt for dt in trading_calendar]

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bars(1)

    def on_start(self):
        self.write_log("策略启动")

    def on_stop(self):
        self.write_log("策略停止")

    def on_bars(self, bars: dict[str, BarData]):
        for vt_symbol, bar in bars.items():
            am = self.ams[vt_symbol]
            am.update_bar(bar)
            if not am.inited:
                continue
            current_date = bar.datetime.date()

            if current_date not in self.trading_calendar:
                raise ValueError("当前日期不在交易日历中")

            current_index = self.trading_calendar.index(current_date)

            # 前一个交易日的日期
            previous_trading_day = self.trading_calendar[current_index - 1]

            # 下一个交易日的日期
            next_trading_day = self.trading_calendar[current_index + 1]

            # 检查是否为月初的第一个交易日
            if previous_trading_day.month != current_date.month:
                pass
                #self.sell_all_positions()
                #self.rebalance(vt_symbol, current_date)

            # 检查是否为月末的最后一个交易日
            if next_trading_day.month != current_date.month:
                self.sell_all_positions()
                self.rebalance(current_date)
                #self.sell_all_positions()

    def rebalance(self, current_date):
        """卖出所有股票的最后时刻后，选择排名前十的股票，进行买入"""
        str_date = current_date.strftime("%Y.%m.%d")
        factor_code = "jump_factor1"
        try:
            factor_df = self.get_factor_data_by_date_list(factor_code_=factor_code, stock_code_list_=[],
                                                          trade_date_list_=[str_date])
            code_list = factor_df.sort_values('value')['code'][:10].apply(lambda x: f'{x}.SSE')
            print(code_list)
            # code_list = ['603858.SSE', '603859.SSE', '603860.SSE', '603861.SSE', '603866.SSE', '603867.SSE', '603868.SSE', '603869.SSE', '603871.SSE', '603876.SSE']

        except Exception as e:
            print("An error occurred:")
            traceback.print_exc()

        # 按照选中的股票进行买入
        for symbol in code_list:
            self.buy(symbol, price=29.6, volume=10)

    def sell_all_positions(self):
        """平掉所有仓位"""
        self.rebalance_portfolio({})