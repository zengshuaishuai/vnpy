#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/16 10:03
@Author  : petrus
@Project : vnpy-master
@File    : monthly_volatility_strategy.py
"""

from datetime import timedelta
from vnpy_ctastrategy import CtaTemplate, BarData, ArrayManager


class MonthlyVolatilityStrategy(CtaTemplate):
    author = "用Python的交易员"

    # 策略参数
    stock_count = 10  # 选择振幅前十的股票
    capital_per_stock = 1_000_000  # 每只股票的投资金额

    # 策略变量
    symbols_to_trade = []  # 本月选中的股票

    parameters = ["stock_count", "capital_per_stock"]
    variables = ["symbols_to_trade"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.am = ArrayManager()

    def on_init(self):
        self.write_log("策略初始化")
        self.load_bar(20)  # 加载20根历史K线

    def on_start(self):
        self.write_log("策略启动")

    def on_stop(self):
        self.write_log("策略停止")

    def on_bar(self, bar: BarData):
        self.am.update_bar(bar)
        if not self.am.inited:
            return

        current_date = bar.datetime.date()

        # 每月第一个交易日
        if current_date.day == 1:
            self.rebalance()

        # 每月最后一个交易日，平仓
        last_day_of_month = (current_date + timedelta(days=1)).day == 1
        if last_day_of_month:
            self.sell_all_positions()

        self.put_event()

    def rebalance(self):
        # 计算所有股票的振幅因子，选择排名前十的股票
        stock_volatilities = self.calculate_volatility()
        selected_stocks = sorted(stock_volatilities, key=lambda x: x[1], reverse=True)[:self.stock_count]

        # 按照选中的股票进行买入
        for symbol, volatility in selected_stocks:
            self.buy(symbol, self.capital_per_stock)

    def calculate_volatility(self):
        # 此处应计算所有股票的月度振幅因子
        # 返回一个包含股票代码和对应振幅因子的列表
        stock_volatilities = []
        for symbol in self.symbols:
            bars = self.cta_engine.get_bars(symbol, Interval.DAILY, 20)  # 假设我们获取最近20天的日线数据
            if bars:
                highs = [bar.high_price for bar in bars]
                lows = [bar.low_price for bar in bars]
                volatility = (max(highs) - min(lows)) / min(lows)  # 计算振幅因子
                stock_volatilities.append((symbol, volatility))
        return stock_volatilities

    def sell_all_positions(self):
        # 平掉所有仓位
        self.close_all()

    def buy(self, symbol, amount):
        # 根据金额和当前价格计算买入数量，并下单
        price = self.cta_engine.get_price(symbol)
        if price:
            size = amount // price
            self.cta_engine.buy(symbol, price, size)

