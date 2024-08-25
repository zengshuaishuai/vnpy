#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/20 11:04
@Author  : petrus
@Project : vnpy-master
@File    : monthly_bar_generator.py
"""

from vnpy.trader.constant import Interval, Exchange
from vnpy.trader.database import get_database
from datetime import datetime

# 获取数据库实例
database = get_database()

# 定义要获取数据的合约
symbol = "600011"  # 替换为你的股票代码
exchange = Exchange.SSE  # 交易所
interval = Interval.MINUTE  # 使用分钟级别数据
start = datetime(2023, 1, 1)  # 开始时间
end = datetime(2023, 12, 31)  # 结束时间

# 从数据库中获取分钟级别的K线数据
bars = database.load_bar_data(symbol, exchange, interval, start, end)

# 定义月度K线生成器
class MonthlyBarGenerator:
    def __init__(self):
        self.current_month = None
        self.monthly_bar = None
        self.monthly_bars = []

    def update_bar(self, bar):
        bar_month = bar.datetime.month

        if self.current_month is None:
            self.current_month = bar_month
            self.monthly_bar = bar
            self.monthly_bar.interval = Interval.MONTHLY
        elif bar_month != self.current_month:
            self.monthly_bars.append(self.monthly_bar)
            self.current_month = bar_month
            self.monthly_bar = bar
            self.monthly_bar.interval = Interval.MONTHLY
        else:
            self.monthly_bar.high_price = max(self.monthly_bar.high_price, bar.high_price)
            self.monthly_bar.low_price = min(self.monthly_bar.low_price, bar.low_price)
            self.monthly_bar.close_price = bar.close_price
            self.monthly_bar.volume += bar.volume
            self.monthly_bar.turnover += bar.turnover

    def get_monthly_bars(self):
        if self.monthly_bar:
            self.monthly_bars.append(self.monthly_bar)
        return self.monthly_bars

# 创建月度K线生成器
generator = MonthlyBarGenerator()

# 更新生成器，合成月度K线
for bar in bars:
    generator.update_bar(bar)

# 获取月度K线
monthly_bars = generator.get_monthly_bars()

# 输出月度K线数据
for monthly_bar in monthly_bars:
    print(f"Month: {monthly_bar.datetime}, Open: {monthly_bar.open_price}, High: {monthly_bar.high_price}, Low: {monthly_bar.low_price}, Close: {monthly_bar.close_price},Volume: {monthly_bar.volume}, Turnover: {monthly_bar.turnover}")

