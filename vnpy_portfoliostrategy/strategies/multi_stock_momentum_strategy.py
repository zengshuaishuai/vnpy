#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 13:43
@Author  : petrus
@Project : vnpy-master
@File    : multi_stock_momentum_strategy.py
"""

from vnpy_portfoliostrategy import StrategyTemplate, StrategyEngine
from vnpy.trader.object import BarData
from vnpy.trader.utility import BarGenerator, ArrayManager
from datetime import time
from vnpy.trader.utility import get_file_path
import logging

class MultiStockMomentumStrategy(StrategyTemplate):
    author = "wb-zengshuai"

    # 策略参数
    max_positions = 5
    buy_threshold_min = 0.095
    buy_threshold_max = 0.099
    fixed_size = 100

    # 策略变量
    daily_bought_stocks_by_date = {}
    yesterday_close = {}

    parameters = [
        "max_positions",
        "buy_threshold_min",
        "buy_threshold_max",
        "fixed_size"
    ]
    variables = [
        "daily_bought_stocks_by_date",
        "yesterday_close"
    ]

    def __init__(self, strategy_engine: StrategyEngine, strategy_name: str, vt_symbols: list[str], setting: dict):
        super().__init__(strategy_engine, strategy_name, vt_symbols, setting)
        self.bgs = {symbol: BarGenerator(self.on_bars) for symbol in vt_symbols}
        self.am = {symbol: ArrayManager() for symbol in vt_symbols}
        self.last_bars = {symbol: None for symbol in vt_symbols}
        self.candidates = []

        # 配置日志记录器
        log_filename = get_file_path(f"strategy_log_{self.strategy_name}.log")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(self.strategy_name)

    def on_init(self):
        self.write_log("策略初始化")
        self.daily_bought_stocks_by_date = {}
        self.load_bars(100)

    def on_start(self):
        self.write_log("策略启动")

    def on_stop(self):
        self.write_log("策略停止")

    def on_bars(self, bars: dict[str, BarData]):
        for vt_symbol, bar in bars.items():
            if bar.datetime.time() == time(9, 30):
                self.write_log(f"日期: {bar.datetime.date()}，交易时间: {bar.datetime.time()}，Symbol: {vt_symbol}")

            am = self.am[vt_symbol]
            am.update_bar(bar)
            if not am.inited:
                continue

            # 获取当前时间
            current_time = bar.datetime.time()
            # 获取当前交易日
            current_day = bar.datetime.date()

            # 只在13:00到15:00之间执行策略
            if not (time(13, 0) <= current_time <= time(15, 0)):
                continue

            #更新昨日收盘价
            if self.last_bars[vt_symbol] and self.last_bars[vt_symbol].datetime.date() != current_day:
                self.yesterday_close[vt_symbol] = self.last_bars[vt_symbol].close_price  # 更新昨日收盘价
                self.candidates = []  # 重置候选股票列表

            if current_day not in self.daily_bought_stocks_by_date:
                self.daily_bought_stocks_by_date[current_day] = []

            if len(self.daily_bought_stocks_by_date[current_day]) >= self.max_positions:
                continue

            # 获取前一交易日的收盘价
            prev_close = self.yesterday_close.get(vt_symbol, None)

            # 如果没有昨日收盘价，直接返回
            if prev_close is None:
                self.last_bars[vt_symbol] = bar  # 保存当前的K线为 last_bar
                continue
            # 计算当前分钟的平均价格
            minute_avg_price = (bar.open_price + bar.high_price + bar.low_price + bar.close_price) / 4
            pct_change = (minute_avg_price - prev_close) / prev_close

            self.write_log(
                f"当前时间: {current_day} {current_time},symbol: {vt_symbol}, 平均价格: {minute_avg_price}, 前一天的收盘价: {prev_close},涨跌幅: {pct_change}")

            if self.buy_threshold_min <= pct_change <= self.buy_threshold_max and vt_symbol not in self.daily_bought_stocks_by_date[current_day]:
                self.candidates.append((vt_symbol, minute_avg_price, pct_change))
                self.write_log(f"符合条件的股票: {vt_symbol}")

            if len(self.candidates) >= self.max_positions or current_time == time(15, 0):
                sorted_candidates = sorted(self.candidates, key=lambda x: x[1], reverse=True)[:self.max_positions]

                for symbol, price, pct_change in sorted_candidates:
                    if len(self.daily_bought_stocks_by_date[current_day]) < self.max_positions:
                        self.buy(symbol, price, self.fixed_size)
                        self.daily_bought_stocks_by_date[current_day].append(symbol)
                        self.write_log(f"买入 {symbol} 于 {current_time}，数量 {self.fixed_size}，价格 {price}，涨幅 {pct_change * 100:.2f}%")

                self.candidates = []

            self.last_bars[vt_symbol] = bar   # 保存当前的K线为 last_bar

    def after_trading(self):
        self.write_log("收盘后操作")

    def write_log(self, msg):
        """
        重写write_log方法，增加日志记录到文件
        """
        self.logger.info(msg)
        super().write_log(msg)

