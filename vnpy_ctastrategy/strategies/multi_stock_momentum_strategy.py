#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 13:43
@Author  : petrus
@Project : vnpy-master
@File    : multi_stock_momentum_strategy.py
"""

from vnpy_ctastrategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)
from datetime import datetime, time
import logging
from vnpy.trader.utility import get_file_path


class MultiStockMomentumStrategy(CtaTemplate):
    author = "wb-zengshuai"

    # 策略参数
    max_positions = 5  # 每天最多买入5支股票
    buy_threshold_min = 0.095  # 涨幅下限
    buy_threshold_max = 0.099  # 涨幅上限
    fixed_size = 100  # 每次买入的股数

    # 策略变量
    daily_bought_stocks_by_date = {}
    yesterday_close = None  # 用于存储昨日收盘价

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

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        super().__init__(cta_engine, strategy_name, vt_symbol, setting)
        self.bg = BarGenerator(self.on_bar)
        self.am = ArrayManager()
        self.candidates = []  # 初始化候选股票列表
        self.last_bar = None  # 用于保存前一根K线
        # 初始化前一日收盘价字典
        #self.prev_close_price = 7.6

        # 配置日志记录器
        log_filename = get_file_path(f"strategy_log_{self.strategy_name}.log")
        logging.basicConfig(
            filename=log_filename,
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        self.logger = logging.getLogger(self.strategy_name)

    def on_init(self):
        """
        策略初始化回调。
        """
        self.write_log("策略初始化")
        self.daily_bought_stocks_by_date = {}  # 初始化当天已购买的股票列表
        self.load_bar(100)

    def on_start(self):
        """
        策略启动回调。
        """
        self.write_log("策略启动")

    def on_stop(self):
        """
        策略停止回调。
        """
        self.write_log("策略停止")

    def before_trading(self):
        """
        开盘前清仓操作。
        """
        self.write_log("每天开盘前清仓操作")
        self.close_all()  # 清仓所有持仓

    def on_bar(self, bar: BarData):
        """
        每根K线数据的回调。
        """
        am = self.am
        am.update_bar(bar)
        if not am.inited:
            return

        # 获取当前时间
        current_time = bar.datetime.time()

        # 检查是否为新的一天
        if self.last_bar and self.last_bar.datetime.date() != bar.datetime.date():
            self.yesterday_close = self.last_bar.close_price  # 更新昨日收盘价
            self.candidates = []  # 重置候选股票列表

        # 只在13:00到15:00之间执行策略
        if not (time(13, 0) <= current_time <= time(15, 0)):
            self.last_bar = bar  # 保存当前的K线为 last_bar
            return

        # 获取当前交易日
        current_day = bar.datetime.date()

        # 初始化当天的购买记录
        if current_day not in self.daily_bought_stocks_by_date:
            self.daily_bought_stocks_by_date[current_day] = []

        # 判断当天的买入股票数是否超过限制
        if len(self.daily_bought_stocks_by_date[current_day]) >= self.max_positions:
            return

        # 获取前一交易日的收盘价
        prev_close = self.yesterday_close

        # 如果没有昨日收盘价，直接返回
        if prev_close is None:
            self.last_bar = bar  # 保存当前的K线为 last_bar
            return

        # 获取每分钟的平均价格
        minute_avg_price = (bar.open_price + bar.high_price + bar.low_price + bar.close_price) / 4

        # 计算涨跌幅
        pct_change = (minute_avg_price - prev_close) / prev_close
        self.write_log(f"当前时间: {current_day} {current_time}, 平均价格: {minute_avg_price}, 前一天的收盘价: {prev_close},涨跌幅: {pct_change}")

        # 判断涨幅是否在指定范围内，并且当前股票没有被重复买入
        if self.buy_threshold_min <= pct_change <= self.buy_threshold_max and bar.symbol not in self.daily_bought_stocks_by_date[current_day]:
            # 将符合条件的股票加入候选列表
            self.candidates.append((bar.symbol, minute_avg_price, pct_change))
            self.write_log(f"符合条件的股票: {bar.symbol}")

        # 如果已经到达最后一根K线，或者有足够多的候选股票，进行排序并买入
        if len(self.candidates) >= self.max_positions or current_time == time(15, 0):
            # 按照价格降序排序，选取价格最高的前5只股票
            sorted_candidates = sorted(self.candidates, key=lambda x: x[1], reverse=True)[:self.max_positions]

            # 执行买入操作
            for symbol, price, pct_change in sorted_candidates:
                if len(self.daily_bought_stocks_by_date[current_day]) < self.max_positions:
                    self.buy(price, self.fixed_size, symbol)
                    self.daily_bought_stocks_by_date[current_day].append(symbol)
                    self.write_log(f"买入 {symbol} 于 {current_time}，数量 {self.fixed_size}，价格 {price}，涨幅 {pct_change * 100:.2f}%")

            # 清空候选列表
            self.candidates = []

        self.last_bar = bar  # 保存当前的K线为 last_bar
        self.put_event()

    def after_trading(self):
        """
        收盘后操作。
        """
        # 可在此记录交易日志等
        self.write_log("收盘后操作")

    def write_log(self, msg):
        """
        重写write_log方法，增加日志记录到文件
        """
        self.logger.info(msg)
        super().write_log(msg)
