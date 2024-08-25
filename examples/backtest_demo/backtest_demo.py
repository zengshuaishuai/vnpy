#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 17:30
@Author  : petrus
@Project : vnpy-master
@File    : backtest_demo.py
"""

from datetime import datetime
from importlib import reload

import vnpy_portfoliostrategy
reload(vnpy_portfoliostrategy)

from vnpy_portfoliostrategy import BacktestingEngine
from vnpy.trader.constant import Interval
from vnpy.trader.optimize import OptimizationSetting  # 设置参数的优化配置

from examples.backtest_demo.stock_database import SqlliteHandler

from vnpy_portfoliostrategy.strategies.multi_stock_momentum_strategy import MultiStockMomentumStrategy


database_path = "/Users/petrus/.vntrader/database.db"
stock_db = SqlliteHandler(database_path)

try:
    # 获取A股代码
    a_stock_symbols = stock_db.get_a_stock_symbols()
finally:
    # 关闭数据库连接
    stock_db.disconnect()

engine = BacktestingEngine()
engine.set_parameters(
    vt_symbols=a_stock_symbols,
    interval=Interval.MINUTE,
    start=datetime(2023, 6, 1),
    end=datetime(2023, 12, 29),
    rates={symbol: 0.3/10000 for symbol in a_stock_symbols},  # 每手手续费
    slippages={symbol: 0 for symbol in a_stock_symbols},
    sizes={symbol: 10 for symbol in a_stock_symbols},
    priceticks={symbol: 1 for symbol in a_stock_symbols},
    capital=1000000,
)
setting={
    "boll_window":20,
    "boll_dev":1,
}
engine.add_strategy(MultiStockMomentumStrategy, setting)

engine.load_data()
engine.run_backtesting()
df = engine.calculate_result()
engine.calculate_statistics()
engine.show_chart()


setting = OptimizationSetting()
setting.set_target("sharpe_ratio")
setting.add_parameter("boll_window", 10, 30, 1)
setting.add_parameter("boll_dev", 1, 3, 1)

# engine.run_ga_optimization(setting)
#
# engine.run_bf_optimization(setting)