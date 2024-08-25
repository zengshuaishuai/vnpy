#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 14:59
@Author  : petrus
@Project : vnpy-master
@File    : multi_stock_momentum_backtest.py
"""

# from datetime import datetime
#
# from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
# from vnpy_ctastrategy.strategies.multi_stock_momentum_strategy import MultiStockMomentumStrategy
#
# from vnpy.trader.constant import Interval
#
# a_stock_symbols=["600011.SSE"]
#
# # 创建回测引擎
# engine = BacktestingEngine()
#
# for symbol in a_stock_symbols:
#     # 设置回测参数（基础参数）
#     engine.set_parameters(
#         vt_symbol=symbol,
#         mode=BacktestingMode.BAR,  # 使用K线数据回测
#         interval=Interval.MINUTE,  # 使用分钟线数据
#         start=datetime(2023, 1, 1),  # 设置回测开始时间
#         end=datetime(2023, 12, 29),  # 设置回测结束时间
#         rate=0.0003,  # 设置交易手续费，万分之三
#         slippage=0.2,  # 设置滑点
#         size=100,  # 每手合约的大小（例如股票的每手数量为100股）
#         pricetick=0.01,  # 最小价格变动单位
#         capital=1000000,  # 设置初始资金
#     )
#     # 添加策略
#     engine.add_strategy(MultiStockMomentumStrategy, {})
#     # 加载历史数据
#     engine.load_data()
#     # 运行回测
#     engine.run_backtesting()
#
# # 计算结果
# df = engine.calculate_result()
# engine.calculate_statistics()
#
# # 显示图表
# fig = engine.show_chart()
# fig.show()


##################################振幅因子###########################
# from datetime import datetime
# from vnpy.trader.database import get_database
# from vnpy.trader.constant import Interval, Exchange
# from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
# from vnpy_ctastrategy.strategies.monthly_volatility_strategy import MonthlyVolatilityStrategy
#
# # 从数据库中提取所有沪深A股代码（中证1000成分股）
# def get_csi1000_symbols():
#     database = get_database()
#     symbol_infos = database.get_all_contracts()
#     a_stock_symbols = []
#     for symbol, exchange, name in symbol_infos:
#         if exchange in [Exchange.SSE, Exchange.SZSE]:  # 过滤上海证券交易所和深圳证券交易所的股票
#             a_stock_symbols.append(f"{symbol}.{exchange.value}")
#     return a_stock_symbols
#
# # 设置回测引擎和回测参数
# engine = BacktestingEngine()
#
# engine.set_parameters(
#     mode=BacktestingMode.BAR,
#     interval=Interval.DAILY,
#     start=datetime(2021, 1, 1),
#     end=datetime(2023, 6, 30),
#     rate=0.0003,
#     slippage=0.2,
#     size=100,
#     pricetick=0.01,
#     capital=10000000,
# )
#
# # 获取中证1000的股票代码列表
# symbols = get_csi1000_symbols()
#
# # 为每只股票运行回测
# for symbol in symbols:
#     engine.set_parameters(vt_symbol=symbol)
#     engine.add_strategy(MonthlyVolatilityStrategy, {})
#
# # 运行回测
# engine.load_data()
# engine.run_backtesting()
#
# # 计算结果和显示
# df = engine.calculate_result()
# engine.calculate_statistics()
# fig = engine.show_chart()
# fig.show()


from datetime import datetime

from vnpy_ctastrategy.backtesting import BacktestingEngine, BacktestingMode
from vnpy_ctastrategy.strategies.multi_stock_momentum_strategy import MultiStockMomentumStrategy

from vnpy.trader.constant import Interval

# 股票代码列表
a_stock_symbols = ["600062.SSE","600011.SSE","600111.SSE","600052.SSE","600008.SSE","600030.SSE","600017.SSE","600021.SSE","600026.SSE","600020.SSE","600051.SSE","600029.SSE"]

# 创建回测引擎
engine = BacktestingEngine()

# 设置回测的基础参数
base_parameters = {
    "mode": BacktestingMode.BAR,  # 使用K线数据回测
    "interval": Interval.MINUTE,  # 使用分钟线数据
    "start": datetime(2023, 1, 1),  # 设置回测开始时间
    "end": datetime(2023, 12, 29),  # 设置回测结束时间
    "rate": 0.0003,  # 设置交易手续费，万分之三
    "slippage": 0.2,  # 设置滑点
    "size": 100,  # 每手合约的大小（例如股票的每手数量为100股）
    "pricetick": 0.01,  # 最小价格变动单位
    "capital": 1000000,  # 设置初始资金
}

# 对每只股票进行回测
for symbol in a_stock_symbols:
    # 为每只股票设置单独的 vt_symbol 参数
    engine.set_parameters(
        vt_symbol=symbol,
        **base_parameters
    )

    # 添加策略
    engine.add_strategy(MultiStockMomentumStrategy, {})

    # 加载历史数据
    engine.load_data()

    # 运行回测
    engine.run_backtesting()

    # 计算结果
    df = engine.calculate_result()
    engine.calculate_statistics()

    # 显示图表
    fig = engine.show_chart()
    fig.show()








