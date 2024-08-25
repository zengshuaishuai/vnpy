#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 14:36
@Author  : petrus
@Project : Project_test
@File    :get_hs_symbol.py
"""
import tushare as ts
import pandas as pd

# 初始化Tushare API（你需要先在Tushare官网申请一个API token）
ts.set_token('f60261c5afe36d13be2b77f19fcdb99a1974dc5c0039fdc528f52948')
pro = ts.pro_api()

# 获取沪深主板A股的股票代码
stock_info = pro.stock_basic(exchange='', list_status='L', fields='ts_code,name')

# 过滤主板股票
# 主板股票的code一般以'000'或'600'开头
main_board_stocks = stock_info[(stock_info['ts_code'].str.startswith('000')) |
                               (stock_info['ts_code'].str.startswith('600'))]

# 将获取到的股票代码保存到一个Excel文件中
main_board_stocks.to_excel('main_board_stocks.xlsx', index=False)

print("股票代码已保存到 main_board_stocks.xlsx 文件中")


