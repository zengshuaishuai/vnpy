#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/15 14:42
@Author  : petrus
@Project : Project_test
@File    : get_hs_minute_k_line.py
"""
import tushare as ts
import requests
from efinance.utils import search_quote
from datetime import datetime
import pandas as pd
from efinance.utils import to_numeric


# 从本地Excel文件中读取股票代码
def get_stocks_from_excel(file_path: str) -> pd.DataFrame:
    # 假设Excel中股票代码在第一列，并且列名为 'ts_code'
    stock_info = pd.read_excel(file_path)
    return stock_info

@to_numeric
def get_quote_history_1_minute(stock_code: str, date: str) -> pd.DataFrame:
    """
    获取股票、债券的指定日期的 1 分钟 K 线数据
    """
    q = search_quote(stock_code)
    df = pd.DataFrame(columns=['股票名称', '股票代码', '日期', '时间', '最新价', '均价', '成交量', '昨收', '开盘'])
    if not q:
        return df
    data = {
        'Day': date,
        'PhoneOSNew': '2',
        'StockID': q.code,
        'Token': '0',
        'UserID': '0',
        'VerSion': '5.2.1.0',
        'a': 'GetStockTrend',
        'apiv': 'w28',
        'c': 'StockL2History'
    }
    url = 'https://apphis.longhuvip.com/w1/api/index.php'
    response = requests.post(url, data=data)

    try:
        js = response.json()
    except:
        return df
    if not js.get('trend'):
        return df
    trend = pd.DataFrame(js['trend'])
    df[['时间', '最新价', '均价', '成交量']] = trend.values[:, :4]
    df['日期'] = datetime.strptime(js['day'], '%Y%m%d').strftime('%Y-%m-%d')
    df['昨收'] = js['preclose_px']
    df['开盘'] = js['begin_px']
    df['股票代码'] = q.code
    df['股票名称'] = q.name
    return df

# 获取指定日期所有股票的分钟线数据，并写入Excel文件
def get_all_stocks_minute_data(date: str, stock_file_path: str, output_file_path: str):
    stocks = get_stocks_from_excel(stock_file_path)
    all_data = pd.DataFrame()

    for stock_code in stocks['ts_code']:
        stock_code_short = stock_code[:6]  # 获取6位股票代码
        df = get_quote_history_1_minute(stock_code_short, date)
        all_data = pd.concat([all_data, df])

    # 将所有数据写入Excel文件
    all_data.to_excel(output_file_path, index=False)
    print(f"所有股票的分钟线数据已保存到 {output_file_path} 文件中")

# 示例：读取Excel文件中的股票代码，并获取这些股票在指定日期的分钟线数据，最终写入Excel文件
date = '20220304'
stock_file_path = 'main_board_stocks.xlsx'  # 请将此路径替换为你的Excel文件路径
output_file_path = 'minute_data_output.xlsx'  # 指定输出文件的路径和名称
get_all_stocks_minute_data(date, stock_file_path, output_file_path)


