#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/16 15:19
@Author  : petrus
@Project : Project_test
@File    : process_kline1_script.py
"""
# import os
# import pandas as pd
# import logging
#
# # 配置日志
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s - %(levelname)s - %(message)s',
#     handlers=[
#         logging.FileHandler("process_data.log"),
#         logging.StreamHandler()
#     ]
# )
#
# # 定义源文件夹和目标文件夹
# base_folder = os.path.dirname(os.path.abspath(__file__))
# target_folder = os.path.join(base_folder, 'process_data')
#
# # 创建目标文件夹（如果不存在）
# os.makedirs(target_folder, exist_ok=True)
#
# # 获取所有需要处理的CSV文件
# csv_files = [f for f in os.listdir(base_folder) if f.startswith('new-kline') and f.endswith('.csv')]
#
# # 记录开始处理的日志
# logging.info("开始处理CSV文件...")
#
# # 遍历每个CSV文件并进行处理
# for csv_file in csv_files:
#     logging.info(f"正在处理文件: {csv_file}")
#     try:
#         # 1. 读取 CSV 文件
#         df = pd.read_csv(os.path.join(base_folder, csv_file))
#
#         # 2. 将多列股票数据转换为长格式，使每行只表示一只股票
#         df_melted = df.melt(id_vars=['datetime', 'datetime_nano'], var_name='stock_field', value_name='value')
#
#         # 3. 将 'stock_field' 列拆分为 'exchange', 'symbol', 'field'
#         df_melted[['exchange', 'symbol', 'field']] = df_melted['stock_field'].str.extract(r'(\w+)\.(\d+)\.(\w+)')
#
#         # 4. 使用 Pivot Table 将数据重新整理成每行表示一只股票的格式
#         df_final = df_melted.pivot_table(index=['datetime', 'symbol', 'exchange'], columns='field', values='value').reset_index()
#
#         # 5. 删除 'close_oi' 和 'open_oi' 字段
#         df_final = df_final.drop(columns=['close_oi', 'open_oi'], errors='ignore')
#
#         # 6. 格式化 datetime 字段
#         df_final['datetime'] = pd.to_datetime(df_final['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')
#
#         # 7. 过滤日期范围为 2023-01-01 到 2023-12-31
#         df_final = df_final[(df_final['datetime'] >= '2023-01-01') & (df_final['datetime'] <= '2023-12-31')]
#
#         # 8. 添加默认的 BarData 所需的列，并处理缺失值
#         df_final['interval'] = '1m'# 假设是1分钟线
#         df_final['turnover'] = 0.0 # 如果没有成交额数据，默认为0# 如果 'open_interest' 列不存在，则添加并填充为0if'open_interest'notin df_final.columns:
#         df_final['open_interest'] = 0
#         # 9. 重命名列以符合 BarData 的字段命名，并重新排序列的顺序
#         df_final = df_final.rename(columns={
#             'datetime': 'datetime',
#             'symbol': 'symbol',
#             'exchange': 'exchange',
#             'open': 'open_price',
#             'high': 'high_price',
#             'low': 'low_price',
#             'close': 'close_price',
#             'volume': 'volume',
#             'open_interest': 'open_interest'
#         })
#
#         # 10. 按照指定顺序重新排序列
#         df_final = df_final[['datetime', 'symbol', 'exchange', 'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'interval', 'turnover', 'open_interest']]
#
#         # 11. 排序数据：先按股票代码排序，再按时间排序
#         df_final = df_final.sort_values(by=['symbol', 'datetime']).reset_index(drop=True)
#
#         # 12. 保存处理后的CSV文件
#         output_file_name = f'bardata_{csv_file}'
#         output_file_path = os.path.join(target_folder, output_file_name)
#         df_final.to_csv(output_file_path, index=False)
#
#         logging.info(f"处理完成: {csv_file} -> {output_file_name}")
#
#     except Exception as e:
#         logging.error(f"处理文件 {csv_file} 时出错: {e}")
#
# # 提示所有文件处理完毕
# logging.info("所有文件处理完成！")



import os
import pandas as pd
import logging
import gc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("process_data.log"),
        logging.StreamHandler()
    ]
)

base_folder = os.path.dirname(os.path.abspath(__file__))
target_folder = os.path.join(base_folder, 'process_data')

# 创建目标文件夹（如果不存在）
os.makedirs(target_folder, exist_ok=True)

# 获取所有需要处理的CSV文件
csv_files = [f for f in os.listdir(base_folder) if f.startswith('new-kline') and f.endswith('.csv')]

# 记录开始处理的日志
logging.info("开始处理CSV文件...")

# 每个批次处理的行数
chunksize = 10**4  # 例如，每次处理100,000行

# 遍历每个CSV文件并进行处理
for csv_file in csv_files:
    logging.info(f"正在处理文件: {csv_file}")
    try:
        # 用于存储批次处理后的结果
        result_chunks = []

        # 逐批次读取文件
        for chunk in pd.read_csv(os.path.join(base_folder, csv_file), chunksize=chunksize):
            # 1. 将多列股票数据转换为长格式，使每行只表示一只股票
            df_melted = chunk.melt(id_vars=['datetime', 'datetime_nano'], var_name='stock_field', value_name='value')

            # 2. 将 'stock_field' 列拆分为 'exchange', 'symbol', 'field'
            df_melted[['exchange', 'symbol', 'field']] = df_melted['stock_field'].str.extract(r'(\w+)\.(\d+)\.(\w+)')

            # 3. 使用 Pivot Table 将数据重新整理成每行表示一只股票的格式
            df_final = df_melted.pivot_table(index=['datetime', 'symbol', 'exchange'], columns='field', values='value').reset_index()

            # 4. 删除 'close_oi' 和 'open_oi' 字段
            df_final = df_final.drop(columns=['close_oi', 'open_oi'], errors='ignore')

            # 5. 格式化 datetime 字段
            df_final['datetime'] = pd.to_datetime(df_final['datetime']).dt.strftime('%Y-%m-%d %H:%M:%S')

            # 6. 过滤日期范围为 2023-01-01 到 2023-12-31
            df_final = df_final[(df_final['datetime'] >= '2023-01-01') & (df_final['datetime'] <= '2023-12-31')]

            # 7. 添加默认的 BarData 所需的列，并处理缺失值
            df_final['interval'] = '1m'  # 假设是1分钟线
            df_final['turnover'] = 0.0  # 如果没有成交额数据，默认为0

            # 如果 'open_interest' 列不存在，则添加并填充为0
            if 'open_interest' not in df_final.columns:
                df_final['open_interest'] = 0

            # 8. 重命名列以符合 BarData 的字段命名，并重新排序列的顺序
            df_final = df_final.rename(columns={
                'datetime': 'datetime',
                'symbol': 'symbol',
                'exchange': 'exchange',
                'open': 'open_price',
                'high': 'high_price',
                'low': 'low_price',
                'close': 'close_price',
                'volume': 'volume',
                'open_interest': 'open_interest'
            })

            # 9. 按照指定顺序重新排序列
            df_final = df_final[['datetime', 'symbol', 'exchange', 'open_price', 'close_price', 'high_price', 'low_price', 'volume', 'interval', 'turnover', 'open_interest']]

            # 收集处理后的批次数据
            result_chunks.append(df_final)

            gc.collect()

        # 合并所有批次并按指定顺序排序
        df_final = pd.concat(result_chunks)
        df_final = df_final.sort_values(by=['symbol', 'datetime']).reset_index(drop=True)

        # 10. 保存处理后的CSV文件
        output_file_name = f'bardata_{csv_file}'
        output_file_path = os.path.join(target_folder, output_file_name)
        df_final.to_csv(output_file_path, index=False)

        logging.info(f"处理完成: {csv_file} -> {output_file_name}")

    except Exception as e:
        logging.error(f"处理文件 {csv_file} 时出错: {e}")
        gc.collect()

# 提示所有文件处理完毕
logging.info("所有文件处理完成！")



