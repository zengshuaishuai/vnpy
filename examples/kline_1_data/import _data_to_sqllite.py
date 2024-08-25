#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/16 17:22
@Author  : petrus
@Project : vnpy-master
@File    : import _data_to_sqllite.py
"""

import logging
import os

import pandas as pd
import pytz
from peewee import SqliteDatabase as PeeweeSqliteDatabase, chunked
from vnpy_sqlite.sqlite_database import DbBarData, DbTickData, DbBarOverview, DbTickOverview

from vnpy.trader.constant import Exchange, Interval
from vnpy.trader.database import BaseDatabase, convert_tz
from vnpy.trader.object import BarData
from vnpy.trader.utility import get_file_path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # 输出到控制台
        logging.FileHandler("process_data.log")  # 输出到文件
    ]
)

class SqliteDatabase(BaseDatabase):
    def __init__(self, db_path: str = None):
        if db_path is None:
            db_path = str(get_file_path("kline1.db"))  # 默认路径
        self.db = PeeweeSqliteDatabase(db_path)
        self.db.connect()
        self.db.create_tables([DbBarData, DbTickData, DbBarOverview, DbTickOverview])
        logging.info(f"Connected to SQLite database at {db_path}")

    def save_bar_data(self, bars):
        bar = bars[0]
        symbol = bar.symbol
        exchange = bar.exchange
        interval = bar.interval

        data = []
        for bar in bars:
            if bar.datetime.tzinfo is None:
                bar.datetime = pd.Timestamp(bar.datetime).tz_localize(pytz.UTC)
            else:
                bar.datetime = convert_tz(bar.datetime)

            original_datetime = bar.datetime
            bar.datetime = bar.datetime.strftime('%Y-%m-%d %H:%M:%S')

            d = bar.__dict__
            d["exchange"] = d["exchange"].value
            d["interval"] = d["interval"].value
            d.pop("gateway_name")
            d.pop("vt_symbol")
            data.append(d)

        with self.db.atomic():
            for c in chunked(data, 1000):
                DbBarData.insert_many(c).on_conflict_replace().execute()

        # 更新K线汇总数据
        overview = DbBarOverview.get_or_none(
            DbBarOverview.symbol == symbol,
            DbBarOverview.exchange == exchange.value,
            DbBarOverview.interval == interval.value,
        )

        if not overview:
            overview = DbBarOverview()
            overview.symbol = symbol
            overview.exchange = exchange.value
            overview.interval = interval.value
            overview.start = bars[0].datetime
            overview.end = bars[-1].datetime
            overview.count = len(bars)
        else:
            overview_start = pd.to_datetime(overview.start, utc=True)
            overview_end = pd.to_datetime(overview.end, utc=True)

            overview.start = min(original_datetime, overview_start).strftime('%Y-%m-%d %H:%M:%S')
            overview.end = max(original_datetime, overview_end).strftime('%Y-%m-%d %H:%M:%S')
            #overview.count += len(bars)
            s = DbBarData.select().where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
            )
            overview.count = s.count()

        overview.save()

        logging.info(f"Saved {len(bars)} bars for {symbol} to database.")

        return True

    def load_bar_data(self, symbol, exchange, interval, start, end):
        s = (
            DbBarData.select()
                .where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
                & (DbBarData.datetime >= start)
                & (DbBarData.datetime <= end)
            )
                .order_by(DbBarData.datetime)
        )

        bars = []
        for db_bar in s:
            bar = BarData(
                symbol=db_bar.symbol,
                exchange=Exchange(db_bar.exchange),
                datetime=db_bar.datetime,
                interval=Interval(db_bar.interval),
                volume=db_bar.volume,
                turnover=db_bar.turnover,
                open_interest=db_bar.open_interest,
                open_price=db_bar.open_price,
                high_price=db_bar.high_price,
                low_price=db_bar.low_price,
                close_price=db_bar.close_price,
                gateway_name="DB",
            )
            bars.append(bar)
        return bars

    def delete_bar_data(self, symbol, exchange, interval):
        query = (
            DbBarData.delete()
                .where(
                (DbBarData.symbol == symbol)
                & (DbBarData.exchange == exchange.value)
                & (DbBarData.interval == interval.value)
            )
        )
        return query.execute()

    def get_bar_overview(self):
        s = DbBarOverview.select()
        overviews = []
        for overview in s:
            overview.exchange = Exchange(overview.exchange)
            overview.interval = Interval(overview.interval)
            overviews.append(overview)
        return overviews

    # 以下方法与TICK数据处理相关，可根据需要实现
    def save_tick_data(self, ticks):
        pass

    def load_tick_data(self, symbol, exchange, start, end):
        pass

    def delete_tick_data(self, symbol, exchange):
        pass

    def get_tick_overview(self):
        pass


def process_all_csv_files(db, folder_path):
    for filename in os.listdir(folder_path):
        if filename.endswith(".csv"):
            csv_file_path = os.path.join(folder_path, filename)
            logging.info(f"Processing file: {csv_file_path}")

            try:
                df = pd.read_csv(csv_file_path)
                df['interval'] = df['interval'].apply(lambda x: Interval(x))

                bars = []
                for _, row in df.iterrows():
                    bar = BarData(
                        symbol=row['symbol'],
                        exchange=Exchange(row['exchange']),
                        interval=Interval(row['interval']),
                        datetime=pd.to_datetime(row['datetime']),
                        volume=row['volume'],
                        turnover=row['turnover'],
                        open_interest=row['open_interest'],
                        open_price=row['open_price'],
                        high_price=row['high_price'],
                        low_price=row['low_price'],
                        close_price=row['close_price'],
                        gateway_name="DB"
                    )
                    bars.append(bar)

                success = db.save_bar_data(bars)
                logging.info(f"Data saved for {filename}: {success}")
            except Exception as e:
                logging.error(f"Error processing file {filename}: {e}")


# 初始化数据库
db = SqliteDatabase()

# 获取 process_data 目录的路径
base_folder = os.path.dirname(os.path.abspath(__file__))
csv_folder_path = os.path.join(base_folder, 'process_data')

# 处理 process_data 目录中的所有 CSV 文件
process_all_csv_files(db, csv_folder_path)


# #5. 读取并检查数据库中的内容
# loaded_bars = db.load_bar_data(
#     symbol="600017",
#     exchange=Exchange.SSE,
#     interval=Interval.MINUTE,
#     start=datetime(2023, 1, 1),
#     end=datetime(2023, 12, 29)
# )
#
# for bar in loaded_bars:
#     print(bar.datetime, bar.open_price, bar.high_price, bar.low_price, bar.close_price,bar.exchange,bar.interval,bar.volume,bar.turnover,bar.open_interest,bar.symbol,bar.gateway_name)
#
#
# # 6. 检查数据库中的汇总信息
# overviews = db.get_bar_overview()
# for overview in overviews:
#     print(f"ID:{overview.id},Symbol: {overview.symbol}, Exchange: {overview.exchange}, Interval: {overview.interval}, Start: {overview.start}, End: {overview.end}, Count: {overview.count}")
