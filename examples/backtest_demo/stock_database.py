#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
@Time    : 2024/8/19 14:41
@Author  : petrus
@Project : vnpy-master
@File    : stock_database.py
"""

import sqlite3


class SqlliteHandler:
    def __init__(self, database_path: str):
        """
        初始化StockDatabase类，连接到SQLite数据库。
        """
        self.database_path = database_path
        self.connection = None

    def connect(self):
        """连接到SQLite数据库。"""
        self.connection = sqlite3.connect(self.database_path)

    def disconnect(self):
        """关闭SQLite数据库连接。"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def get_a_stock_symbols(self) -> list[str]:
        """
        从SQLite数据库中获取所有A股的股票代码。
        """
        if not self.connection:
            self.connect()

        cursor = self.connection.cursor()

        # 查询股票信息
        query = """
        SELECT distinct symbol, exchange 
        FROM dbbaroverview 
        WHERE exchange IN ('SSE')
        """

        cursor.execute(query)
        results = cursor.fetchall()

        # 构造股票代码列表
        a_stock_symbols = [f"{symbol}.{exchange}" for symbol, exchange in results]

        return a_stock_symbols


