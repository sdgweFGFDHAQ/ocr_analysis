# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/3 10:11
# @Author  : zzx
# @File    : down_not_null_data.py
# @Software: PyCharm
# -*- encoding: utf-8 -*-
import datetime

from clickhouse_sqlalchemy import select, make_session
from sqlalchemy import create_engine, text
import pandas as pd

data_columns = ['id', 'name', 'category1_new', 'address', 'photos']

conf = {
    'user': 'default',
    'password': 'xwclickhouse2022',
    'server_host': '139.9.51.13',
    'port': 9090,
    'db': 'ai_db'
}


# 创建ClickhouseClient类
class ClickhouseClient:
    def __init__(self, conf):
        self._connection = 'clickhouse://{user}:{password}@{server_host}:{port}/{db}'.format(**conf)
        self._engine = create_engine(self._connection, pool_size=100, pool_recycle=3600, pool_timeout=20)
        self._session = make_session(self._engine)

    def query_data_with_raw_sql(self, sql):
        try:
            # 使用 text() 函数构建原生 SQL 查询
            query = text(sql)

            # 执行查询并获取结果
            return self._session.execute(query).fetchall()  # 可以使用.fetchmany(size=50000)优化
        except Exception as e:
            print('[error]', datetime.datetime.now(), e)
            return []
        finally:
            self._session.close()


if __name__ == '__main__':
    print("=====start======")
    # 创建clickhouse客户端
    clickhouse_client = ClickhouseClient(conf)
    sql = ""
    result = clickhouse_client.query_data_with_raw_sql(sql=sql)
    frame = pd.DataFrame(result, columns=data_columns)
    frame.to_csv('', index=False)
    print("=====end======")

# nohup python -u down_not_null_data.py > log.log 2>&1 &

# # 编写 ClickHouse 支持的 SQL 查询
# sql = """
#       SELECT * FROM store_tags_statistics WHERE tag_id = 2004 AND num = 70
#       """
# # 执行查询并打印结果
# result = clickhouse_client.query_data_with_raw_sql(sql)
# logging.info(result)
