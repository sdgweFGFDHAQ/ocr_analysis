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

data_columns = ['id', 'address', 'appcode', 'last_visittime', 'name', 'photos', 'filepath']

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
    batch_size = 100000
    offset = 0
    data_num = 8
    prefix_path = '/home/DI/zhouzx/code/ocr_analysis/main/data_sets'
    print("=====start======")
    # 创建clickhouse客户端
    clickhouse_client = ClickhouseClient(conf)

    query = "SELECT id, address, appcode, last_visittime, name, photos, filepath " \
            "FROM ai_db.ods_di_store_dedupe dsd " \
            "where (channeltype_new is null or channeltype_new = '') and category1_new = '' " \
            "and filepath <> '' and visit_num_6m > 0"

    for i in range(data_num):
        offset = i * batch_size
        print('第{}批次数据集，起始index：{}'.format(i, offset))
        sql = f"{query} LIMIT {batch_size} OFFSET {offset}" if offset != 0 else f"{query} LIMIT {batch_size}"
        result = clickhouse_client.query_data_with_raw_sql(sql=sql)
        frame = pd.DataFrame(result, columns=data_columns)
        frame['id'] = frame['id'].astype(str)
        frame.to_csv(prefix_path + '/ocr_null_10w-' + str(i) + '.csv', index=False)
        print('查询数据量：{}'.format(frame.shape[0]))
    print("=====end======")

# nohup python -u down_not_null_data.py > log.log 2>&1 &

# # 编写 ClickHouse 支持的 SQL 查询
# sql = """
#       SELECT * FROM store_tags_statistics WHERE tag_id = 2004 AND num = 70
#       """
# # 执行查询并打印结果
# result = clickhouse_client.query_data_with_raw_sql(sql)
# logging.info(result)
