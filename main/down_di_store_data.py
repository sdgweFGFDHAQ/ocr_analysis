# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/3 10:11
# @Author  : zzx
# @File    : down_not_null_data.py
# @Software: PyCharm
# -*- encoding: utf-8 -*-
"""
该数据是di_store未分类的数据，用于ocr规则分类
"""
import os
import time

from pyhive import hive
import pandas as pd

data_columns = ['id', 'name', 'state', 'address', 'appcode', 'visit_num_6m', 'photos', 'filepath']


def download_data():
    prefix_path = "/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/"
    conn = hive.Connection(host='124.71.220.115', port=10015, username='hive', password='xwbigdata2022',
                           database='standard_db', auth='CUSTOM')
    # conn = hive.Connection(host='192.168.0.150',port=10015,username='ai',password='ai123456',
    #                      database='standard_db',auth='CUSTOM')
    cursor = conn.cursor()

    sql = "SELECT id,name,state,address,appcode,visit_num_6m,photos,filepath FROM standard_db.di_store " \
          "WHERE channeltype_new='' and id not in (select id from standard_db.di_store_channeltype_new) " \
          "and (filepath<>'' or photos<>'') and visit_num_6m>0"
    cursor.execute(sql)
    print("=====执行sql=====")

    # 按每 20 万条数据保存为一个 CSV 文件
    chunk_size = 200000
    file_index = 1
    while True:
        rows = cursor.fetchmany(chunk_size)
        if not rows:
            break

        # 将查询结果转换为 DataFrame
        df = pd.DataFrame(rows, columns=[desc[0] for desc in cursor.description])

        # 保存为 CSV 文件 该数据是di_store未分类的数据，用于ocr规则分类
        file_name = f'di_store_unclassified_data_{file_index}.csv'
        df.to_csv(prefix_path + file_name, index=False)
        print("数据保存到文件{}".format(file_index))

        file_index += 1

    # 关闭连接
    cursor.close()
    conn.close()
    print("=====文件下载完成======")


if __name__ == '__main__':
    download_data()
