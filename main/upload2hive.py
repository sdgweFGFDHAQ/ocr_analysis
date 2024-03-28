# -*- coding:utf-8 -*-
######################################################
# 描述：用于读取算法服务器csv文件写入di_store_dedupe_labeling
# 修改记录：
# 日期           版本       修改人    修改原因说明
# 2023/02/20     V1.00      lrz      新建代码
######################################################
import os
import sys
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from recall_tools.pyspark_common import pySparkCm
import csv
from recall_tools.ssh import SSH
import gc

# 清空目标表数据
# table_name = 'di_store_dedupe_ifexists'
# cm = pySparkCm(table_name)
# spark = cm.sparkenv()
# spark.sql("truncate table standard_db.di_store_dedupe_ifexists")
# print("table truncate complete")

# 数据集路径
path = "/home/data/temp/zhouzx/ocr_analysis/main/data_sets/di_store"

# 获取目标文件名
csv_files = [path + '/di_store_uc_data_1_clean.csv', path + '/di_store_uc_data_3_clean.csv']
# for i in range(5, 6):
#     csv_files.append(path + '/di_store_uc_data_{}_clean.csv'.format(i))
print("1.获取到所有的文件名:", csv_files)

for csv_path in csv_files:
    # 远程连接服务器读取文件
    ssh = SSH()
    connect = ssh.connect()
    sftp_client = connect.open_sftp()
    # 读取csv文件输出字典
    data = []
    try:
        with sftp_client.open(csv_path) as f:
            for row in csv.DictReader(f, skipinitialspace=True):
                dict_line = dict(row)
                data.append(dict_line)
    except Exception as ex:
        sys.exit(ex)

    sftp_client.close()
    ssh.close()
    print("2.read data success: " + str(len(data)))

    # 定义变量
    table_name = 'di_store_add_ocr_entity'
    cm = pySparkCm(table_name)
    spark = cm.sparkenv()

    df = spark.createDataFrame(data) \
        .selectExpr('id', 'name', 'photos', 'filepath', 'ocr_clean')
    print('==========df===============')
    df.printSchema()

    # 写入表
    print("3.write data count=" + str(df.count()))
    cm.write_to_hudi(df, 'standard_db', table_name, 'id', '', 'id', 'append', 'insert')
    print("4.Complete!")
