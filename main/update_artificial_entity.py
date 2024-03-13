#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/3/12 10:50
# @Author  : zzx
# @File    : update_artificial_entity.py
# @Software: PyCharm
import pandas as pd


def run():
    # 读取 Excel 文件，获取 DataFrame 对象 df
    df = pd.read_csv('./temp/di_store_sku_drink_label_202403111354.csv')

    # 提取列 a 的数据并去重
    brand_name_data = df['brand_name'].unique().tolist()
    series_name_data = df['series_name'].unique().tolist()
    datas = list(set(brand_name_data + series_name_data))
    print("加入的文本行数：", len(datas))

    # 读取现有的文本文件内容
    existing_data = set()
    with open('../resources/artificial_entity.txt', 'r', encoding='utf-8') as file:
        existing_data = existing_data.union(set(file.read().splitlines()))
        line_count = len(existing_data)
    print("当前文本行数：", line_count)

    # 追加写入去重后的数据
    with open('../resources/artificial_entity.txt', 'a', encoding='utf-8') as file:
        for data in datas:
            if data and data not in existing_data:
                file.write(str(data) + '\n')
                existing_data.add(data)
                line_count += 1
    print("追加写入文本后文本总行数：", line_count)


def txt_sort():
    # 读取文件内容
    with open('../resources/artificial_entity.txt', 'r', encoding='utf-8') as file:
        lines = file.read().splitlines()

    # 根据字符串长度和字典序排序
    lines.sort(key=lambda x: (-len(x), x))

    # 写入修改后的内容
    with open('../resources/artificial_entity.txt', 'w', encoding='utf-8') as file:
        file.write('\n'.join(lines))


if __name__ == '__main__':
    #
    run()
    #
    # txt_sort()
