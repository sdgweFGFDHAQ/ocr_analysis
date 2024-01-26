#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/12/19 10:51
# @Author  : zzx
# @File    : url_cleaning.py
# @Software: PyCharm
import json

import pandas as pd


def clean(row):
    row = row.replace('\\"', '\'')
    data = json.loads(row)
    url_list = []
    for d in data:
        photos = d['photos']
        photos = eval(photos)
        if photos:
            url = photos[0].get('url')
            url_list.append(url)
        else:
            print("未找到照片信息")
    return url_list


def process_url():
    df = pd.read_csv("../第二批ocr提取3500/ocr_extract_3500.csv")
    print(df.head())
    df['photo_url'] = df['photos'].apply(clean)
    print(df.head())
    df.to_csv("ocr_data_3500.csv", index=False)


if __name__ == '__main__':
    process_url()
