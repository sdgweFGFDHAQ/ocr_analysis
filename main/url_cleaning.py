#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2023/12/19 10:51
# @Author  : zzx
# @File    : url_cleaning.py
# @Software: PyCharm
import json

import pandas as pd


def format_url_for_photos(row):
    url_list = []
    if str(row) == 'None' or pd.isnull(row):
        return url_list
    row = row.replace('\\"', '\'')
    data = json.loads(row)
    for d in data:
        try:
            photos = d['photos']
            photos = eval(photos)
            for photo in photos:
                if photo and photo != '' and len(photo) > 0:
                    url = photo.get('url')
                    url_list.append(url)
        finally:
            continue
    return url_list


def format_url_for_filepath(row):
    url_list = []
    if str(row) == 'None' or pd.isnull(row):
        return url_list
    row = row.replace('\\"', '\'')
    # 1 特定格式的处理
    url_data = json.loads(row)
    for d in url_data:
        photos = d['filepath']
        # photos = eval(photos)
        if photos != '':
            # url = photos[0].get('url')
            url_list.append(photos)
    # 1 特定格式的处理
    url_list.append(row)
    return url_list


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
