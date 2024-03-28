# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/3 18:22
# @Author  : ZZX
# @File    : process_1_front.py
# @Software: PyCharm
# coding=gbk
"""
linux T4
用于判别图片是否为店面照
"""
import argparse
import base64
import json
import logging
import os
import shutil
import time
from ast import literal_eval
from multiprocessing import Pool, Manager

import pandas as pd
import requests
from tqdm import tqdm

logging.disable(logging.DEBUG)
pd.options.mode.chained_assignment = None

prefix_path = "/home/data/temp/zhouzx/ocr_analysis/main"
data_path = prefix_path + "/data_sets/di_store"

default_file_path = data_path + '/di_store_unclassified_data_4.csv'
default_check_path = data_path + '/di_store_unclassified_data_4_front.csv'

re_url = "http://192.168.0.112:9091/imageprocess/signboard"  # 华为云

# re_url = "http://139.9.49.41:9091/imageprocess/signboard"

columns = ['id', 'name', 'state', 'address', 'appcode', 'visit_num_6m', 'photos', 'filepath']


# df对象 品类打标
# df_data = pd.read_csv(default_file_path,
#                    usecols=['id', 'name', 'storetype', 'drink_labels', 'plant', 'fruit_vegetable', 'protein',
#                             'flavored', 'tea', 'carbonated', 'coffee', 'water', 'special_uses', 'plant_clean',
#                             'fruit_vegetable_clean', 'protein_clean', 'flavored_clean', 'tea_clean', 'carbonated_clean',
#                             'coffee_clean', 'water_clean', 'special_uses_clean', 'photos', 'filepath'])


# 针对不同数据格式进行修改
def format_url_for_photos(row):
    url_list = []

    if str(row) == 'None' or pd.isnull(row):
        return url_list

    data = literal_eval(row)
    for photo_dict in data:
        if photo_dict and len(photo_dict) > 0:
            try:
                url = photo_dict.get('url')
                if photo_dict and photo_dict != '' and len(photo_dict) > 0:
                    url_list.append(url)
            except Exception as e:
                print("url格式解析出错:", e)
    return url_list


# 针对不同数据格式进行修改
def format_url_for_filepath(row):
    url_list = []

    if str(row) == 'None' or pd.isnull(row):
        return url_list

    row = row.replace('\\"', '\'')
    row = eval(row)
    if isinstance(row, str):
        row = [row]  # 变成list

    if isinstance(row, list):
        for url in row:
            if url.split('//')[0] == 'https:' or url.split('//')[0] == 'http:':
                image_url = url
            else:
                spot_num = len(url.split('.')) - 1  # url中小数点的数量
                if spot_num == 1:
                    image_url = 'http://mvs.wljhealth.net:9000/defaultBucket/' + url
                elif spot_num == 0:
                    image_url = 'http://mvs.wljhealth.net:9000/defaultBucket/' + url + '.jpg'
                else:
                    image_url = 'https://' + url
            url_list.append(image_url)
    else:
        print("filepath图片字段的格式必须是str或list:", row)
    return url_list


def recognition(image):
    try:
        with open(image, "rb") as image_file:
            base64_data = base64.b64encode(image_file.read())
        content = base64_data.decode()

        headers = {
            "User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
            "tenantId": "dtcj",
            "traceId": "dtcj",
            "content-type": "application/json"
        }  # 请求头
        payload = {
            "base64Data": content
        }

        response = requests.post(re_url, headers=headers, json=payload)
        if response.status_code == 200:
            responsedata = response.json()
            return responsedata['data']['resultdetaillist'][0]['class']
        else:
            return 0
    except Exception as e:
        return -1


# 判断图片是否是店面照 只能走本地ip 放在华为云服务器
def check_photo(row_index):
    process_id = os.getpid()
    name_st = prefix_path + '/pic_ocr/picture_' + str(process_id) + '.jpg'
    # 把图片字段(photos、filepath)的url合并为一个list
    image_url_list = []
    row = new_data_df.loc[row_index]
    # 清洗URL格式
    if 'photos' in row.index:
        photos_list = format_url_for_photos(row['photos'])
        if len(photos_list) > 0:
            for pici in photos_list:
                try:
                    if pici.split('//')[0] == 'https:' or pici.split('//')[0] == 'http:':
                        image_url = pici
                    else:
                        image_url = 'https://' + pici
                    image_url_list.append(image_url)
                finally:
                    continue

    if 'filepath' in row.index:
        filepath_list = format_url_for_filepath(row['filepath'])
        if len(filepath_list) > 0:
            for pici in filepath_list:
                try:
                    if pici.split('//')[0] == 'https:' or pici.split('//')[0] == 'http:':
                        image_url = pici
                    else:
                        image_url = 'https://' + pici
                    image_url_list.append(image_url)
                finally:
                    continue
    headers = {
        "User-Agent": "Mozilla/4.0 (compatible; MSIE 5.5; Windows NT)",
        "tenantId": "dtcj",
        "traceId": "dtcj",
        "content-type": "application/json"
    }  # 请求头
    # 区分是否为店面照
    front_path, inside_path = [], []
    image_url_list = list(set(image_url_list))
    for image_url in image_url_list:
        try:
            response = requests.get(image_url, headers=headers)
            with open(name_st, 'wb') as f:
                f.write(response.content)
            # 判断是否是店面照
            is_storefront = recognition(name_st)
            if is_storefront == '店面照':
                front_path.append(image_url)
            else:  # is_storefront == 0
                inside_path.append(image_url)
        finally:
            continue

    row['front_path'] = front_path
    row['inside_path'] = inside_path

    rows_to_save.append(row)
    return row_index


manager = Manager()
rows_to_save = manager.list()
global new_data_df


def multi_process_check_photo(file_path=default_file_path, save_path=default_check_path):
    # 清空存储图片的文件夹
    folder_path = prefix_path + '/pic_ocr'
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.makedirs(folder_path)

    start0 = time.time()
    print('===开始判断是否为店面照===')
    df_data = pd.read_csv(file_path,
                          usecols=columns)
    print("输入数据量：", df_data.shape[0])
    # 简单过滤
    global new_data_df
    new_data_df = df_data[(~df_data['photos'].isnull()) | (~df_data['filepath'].isnull())]
    print("过滤photos和filepath同时为空的数据，剩余的数据量：", new_data_df.shape[0])

    with Pool(5) as pool:
        # 使用进程池并行处理每一行
        count = pool.imap(check_photo, new_data_df.index)
        for _ in tqdm(count, total=len(new_data_df)):
            pass
    pool.close()
    pool.join()

    df_to_save = pd.DataFrame.from_records(rows_to_save)
    df_to_save['id'] = df_to_save['id'].astype(str)
    df_to_save.to_csv(save_path, index=False)

    end0 = time.time()
    print('URL店面照判别 time: {} minutes'.format((end0 - start0) / 60))


if __name__ == '__main__':
    i = 4
    # 对url重新分组，店面照为列front_path，店内照为列inside_path
    multi_process_check_photo(file_path=data_path + '/di_store_unclassified_data_{}.csv'.format(i),
                              save_path=data_path + '/di_store_unclassified_data_{}_front.csv'.format(i))

# nohup python -u process_1_front.py > process_1_front.log 2>&1 &
