# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/3 18:22
# @Author  : LC
# @File    : process1.py
# @Software: PyCharm
# coding=gbk
"""
linux 4090
用于进行ocr文本提取
"""
import argparse
import json
import logging
import os
import time
from ast import literal_eval

import pandas as pd
import requests
import unicodedata

from PIL import Image, ImageFilter, ImageEnhance

from paddleocr import PaddleOCR

os.environ["CUDA_DEVICE_ORDER"] = "PCI_BUS_ID"  # 获取GPU设备，不使用GPU时注释
os.environ["CUDA_VISIBLE_DEVICES"] = "0"  # 设置GPU编号，不使用GPU时注释

logging.disable(logging.DEBUG)
pd.options.mode.chained_assignment = None

parser = argparse.ArgumentParser()
# linux有内存溢出问题，使用该变量
parser.add_argument("--bp_index", type=int, default=0)
parser.add_argument("--mini_batch", type=int, default=5000)
parser.add_argument("--batch_num", type=int, default=1)
args = parser.parse_args()
breakpoint_index = args.bp_index
mini_batch = args.mini_batch
batch_num = args.batch_num

prefix_path = "/home/DI/zhouzx/code/ocr_analysis/main"
data_path = prefix_path + "/data_sets/di_store"
default_file_path = data_path + '/di_store_unclassified_data_' + str(batch_num) + '.csv'
default_check_path = data_path + '/di_store_unclassified_data_' + str(batch_num) + '_front.csv'
default_save_path = data_path + '/di_store_unclassified_data_' + str(batch_num) + '_ocr.csv'

# csv文件的字段列表
csv_colunms = ['id', 'name', 'state', 'address', 'appcode', 'visit_num_6m', 'photos', 'filepath']


def change_pic(name_st1):
    image = Image.open(name_st1)
    h = image.size[0]
    l = image.size[1]
    cropped_image = image.crop((0, 0, h, l * 0.85))
    cropped_image.save(prefix_path + '/pic_ocr/picture.jpg')


def ocr_word(image):
    txts = []
    # try:
    p_ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, gpu_id=0, use_mp=True, mp_workers=4,
                      enable_mkldnn=True, cpu_threads=10, total_process_num=10)

    result = p_ocr.ocr(image, cls=True)
    # result = result[0]
    txts = [line[1][0] for line in result]
    # except Exception as e:
    #     print('ocr_word() error!', e)
    # finally:
    return txts


def process_image(url, photo_file):
    ocr_text = []
    response = requests.get(url)

    with open(photo_file, 'wb') as f:
        f.write(response.content)
    # 切割水印
    change_pic(photo_file)
    # ocr
    image = Image.open(photo_file)
    s_image = image.filter(ImageFilter.UnsharpMask(radius=2, percent=100, threshold=3))
    s_image = ImageEnhance.Color(s_image).enhance(1.2)
    s_image.save(prefix_path + "/pic_ocr/ruihua.jpg")

    # 提取文本
    ocr_text += ocr_word(prefix_path + "/pic_ocr/ruihua.jpg")
    return ocr_text


def exist_word(word):
    for temp in word:
        if 'CJK' in unicodedata.name(temp):
            return True
    return False


def ocr_clear(ocr_list):
    ocr_set = set(ocr_list)
    list_rub = [word for word in ocr_set if not exist_word(word)]
    new_ocr_list = list(ocr_set - set(list_rub))

    return ' '.join(new_ocr_list)


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
                print("url格式解析出错！")
    return url_list


# 针对不同数据格式进行修改
def format_url_for_filepath(row):
    url_list = []
    if str(row) == 'None' or pd.isnull(row):
        return url_list
    pici = row.replace('\\"', '\'')

    if pici.split('//')[0] == 'https:' or pici.split('//')[0] == 'http:':
        image_url = pici
    else:
        image_url = 'https://' + pici
    url_list.append(image_url)
    return url_list


def res_ssl(file_path=default_check_path, photos='photos', filepath='filepath', save_path=default_save_path):
    start0 = time.time()
    # 由于bash脚本机制问题，不能清空文件，在重跑代码时只能手动删除
    # if os.path.exists(save_path):
    #     os.remove(save_path)
    print('===开始执行OCR文本提取===')
    data = pd.read_csv(file_path)
    print("输入数据量：", data.shape[0])
    print('breakpoint_index:', breakpoint_index)

    data['id'] = data['id'].astype(str)
    for i, row in data.iterrows():
        # 控制ocr的batch，防止内存溢出过多
        if i < breakpoint_index:
            continue
        if i == breakpoint_index + mini_batch:
            break
        print('--index:{}--'.format(i))
        front_text, inside_text = [], []
        name_st = prefix_path + '/pic_ocr/picture.jpg'
        # 清洗URL格式
        if photos in row.index:
            photos_list = format_url_for_photos(row[photos])  # ++++++++++++++++++++++++++++
            # photos_list = literal_eval(row[photos])
            if len(photos_list) > 0:
                for pici in photos_list:
                    try:
                        photos_text = process_image(pici, name_st)
                        front_text.extend(photos_text)
                    finally:
                        continue
        if filepath in row.index:
            filepath_list = format_url_for_filepath(row[filepath])  # ++++++++++++++++++++++++++++
            # filepath_list = literal_eval(row[filepath])
            if len(filepath_list) > 0:
                for pici in filepath_list:
                    try:
                        filepath_text = process_image(pici, name_st)
                        inside_text.extend(filepath_text)
                    finally:
                        continue

        # 对ocr结果初步处理
        row['ocr_front_text'] = ocr_clear(front_text)
        row['ocr_inside_text'] = ocr_clear(inside_text)
        # 逐条保存到文件
        if os.path.exists(save_path) and os.path.getsize(save_path):
            row.to_frame().transpose().to_csv(save_path, mode='a', header=False, index=False)
        else:
            row.to_frame().transpose().to_csv(save_path, mode='w', index=False)

    # 把csv文件转为xlsx
    # new_save_path = save_path.replace('.csv', '.xlsx')
    # csv = pd.read_csv(save_path, index_col=0)
    # csv['id'] = csv['id'].astype(str)
    # csv.to_excel(new_save_path)
    end0 = time.time()
    print('该batch执行结束 time: {} minutes'.format((end0 - start0) / 60))


if __name__ == '__main__':
    # ocr
    res_ssl(file_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_unclassified_data_2.csv',
            # photos='front_path', filepath='inside_path',
            save_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_unclassified_data_2_ocr.csv')

# nohup python -u process1.py > process1.log 2>&1 &
