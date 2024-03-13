# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/3 18:22
# @Author  : LC
# @File    : process1.py
# @Software: PyCharm
# coding=gbk
"""
linux
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
import unicodedata
from tqdm import tqdm

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
parser.add_argument("--batch_num", type=int, default=0)
args = parser.parse_args()
breakpoint_index = args.bp_index
mini_batch = args.mini_batch
batch_num = args.batch_num

prefix_path = "/home/DI/zhouzx/code/ocr_analysis/main"
default_file_path = prefix_path + '/data_sets/ocr_null_10w-' + str(batch_num) + '.csv'
default_check_path = prefix_path + '/data_sets/10w' + str(batch_num) + '_data_result.csv'
default_save_path = prefix_path + '/data_sets/10w' + str(batch_num) + '_data_result.csv'
re_url = "http://192.168.0.112:9091/imageprocess/signboard"  # 华为云

# re_url = "http://139.9.49.41:9091/imageprocess/signboard"

# csv文件的df对象
# data = pd.read_csv('/home/data/temp/zhouzx/ocr_analysis/main/data_sets/fs_drink_sku_data_0221.csv',
#                    usecols=['id', 'name', 'storetype', 'drink_labels', 'plant', 'fruit_vegetable', 'protein',
#                             'flavored', 'tea', 'carbonated', 'coffee', 'water', 'special_uses', 'plant_clean',
#                             'fruit_vegetable_clean', 'protein_clean', 'flavored_clean', 'tea_clean', 'carbonated_clean',
#                             'coffee_clean', 'water_clean', 'special_uses_clean', 'photos', 'filepath'])
data = pd.read_csv('/home/DI/zhouzx/code/ocr_analysis/main/data_sets/fs_drink_sku_data_0221.csv',
                   usecols=['id', 'name', 'storetype', 'drink_labels', 'plant', 'fruit_vegetable', 'protein',
                            'flavored', 'tea', 'carbonated', 'coffee', 'water', 'special_uses', 'plant_clean',
                            'fruit_vegetable_clean', 'protein_clean', 'flavored_clean', 'tea_clean', 'carbonated_clean',
                            'coffee_clean', 'water_clean', 'special_uses_clean', 'photos', 'filepath'])


# data = pd.read_csv('/home/data/temp/zhouzx/ocr_analysis/main/data_sets/ocr_10w0.csv',
#                    usecols=['id', 'name', 'category1_new', 'photos', 'filepath'])

def change_pic(name_st1):
    image = Image.open(name_st1)
    h = image.size[0]
    l = image.size[1]
    cropped_image = image.crop((0, 0, h, l * 0.85))
    cropped_image.save(prefix_path + '/pic_ocr/picture.jpg')


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
    data = json.loads(row)
    for d in data:
        photos = d['filepath']
        # photos = eval(photos)
        if photos != '':
            # url = photos[0].get('url')
            url_list.append(photos)
    return url_list


def ocr_word(image):
    txts = []
    # try:
    p_ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=True, enable_mkldnn=True, cpu_threads=4, use_mp=True,
                      total_process_num=4)

    result = p_ocr.ocr(image, cls=True)
    # result = result[0]
    txts = [line[1][0] for line in result]
    # except Exception as e:
    #     print('ocr_word() error!', e)
    # finally:
    return txts


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
    s_image.save(r"pic_ocr/ruihua.jpg")

    # 提取文本
    ocr_text += ocr_word(r"pic_ocr/ruihua.jpg")
    return ocr_text


# 判断图片是否是店面照 只能走本地ip 放在华为云服务器
def check_photo(row_index):
    process_id = os.getpid()
    name_st = r'pic_ocr/picture_' + str(process_id) + '.jpg'
    # 把图片字段(photos、filepath)的url合并为一个list
    image_url_list = []
    row = data.loc[row_index]
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
    # 区分是否为店面照
    front_path, inside_path = [], []
    image_url_list = list(set(image_url_list))
    for image_url in image_url_list:
        try:
            response = requests.get(image_url)
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


def multi_process_check_photo(save_path=default_check_path):
    # 清空存储图片的文件夹
    folder_path = r'/home/data/temp/zhouzx/ocr_analysis/main/pic_ocr'
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
    os.makedirs(folder_path)

    start0 = time.time()
    print('===开始判断是否为店面照===')
    print("输入数据量：", data.shape[0])
    # 简单过滤
    data_df = data[(~data['photos'].isnull()) | (~data['filepath'].isnull())]
    print("过滤photos和filepath同时为空的数据，剩余的数据量：", data_df.shape[0])

    with Pool(5) as pool:
        # 使用进程池并行处理每一行
        count = pool.imap(check_photo, data_df.index)
        for _ in tqdm(count, total=len(data_df)):
            pass
    df_to_save = pd.DataFrame.from_records(rows_to_save)
    df_to_save['id'] = df_to_save['id'].astype(str)
    df_to_save.to_csv(save_path, index=False)

    end0 = time.time()
    print('URL店面照判别 time: {} minutes'.format((end0 - start0) / 60))


namenoise = {}
with open('/home/DI/zhouzx/code/ocr_analysis/resources/noisewords.txt', encoding='GBK') as f:
    for temp in f:
        temp = temp.rstrip('\n')
        namenoise[temp] = 1


def exist_word(word):
    for temp in word:
        if 'CJK' in unicodedata.name(temp):
            return True
    return False


def ocr_clear(ocr_list):
    ocr_list = list(set(ocr_list))
    new_ocr_list = []
    list_rub = []

    for word in ocr_list:
        if not exist_word(word):
            list_rub.append(word)
    ocr_list = [word for word in ocr_list if word not in list_rub]

    for temp in ocr_list:
        if not any(word in temp for word in namenoise):
            new_ocr_list.append(temp)

    return new_ocr_list


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
        name_st = r'pic_ocr/picture.jpg'
        # 清洗URL格式
        if photos in row.index:
            # photos_list = format_url_for_photos(row[photos])
            photos_list = literal_eval(row[photos])
            if len(photos_list) > 0:
                for pici in photos_list:
                    # try:
                    photos_text = process_image(pici, name_st)
                    front_text.extend(photos_text)
                    # finally:
                    #     continue
        if filepath in row.index:
            # filepath_list = format_url_for_filepath(row[filepath])
            filepath_list = literal_eval(row[filepath])
            if len(filepath_list) > 0:
                for pici in filepath_list:
                    # try:
                    filepath_text = process_image(pici, name_st)
                    inside_text.extend(filepath_text)
                    # finally:
                    #     continue

        # 对ocr结果初步处理
        new_front_text = ocr_clear(front_text)
        row['ocr_storefront_words'] = ' '.join(new_front_text)
        new_inside_text = ocr_clear(inside_text)
        row['ocr_words'] = ' '.join(new_inside_text)
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
    # 对url重新分组，店面照为列front_path，店内照为列inside_path
    # multi_process_check_photo(save_path='/home/data/temp/zhouzx/ocr_analysis/main/data_sets/ocr_10w0-front.csv')
    # ocr
    res_ssl(file_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/fs_drink_sku_data_temp.csv',
            photos='front_path', filepath='inside_path',
            save_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/temp.csv')

# nohup python -u process1.py > process1.log 2>&1 &
