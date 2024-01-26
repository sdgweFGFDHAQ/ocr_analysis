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
import pandas as pd
import requests

from PIL import Image, ImageFilter, ImageEnhance
from paddleocr import PaddleOCR

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

# prefix_path = "/home/DI/zhouzx/code/ocr_analysis/main"
# data = pd.read_csv(prefix_path + '/data_sets/ocr_null_10w' + str(batch_num) + '.csv')
# save_path = prefix_path + '/data_sets/10w' + str(batch_num) + '_data_result.csv'
prefix_path = ''
data = pd.read_csv('ocr_null_10w' + str(batch_num) + '.csv')
save_path = './temp/10w' + str(batch_num) + '_data_result.csv'
re_url = "http://192.168.0.112:9091/imageprocess/signboard"


# re_url = "http://139.9.49.41:9091/imageprocess/signboard" 华为云公网

def change_pic(name_st1):
    image = Image.open(name_st1)
    h = image.size[0]
    l = image.size[1]
    cropped_image = image.crop((0, 0, h, l * 0.85))
    cropped_image.save(prefix_path + '/pic_ocr/picture.jpg')


def format_url(row):
    url_list = []
    if str(row) == 'None':
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


def preprocess_image(image):
    s_image = image.filter(ImageFilter.UnsharpMask(radius=2, percent=100, threshold=3))
    s_image = ImageEnhance.Color(s_image).enhance(1.2)
    return s_image


def ocr_word(image):
    txts = []
    try:
        ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=False, cpu_threads=8)
        result = ocr.ocr(image, cls=True)
        txts = [line[1][0] for line in result]
    except Exception as e:
        print('ocr_word() error!', e)
    finally:
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
            return -1
    except Exception as e:
        return -1


def process_image(url, photo_file, front_text, inside_text):
    if url.split('//')[0] == 'https:' or url.split('//')[0] == 'http:':
        image_url = url
    else:
        image_url = 'https://' + url
    response = requests.get(image_url)
    with open(photo_file, 'wb') as f:
        f.write(response.content)
    # 判断是否是店面照
    is_storefront = recognition(photo_file)
    # 切割水印
    change_pic(photo_file)
    image = Image.open(photo_file)
    s_image = preprocess_image(image)
    # 提取文本
    if is_storefront == '店面照':
        front_text += ocr_word(s_image)
    else:
        inside_text += ocr_word(s_image)
    return front_text, inside_text


def res_ssl(data):
    data['ocr_words'] = None
    data['id'] = data['id'].astype(str)

    print('breakpoint_index:', breakpoint_index)
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
        if 'photos' in row.index:
            row['photos'] = format_url(row['photos'])
            if len(row['photos']) == 0:
                continue
            for pici in row['photos']:
                try:
                    front_text, inside_text = process_image(pici, name_st, front_text, inside_text)
                finally:
                    continue
        if 'filepath' in row.index:
            row['filepath'] = format_url(row['filepath'])
            if len(row['filepath']) == 0:
                continue
            for pici in row['filepath']:
                try:
                    front_text, inside_text = process_image(pici, name_st, front_text, inside_text)
                finally:
                    continue
        row['ocr_storefront_words'] = ' '.join(front_text)
        row['ocr_words'] = ' '.join(inside_text)

        if os.path.exists(save_path) and os.path.getsize(save_path):
            row.to_frame().transpose().to_csv(save_path, mode='a', header=False, index=False)
        else:
            row.to_frame().transpose().to_csv(save_path, mode='w', index=False)

    return data


if __name__ == '__main__':
    data1 = res_ssl(data)

# nohup python -u process1.py > process1.log 2>&1 &
