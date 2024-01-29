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
import json
import os
import logging

import pandas as pd
from paddleocr import PaddleOCR, draw_ocr, paddleocr
from PIL import Image, ImageFilter, ImageEnhance
import requests
import numpy as np
import jieba
import unicodedata

logging.disable(logging.DEBUG)
pd.options.mode.chained_assignment = None
# 设置对哪一批次数据进行处理
batch_num = 2  # 从0开始，2表示第三批

# 本地没有内存溢出问题，使用该变量
breakpoint_index = 0  # 默认为0,程序中断后，从该索引重跑

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
data = pd.read_csv(prefix_path + '/data_sets/ocr_null_10w' + str(batch_num) + '.csv')
save_path = prefix_path + '/data_sets/10w' + str(batch_num) + '_data_result.csv'


def change_pic(name_st1):
    image = Image.open(name_st1)
    h = image.size[0]
    l = image.size[1]
    cropped_image = image.crop((0, 0, h, l * 0.85))
    cropped_image.save(prefix_path + '/pic_ocr/picture.jpg')


def clean_f(row):
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


def ocr_word(path):
    txts = []
    try:

        image = Image.open(path)
        s_image = image.filter(ImageFilter.UnsharpMask(radius=2, percent=100, threshold=3))
        s_image = ImageEnhance.Color(s_image)
        s_image = s_image.enhance(1.2)
        s_image.save(r"pic_ocr/ruihua.jpg")
        ocr = PaddleOCR(use_angle_cls=True, lang="ch", use_gpu=False, cpu_threads=8)
        img_path = r"pic_ocr/ruihua.jpg"
        # 输出结果保存路径
        result = ocr.ocr(img_path, cls=True)
        # result = result[0]
        txts = [line[1][0] for line in result]
    except Exception as e:
        print('ocr_word() error!', e)
    finally:
        return txts


def res_ssl(data):
    data['ocr_words'] = None
    data['id'] = data['id'].astype(str)
    # colunm = data.columns.tolist()
    # res=''
    # for temp in colunm:
    #     res += '"'
    #     res += str(temp)
    #     res += '",'
    # res += '\n'
    # f = open(save_path, 'a')
    # f.write(res)
    # f.close()
    print('breakpoint_index:', breakpoint_index)
    for i, row in data.iterrows():
        if i < breakpoint_index:
            continue
        if i == breakpoint_index + mini_batch:
            break
        print('--index:{}--'.format(i))
        data['filepath'][i] = clean_f(data['filepath'][i])
        if len(data['filepath'][i]) == 0: continue
        text = []
        pic_url_list = data['filepath'][i]
        for pici in pic_url_list:
            try:
                pic_name = pici.split('/')[-1]
                if pici.split('//')[0] == 'https:' or pici.split('//')[0] == 'http:':
                    image_url = pici
                else:
                    image_url = 'https://' + pici
                # pic_name=re.sub(r"[^a-zA-Z0-9]", "", pic_name)
                name_st = r'pic_ocr/picture.jpg'
                # if not os.path.isfile(name_st):
                r = requests.get(image_url)
                with open(name_st, 'wb') as f:
                    f.write(r.content)
                change_pic(name_st)
                text += ocr_word(name_st)
            finally:
                continue
        if len(text) == 0:
            continue
        row['ocr_words'] = ' '.join(text)
        print(text)
        # dic1=row.to_dict()
        # data_new=pd.DataFrame(dic1)
        # res=''
        # for temp in data.iloc[i]:
        #     res+='"'
        #     res+=str(temp)
        #     res+='",'
        # res+='\n'
        # f=open(save_path,'a')
        # f.write(res)
        # f.close()

        if os.path.exists(save_path) and os.path.getsize(save_path):
            row.to_frame().transpose().to_csv(save_path,
                                              mode='a',
                                              header=False,
                                              index=False)
        else:
            row.to_frame().transpose().to_csv(save_path,
                                              mode='w',
                                              index=False)

    return data


if __name__ == '__main__':
    data1 = res_ssl(data)

# nohup python -u process1.py > process1.log 2>&1 &
