# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/4 10:49
# @Author  : LC
# @File    : ocr_clear.py
# @Software: PyCharm
import json
from time import sleep

import pandas as pd
from tqdm import tqdm
import unicodedata

pd.set_option('mode.chained_assignment', None)

batch_number = 3

prefix_path = '/home/DI/zhouzx/code/ocr_analysis'

namenoise = {}
f = open(prefix_path + '/resources/noisewords.txt', encoding='GBK')
for temp in f:
    if temp[-1] == '\n': temp = temp[:-1]
    namenoise[temp] = 1
f.close()


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


def is_same_word(word1, word2):
    dic1 = {}
    num = 0
    num_less = min(len(word1), len(word2))
    if num_less == 0: return False
    num_less = int(0.75 * num_less)
    num_less = max(num_less, 3)
    if abs(len(word1) - len(word2)) < 2 and max(len(word1), len(word2)) <= 3 or num_less > min(len(word1), len(word2)):
        num_less = min(len(word1), len(word2))
    for index in word1:
        if index == '(' or index == ')' or index == '（' or index == '）': continue
        if index not in dic1:
            dic1[index] = 1
        else:
            dic1[index] += 1
    for index in word2:
        if index in dic1:
            num += 1
            dic1[index] -= 1
            if dic1[index] == 0:
                dic1.pop(index)
        if num >= num_less:
            return True
    return False


def exist_word(word):
    for temp in word:
        if 'CJK' in unicodedata.name(temp):
            return True
    return False


# 0. 去除噪声词
def ocr_clear():
    for batch_num in range(batch_number):
        pd_file = pd.read_csv(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_data_result.csv')
        pd_file['id'] = pd_file['id'].astype(str)
        pd_file['ocr_clear'] = None
        for i in range(len(pd_file)):
            try:
                pd_file['filepath'][i] = clean_f(pd_file['filepath'][i])
                ocr_words = pd_file['ocr_words'][i].split(' ')
                ocr_words = list(set(ocr_words))
                data_name = pd_file['name'][i]
                new_words = []
                list_rub = []
                for temp in ocr_words:
                    if not exist_word(temp):
                        list_rub.append(temp)
                ocr_words = [word for word in ocr_words if word not in list_rub]

                for temp in ocr_words:
                    if not any(word in temp for word in namenoise):
                        new_words.append(temp)
                pd_file['ocr_clear'][i] = ' '.join(new_words)
            finally:
                continue
        # res.to_csv(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_clean_data.csv', index=False)
        pd_file = pd_file[pd_file['ocr_clear'].notna() & (pd_file['ocr_clear'] != '')]
        pd_file.to_excel(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_clean_data.xlsx', index=False)


entity_words_path = prefix_path + '/resources/artificial_entity.txt'


# 1.从共享文件获取人工辨别的实体词汇，更新到artificial_entity.txt词集
def update_entity_words(is_clear=False):
    # 1.1
    df = pd.read_excel(prefix_path + "/main/temp/未分类数据ocr_关键词提取.xlsx", sheet_name='第一批')
    df = df[df['words'].notna() & (df['words'] != '')]
    print("人工提取词的数据量：", df.shape[0])
    all_words = []
    # 遍历'a'列的每一行
    for lst in df['words']:
        for item in lst.split(' '):
            all_words.append(item)

    all_words = list(set(all_words))

    # 1.2
    if is_clear:
        open(entity_words_path, 'w', encoding='utf-8').close()
    else:
        # 保存到txt文件
        with open(entity_words_path, 'a', encoding='utf-8') as file:
            for item in all_words:
                file.write(item + '\n')


# 2.利用该词集进一步清洗字段ocr_clear，并把匹配到的词保留在新字段ocr_entity
def clear_again():
    entity_list, word_frequency, sample = [], {}, {}
    with open(entity_words_path, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            content = line.strip()
            if content:
                entity_list.append(content)
            word_frequency[content] = 0
            sample[content] = []

    for batch_num in range(batch_number):
        df = pd.read_excel(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_clean_data.xlsx', sheet_name=0)
        df['ocr_entity'] = None
        for i in range(len(df)):
            ocr_words = df['ocr_clear'][i].split(' ')
            ocr_words = list(set(ocr_words))

            new_words = []
            for entity in entity_list:
                if any(entity in word for word in ocr_words):
                    new_words.append(entity)
                    word_frequency[entity] += 1
                    if len(sample[entity]) < 20:
                        sample[entity].append(str(df['name'][i]))
            df['ocr_entity'][i] = ' '.join(new_words)
        # 输出提取文本的数据
        df['id'] = df['id'].astype(str)
        # df.to_csv(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_clean_data.csv', index=False)
        df.to_excel(prefix_path + '/main/data_sets/10w' + str(batch_num) + '_clean_data.xlsx', index=False)
    # 输出词频
    word_frequency_df = pd.DataFrame(
        {'word': word_frequency.keys(), 'frequency': word_frequency.values(), 'sample': sample.values()})
    word_frequency_df.to_excel(prefix_path + '/main/data_sets/ocr_words_analysis.xlsx', index=False)
    print("===end===")


if __name__ == '__main__':
    # 1.去噪
    # ocr_clear()
    # 2.迭代
    # update_entity_words()
    # clear_again()
    print("")
# nohup python -u process2.py > process2.log 2>&1 &
