# !/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/4 10:49
# @Author  : ZZX
# @File    : process_3_clean.py
# @Software: PyCharm
"""
linux 4090
用于清洗ocr文本，提取实体词汇
"""
import json
import re
import time

import pandas as pd
from tqdm import tqdm

pd.set_option('mode.chained_assignment', None)

prefix_path = "/home/DI/zhouzx/code/ocr_analysis"
data_path = prefix_path + "/main/datasets/"

# 噪声词集
namenoise = {}
with open(prefix_path + '/resources/noisewords.txt', encoding='GBK') as file:
    for noise_word in file:
        noise_word = noise_word.rstrip('\n')
        namenoise[noise_word] = 1
# 实体词集
entity_words_path = prefix_path + '/resources/artificial_entity.txt'

batch_number = 1  # 控制批量数据集的文件数量
columns = ['id', 'name', 'photos', 'filepath', 'ocr_front_text', 'ocr_inside_text']  # 需要读取的字段
save_columns = ['id', 'name', 'photos', 'filepath', 'ocr_clean']  # 需要保留的字段


# 去除 , ， ! ！等字符
def clean_text(text):
    # 使用正则表达式保留中英文字符
    cleaned_text = re.sub(r"[^a-zA-Z\u4e00-\u9fa5]", " ", text)
    return cleaned_text


# 提取店名
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


# 0.从共享文件获取人工辨别的实体词汇，更新到artificial_entity.txt词集
def update_entity_words(is_clear=False):
    print("==start:update_entity_words()==")
    # 1.1
    df = pd.read_excel(prefix_path + "/main/temp/未分类数据ocr_关键词提取.xlsx", sheet_name='第一批')
    df = df[df['words'].notna() & (df['words'] != '')]
    print("人工提取词的数据量：", df.shape[0])
    entity_words = []
    # 遍历'words'列的每一行
    for lst in df['words']:
        for item in lst.split(' '):
            entity_words.append(item)

    entity_words = list(set(entity_words))

    # 1.2
    if is_clear:
        open(entity_words_path, 'w', encoding='utf-8').close()
    else:
        # 保存到txt文件
        with open(entity_words_path, 'a', encoding='utf-8') as file:
            for item in entity_words:
                file.write(item + '\n')

    print("==end:update_entity_words()==")


# 去除噪声词的核心逻辑代码
def oc_process_row(row):
    ocr_front_text = row['ocr_front_text']
    ocr_inside_text = row['ocr_inside_text']

    if pd.notna(ocr_front_text) and pd.notna(ocr_inside_text):
        ocr_word = ocr_front_text.split(' ') + ocr_inside_text.split(' ')
    elif pd.notna(ocr_front_text):
        ocr_word = ocr_front_text.split(' ')
    elif pd.notna(ocr_inside_text):
        ocr_word = ocr_inside_text.split(' ')
    else:
        return ''

    ocr_word = list(set(ocr_word))
    # 模糊匹配噪声，如电话、对联
    new_ocr_word = [word for word in ocr_word if not any(noise in word for noise in namenoise)]
    # 去除标点符号
    new_ocr_word = [clean_text(word) for word in new_ocr_word]
    return ' '.join(new_ocr_word)


# 1. 去除噪声词
def ocr_clean(file_path=data_path + '/ocr_10w0-front_ocr.csv', save_path=data_path + '/ocr_10w0-front_ocr_clean.csv'):
    """
    clear的目标字段:
        ocr_front_text,店面文本
        ocr_inside_text,店内文本
    """
    start0 = time.time()
    print("==start:ocr_clean()==")

    for batch_num in range(batch_number):
        if batch_number == 1:
            pd_file = pd.read_csv(file_path, usecols=columns)
        else:
            pd_file = pd.read_csv(
                prefix_path + '/main/data_sets/di_store_unclassified_data_' + str(batch_num) + '_ocr.csv')

        # 先去个重先
        pd_file = pd_file.drop_duplicates()
        print("0.ocr文本去噪清洗，有效数据的数据量为：", pd_file.shape[0])
        # --核心逻辑代码
        # url数据被格式化为front_path,inside_path
        pd_file['ocr_clean'] = pd_file.apply(oc_process_row, axis=1)

        pd_file = pd_file[pd_file['ocr_clean'].notna() & (pd_file['ocr_clean'] != '')]
        print("1.ocr文本去噪清洗，ocr_clean不为空的数据量为：", pd_file.shape[0])
        pd_file['id'] = pd_file['id'].astype(str)
        if batch_number == 1:
            pd_file[save_columns].to_csv(save_path, index=False)
            # pd_file[save_columns].to_excel(save_path, index=False)
        else:
            pd_file[save_columns].to_csv(
                prefix_path + '/main/data_sets/di_store_uc_data_' + str(batch_num) + '_ocr.csv', index=False)
            # pd_file[save_columns].to_excel(
            #     prefix_path + '/main/data_sets/di_store_uc_data_' + str(batch_num) + '_ocr.xlsx', index=False)
        print("==end:ocr_clean()==")
        end0 = time.time()
        print("1.ocr文本去噪清洗 执行结束所需 time: {} minutes".format((end0 - start0) / 60))


# 提取实体的核心逻辑代码
def ca_process_row(row, entity_list, word_frequency, sample):
    ocr_clean_words = row['ocr_clean']
    if pd.notna(ocr_clean_words):
        ocr_words = row['ocr_clean'].split(' ')
        new_words = []
        matched_entities = set()

        for entity in entity_list:
            for word in ocr_words:
                if entity in word:
                    if not any(matched_entity in word for matched_entity in matched_entities):
                        new_words.append(entity)
                        matched_entities.add(entity)

                        word_frequency[entity] += 1
                        if len(sample[entity]) < 20:
                            sample[entity].append(str(row['name']))

        return ' '.join(new_words)
    return ''


# 2.利用该词集进一步清洗字段ocr_clean，并把匹配到的词保留在新字段ocr_entity
def clear_again(file_path=data_path + '/ocr_10w0-front_ocr_clean.csv',
                save_path=data_path + '/ocr_10w0-front_ocr_clean.xlsx'):
    start0 = time.time()
    print("==start:clear_again()==")

    entity_list, word_frequency, sample = [], {}, {}
    with open(entity_words_path, 'r', encoding='utf-8') as f:
        for line in f.readlines():
            content = line.strip()
            if content:
                entity_list.append(content)
            word_frequency[content] = 0
            sample[content] = []

    # 按字符串长度倒序排列
    entity_list.sort(key=lambda x: -len(x))

    for batch_num in range(batch_number):
        if batch_number == 1:
            df = pd.read_csv(file_path)
            # df = pd.read_excel(file_path, sheet_name=0)
        else:
            df = pd.read_csv(
                prefix_path + '/main/data_sets/di_store_uc_data_' + str(batch_num) + '_ocr.csv')
            # df = pd.read_excel(
            #     prefix_path + '/main/data_sets/di_store_uc_data_' + str(batch_num) + '_ocr.xlsx', sheet_name=0)

        # --核心逻辑代码
        df['ocr_entity'] = df.apply(ca_process_row, args=(entity_list, word_frequency, sample), axis=1)

        # 输出提取文本的数据
        df['id'] = df['id'].astype(str)
        if batch_number == 1:
            df.to_excel(save_path, index=False)
        else:
            df.to_excel(prefix_path + '/main/data_sets/di_store_uc_data_' + str(batch_num) + '_ocr.xlsx',
                        index=False)

    end0 = time.time()
    print("==end:clear_again()==")
    print("2.ocr实体词汇提取 执行结束所需 time: {} minutes".format((end0 - start0) / 60))
    # 输出词频
    print("输出词频")
    word_frequency_df = pd.DataFrame(
        {'word': word_frequency.keys(), 'frequency': word_frequency.values(), 'sample': sample.values()})
    word_frequency_df.to_excel(prefix_path + '/main/data_sets/ocr_words_analysis.xlsx', index=False)
    print("词频输出完成")


# ocr清洗 整体流程
def run_ocr_clean(is_iterate=False):
    # 0.迭代
    # if is_iterate:
    #     update_entity_words()

    for i in [1, 3]:
        # 1.去噪
        print("=======数据文件编号[{0}]=========".format(i))
        ocr_clean(
            file_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_unclassified_data_{}_ocr.csv'
            .format(i),
            save_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_uc_data_{}_clean.csv'
            .format(i))
        # 2.提取实体
        # clear_again(
        #     file_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_uc_data_{}_clean.csv'
        #     .format(i),
        #     save_path='/home/DI/zhouzx/code/ocr_analysis/main/data_sets/di_store/di_store_uc_data_{}_clean.xlsx'
        #     .format(i))


if __name__ == '__main__':
    run_ocr_clean()
# nohup python -u process_3_clean.py > process2.log 2>&1 &
