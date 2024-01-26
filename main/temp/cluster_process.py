#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2024/1/16 10:31
# @Author  : zzx
# @File    : cluster_process.py
# @Software: PyCharm
import jieba
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans
from sklearn import metrics

# 准备数据
texts = []  # 中文文本数据
with open('../../resources/artificial_entity.txt', 'r', encoding='utf-8') as file:
    for line in file.readlines():
        texts.append(line.strip())
# 文本预处理
# 例如，分词、去除停用词等
preprocessed_texts = [' '.join(jieba.lcut(text)) for text in texts]
# 数据预处理和特征提取
corpus_iterator = iter(preprocessed_texts)  # 将列表转换为迭代器

# 特征提取
vectorizer = TfidfVectorizer()
features = vectorizer.fit_transform(corpus_iterator)

# 聚类算法
k = 80  # 聚类簇数
kmeans = KMeans(n_clusters=k)
kmeans.fit(features)

# 聚类结果
labels = kmeans.labels_
data = {'text': texts, '类别': labels}
df = pd.DataFrame(data)
df.to_csv('聚类结果.csv', index=False)
# 聚类评估
silhouette_score = metrics.silhouette_score(features, labels)

print(f"Silhouette Score: {silhouette_score}")
