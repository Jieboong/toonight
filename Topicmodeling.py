#!/usr/bin/env python
# coding: utf-8

# In[2]:


# 필요한 모듈 import
import pyspark
import os
import numpy as np
import datetime

from gensim import corpora
from gensim import models
import re
from konlpy.tag import Mecab

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame

from pyspark.sql.types import *
import pyspark.sql.functions as F
from pyspark.sql.functions import *


# In[3]:


class LDAtopic3:
    def __init__(self, tab):
        self.num_topic_words = 7 # topic 당 할당할 토큰 개수
        self.topic_result = []
        self.data = tab
        self.tokenizer = Mecab()        
        
    # 댓글 내용 전처리 : 이모지 등 기타 특수문자 전처리
    def preprocess(self, text):
        return re.sub('[^A-Za-z0-9가-힣]', ' ', text).strip()

    # mecab tokenizer : 일반명사(NNG), 고유명사(NNP), 형용사(VA), 어근(XR) 추출
    def m_tokenizer(self, raw, pos=['NNG', 'NNP', 'VA', 'XR']): 
        raw = self.preprocess(raw)
        return [word for word, tag in self.tokenizer.pos(raw) if len(word) > 1 and tag in pos]

    # 웹툰 ID 필터링 및 전처리
    def comment_preprocess(self, t_index):
        comics = self.data.filter(self.data.t_index == t_index).select(explode(col("t_comments"))).collect()
        temp = []
        for i in range(len(comics)):
            temp.append(self.m_tokenizer(comics[i][0]))
        return temp

    # 토큰화 처리된 문장 리스트(document)를 courpus와 dictionary로 반환
    def build_doc_word_matrix(self, docs):
        dictionary = corpora.Dictionary(docs)
        dictionary.filter_extremes(no_below=1, no_above=0.7)
        corpus = []
        for doc in docs:
            bow = dictionary.doc2bow(doc)
            corpus.append(bow)

        return corpus, dictionary

    # LDA modeling & top3 topic return
    def get_topic_keyword(self, com_list, num_topics = 5):

        corpus, dictionary = self.build_doc_word_matrix(com_list)
        # corpus = dtm based on frequency

        # print('Number of unique tokens: %d' % len(dictionary))
        # print('Number of documents: %d' % len(corpus))

        lda_model = models.ldamodel.LdaModel(corpus, num_topics=num_topics,
                                             id2word=dictionary,
                                             alpha='auto', random_state=0)
        result_top = [lda_model.show_topic(topic_id, self.num_topic_words)[0] for topic_id in range(lda_model.num_topics)]
        return list(set([r[0] for r in result_top]))[:3]
    
    def run(self, t_index):
        result = self.comment_preprocess(t_index)
        self.topic_result = self.get_topic_keyword(result)
        return self.topic_result


# In[7]:


# In[207]:


conf = SparkConf()


# In[208]:


conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY")
conf.set("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

spark = SparkSession.builder.config(conf= conf).appName("preprocessSpark").getOrCreate()


# In[209]:


# day = datetime.datetime.now()-datetime.timedelta(28)
day = datetime.datetime.now()
w_df = spark.read.json("s3a://toonightbucket/best_comment/"+ day.strftime("%Y%m%d") +".json")

for week in range(1, 6) :
    try:

        file = "s3a://toonightbucket/best_comment/"+ (day-datetime.timedelta(days=week)).strftime("%Y%m%d") +'.json'
        print(file)
        df2= spark.read.json(file)
        w_df = w_df.union(df2)
        print(w_df.count())

    except pyspark.sql.utils.AnalysisException:
        pass


# In[47]:


# conf = SparkConf()

# conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# conf.set("spark.hadoop.fs.s3a.access.key", "AWS_ACCESS_KEY")
# conf.set("spark.hadoop.fs.s3a.secret.key", "AWS_SECRET_KEY")
# conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

# spark = SparkSession.builder.config(conf= conf).appName("preprocessSpark").getOrCreate()

# # day = datetime.datetime.now()-datetime.timedelta(28)
# day = datetime.datetime.now()-datetime.timedelta(4)
# w_df = spark.read.json('../data/'+day.strftime("%Y%m%d") +".json")

# for week in range(1, 6) :
#     try:

#         file = "../data/"+ (day-datetime.timedelta(days=week)).strftime("%Y%m%d") +'.json'
#         print(file)
#         df2= spark.read.json(file)
#         w_df = w_df.union(df2)
#         print(w_df.count())

#     except pyspark.sql.utils.AnalysisException:
#         pass


# In[33]:


# t_index : N(aver)+t_IDs
w_df = w_df.withColumn('t_index', F.concat(lit('N'),F.col('t_IDs')))
w_df = w_df.drop("t_IDs")

# site : kakao webtoon(0) or naver comic(1)
w_df = w_df.withColumn("site", lit(1))

# t_auth : co-author split to list
w_df = w_df.withColumn("t_auth", split(col("t_auth")," / "))

# t_genre : co-author split to list
w_df = w_df.withColumn("t_genre", split(col("t_genre"),', '))

# sort columns
rearanged_col = ['t_index','site','t_weekdays','t_names','t_auth','t_genre','t_age','t_sum','t_thumbs','t_urls','t_comments']
w_df = w_df.select(rearanged_col)


# In[34]:


c_tab = w_df.select("t_index","t_comments")
c_tab_fil = c_tab.filter(size("t_comments")>0)


# In[36]:


result = []



for i in range(c_tab_fil.count()):
    idx = c_tab_fil.select("t_index").collect()[i][0]
    try:
        l3 = LDAtopic3(c_tab_fil).run(str(idx))
        result.append((idx,l3))
    except:
        print(idx)


# In[44]:

for rec in result:
    print(rec)


topics_df = spark.createDataFrame([(r[0],r[1]) for r in result],["t_index","t_topics"]).distinct()


# In[85]:


import sys
import os
import boto3

dynamodb = boto3.resource('dynamodb',
                          region_name = 'ap-northeast-2',
                          aws_access_key_id = 'AWS_ACCESS_KEY',
                          aws_secret_access_key='AWS_SECRET_KEY')

table = dynamodb.Table('toonightTopic')
topic_count = topics_df.count()
topic = topics_df.collect()

for number in range(0, topic_count):
    table.put_item(Item = topic[number].asDict())


# In[ ]:




