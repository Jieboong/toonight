#!/usr/bin/env python
# coding: utf-8

# In[23]:


#!/usr/bin/env python
# coding: utf-8

# 필요한 모듈 import
import pyspark
import os
import pandas as pd
import numpy as np
import datetime
import random

import re
from konlpy.tag import Mecab
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import linear_kernel

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.conf import SparkConf


from pyspark.sql.types import *
import pyspark.sql.functions as F

from pyspark.sql.functions import *

import datetime

# In[40]:


class SimReview:
    def __init__(self, i_tab, c_tab):
        self.info_tab = i_tab
        self.com_tab = c_tab
        self.tokenizer = Mecab()        
        
    # 댓글 내용 전처리 : 이모지 등 기타 특수문자 전처리
    def preprocess(self, text):
        return re.sub('[^A-Za-z0-9가-힣]', ' ', text).strip()

    # mecab tokenizer : 일반명사(NNG), 고유명사(NNP), 형용사(VA), 어근(XR) 추출
    def m_tokenizer(self, raw, pos=['NNG', 'NNP', 'VA', 'XR']): 
        raw = self.preprocess(raw)
        return [word for word, tag in self.tokenizer.pos(raw) if len(word) > 1 and tag in pos]
    
    def set_user_info(self, review_idx, fav_g, review):
        self.user_rev_idx = review_idx
        if type(fav_g) is list:
            self.user_fav_g = random.choice(fav_g) # fav_g : list
        elif type(fav_g) is str:
            self.user_fav_g = fav_g
        else:
            self.user_fav_g = '스토리' # 고정값 임의설정
        self.user_review = review
        
    
    def get_tfidf_vec(self, user_rev, total_rev):
        # total_rev => list, user_rev => String
        tfidf_gen = TfidfVectorizer(tokenizer=self.m_tokenizer, max_features=2000)
        total_mat = tfidf_gen.fit_transform(total_rev)
        user_mat = tfidf_gen.transform([user_rev])
        return total_mat, user_mat
    
    def top10_indices(self, total_mat, user_mat):
        cos_sim = linear_kernel(user_mat, total_mat)[0]
        cos_sim_score = list(enumerate(cos_sim))
        cos_sim_score = sorted(cos_sim_score, key = lambda x: x[1], reverse=True)

        score = cos_sim_score[1:21]
        print('max score is ',score[0][1])
        tag_indices = [i[0] for i in score if i[1]>0]
        return tag_indices
    
    def run(self):
        temp_tab = self.com_tab.select("t_id",explode(self.com_tab.best_comments).alias("best_comments")).where(col('t_id') != self.user_rev_idx)
        t_rev = [row['best_comments'] for row in temp_tab.collect()]
        t_mat, u_mat = self.get_tfidf_vec(self.user_review,t_rev)
        tag_indices = self.top10_indices(t_mat,u_mat)
        result = []
        for i in tag_indices:
            result.append(temp_tab.collect()[i]) # (t_id, cosine_similarity)
        
        sim_tab = spark.createDataFrame(list(dict.fromkeys(result)),['t_id','sim_score']) # to df
        sw_tab = self.info_tab.join(sim_tab,self.info_tab.t_id == sim_tab.t_id,"inner").drop(sim_tab.t_id)
        
        # multiply user favorite genre
        top5_row = sw_tab.withColumn('modi_sim_score',when(F.array_contains(col('genre'), self.user_fav_g), col("sim_score")*1.3).otherwise(col('sim_score')))        .sort(col('modi_sim_score').desc()).select('t_id').head(5)
        top5 = [r['t_id'] for r in top5_row]
        
        return self.user_rev_idx, top5


# In[15]:


# In[207]:


conf = SparkConf()

conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
conf.set("spark.hadoop.fs.s3a.access.key", "ACCESS_KEY")
conf.set("spark.hadoop.fs.s3a.secret.key", "SECRET_KEY")
conf.set("spark.hadoop.fs.s3a.endpoint", "s3.ap-northeast-2.amazonaws.com")

spark = SparkSession.builder.config(conf= conf).appName("recommendSpark").getOrCreate()


# In[209]:


# day = datetime.datetime.now()-datetime.timedelta(28)
day = datetime.datetime.now()

df = spark.read.json("s3a://toonightbucket/kakao/"+ day.strftime("%Y-%m-%d") +".json")

for week in range(1, 6) :
    try:

        file = "s3a://toonightbucket/kakao/"+ (day-datetime.timedelta(days=week)).strftime("%Y-%m-%d") +'.json'
        print(file)
        df2= spark.read.json(file)
        df = df.union(df2)
        print(df.count())

    except pyspark.sql.utils.AnalysisException:
        pass


sqlContext = SQLContext(spark)


# In[212]


df.registerTempTable("toon")


# In[213]:


## 우리 웹툰 고유 번호 tid
df = sqlContext.sql("SELECT *, CONCAT('K', id) as t_id FROM toon" )


# In[215]:


df.registerTempTable("tid_df")


# ##### 웹툰 댓글 테이블
# -> df_review를 table로 export

# In[216]:


reviewTable = sqlContext.sql("select t_id,best_comments, key_words from tid_df")
reviewTable.printSchema()


# ##### 웹툰 정보 테이블

# In[217]:


infoTable = df.drop('best_comments', 'key_words')


# In[218]:


infoTable = infoTable.withColumn("genre", split(col("genre"), "/")).withColumn("author", when(infoTable.illustrator == infoTable.story_writer, df.illustrator)
           .otherwise(concat(df.story_writer,lit('/'), df.illustrator)))


# In[219]:


infoTable = infoTable.drop('story_writer', 'illustrator')
infoTable = infoTable.withColumn("site", lit(0))


# ### Naver

# In[256]:


day = datetime.datetime.now()-datetime.timedelta(1)

w_df = spark.read.json("s3a://toonightbucket/best_comment/"+ day.strftime("%Y%m%d") +".json")


for week in range(1,6) :
    try:

        file ="s3a://toonightbucket/best_comment/"+ (day-datetime.timedelta(days=week)).strftime("%Y%m%d") +'.json'
        df2= spark.read.json(file)
        w_df = w_df.union(df2)

    except pyspark.sql.utils.AnalysisException:
        pass
    

w_df = w_df.withColumn('t_index', F.concat(lit('N'),F.col('t_IDs')))
w_df = w_df.drop("t_IDs")

# site : kakao webtoon(0) or naver comic(1)
w_df = w_df.withColumn("site", lit(1))

# t_genre : co-author split to list
w_df = w_df.withColumn("t_genre", split(col("t_genre"),', '))
# sort columns
rearanged_col = ['t_index','site','t_weekdays','t_names','t_auth','t_genre','t_age','t_sum','t_thumbs','t_urls','t_comments']
w_df = w_df.select(rearanged_col)
w_df = w_df.withColumn("story_line", col('t_sum'))


w_tab = w_df.drop("t_comments").dropDuplicates(['t_index']) # except t_comments
c_tab = w_df.select("t_index","t_comments")




n_info = w_tab.select(col("t_index").alias("t_id"), col("site"), col("t_names").alias("title"), col("t_weekdays").alias("weekdays"), col("t_auth").alias("author"), col("t_genre").alias("genre"), col("story_line"), col("t_thumbs").alias("thumb") , col("t_urls").alias("url"))
k_info = infoTable.select(col("t_id"), col("site"), col("title"), col("day").alias("weekdays"), col("author"),col("genre"), col("story_line"), col("picUrl").alias("thumb"), col("url"))
info = n_info.union(k_info)

n_comment = c_tab.select(col("t_index").alias("t_id"), col("t_comments").alias("best_comments"))
k_comment = reviewTable.select(col("t_id"), col("best_comments"))
comment = n_comment.union(k_comment)


# In[58]:

sr = SimReview(info,comment)
sr.set_user_info('N748105', '에피소드', '귀안들려도 괜찮아! 다른감각으로 찾으면되지~다른감각 다 무뎌져도 괜찮아 내가 다 찾아주고 필요한거 챙겨주면되지! 이제껏 말못해도 서로의 교감으로 알아들은것처럼 우리는 그렇게 오랫동안 함께할수있어 나이들며 서서히 약해지는건 받아들이고 우리둘다 건강챙기면서 맛난거먹고 좋은거 함께보고 가늘고 길게살자~ 사랑해 우리 미미')
print(sr.run())


# In[ ]:




