#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#!/usr/bin/env python
# coding: utf-8

# In[173]:


import pyspark
import os
import pandas as pd
import numpy as np


# In[206]:


from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf
from pyspark.sql.types import *

import pyspark.sql.functions as F
from pyspark.sql.functions import *

import datetime


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


# In[210]:


df.head(5)


# In[211]:


sqlContext = SQLContext(spark)


# In[212]:


df.registerTempTable("toon")


# In[213]:


## 우리 웹툰 고유 번호 tid
df = sqlContext.sql("SELECT *, CONCAT('K', id) as t_id FROM toon" )


# In[214]:


df.printSchema()


# In[215]:


df.registerTempTable("tid_df")


# ##### 웹툰 댓글 테이블
# -> df_review를 table로 export

# In[216]:


reviewTable = sqlContext.sql("select t_id,best_comments, key_words from tid_df")
reviewTable.printSchema()



# ### Naver

# In[256]:


day = datetime.datetime.now()

w_df = spark.read.json("s3a://toonightbucket/best_comment/"+ day.strftime("%Y%m%d") +".json")


for week in range(1,6) :
    try:

        file ="s3a://toonightbucket/best_comment/"+ (day-datetime.timedelta(days=week)).strftime("%Y%m%d") +'.json'
        df2= spark.read.json(file)
        w_df = w_df.union(df2)

    except pyspark.sql.utils.AnalysisException:
        pass

# In[257]:


w_df.printSchema()


# In[258]:


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


# In[105]:


w_df.head(5)



# In[259]:


w_tab = w_df.drop("t_comments").dropDuplicates(['t_index']) # except t_comments
c_tab = w_df.select("t_index","t_comments")


# In[108]:


c_tab.take(5)


# In[260]:


c_tab.printSchema()


# In[261]:


reviewTable.printSchema()


# In[265]:

n_info = c_tab.select(col("t_index").alias("t_id"), col("t_comments").alias("best_comments"))

# In[266]:


n_info.printSchema()


# In[269]:


k_info = reviewTable.select(col("t_id"), col("best_comments"))


# In[270]:


k_info.printSchema()


# In[271]:


info = n_info.union(k_info)


# In[276]:


info.take(5)


# In[ ]:
import sys
import os
import boto3

dynamodb = boto3.resource('dynamodb',
                          region_name = 'ap-northeast-2',
                          aws_access_key_id = 'AWS_ACCESS_KEY',
                          aws_secret_access_key='AWS_SECRET_KEY')

table = dynamodb.Table('toonightComment')
info_count = info.count()
info = info.collect()

for number in range(1, info_count):

#    table.put_item(atem = info[number])
    table.put_item(Item = info[number].asDict())
#    print(info[number].asDict())





