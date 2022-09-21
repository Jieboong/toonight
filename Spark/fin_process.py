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


# In[211]:


sqlContext = SQLContext(spark)


# ### Naver

# In[256]:


day = datetime.datetime.now() - datetime.timedelta(50)

w_df = spark.read.json("s3a://toonightbucket/fin_comic_info/" + day.strftime("%Y%m%d") + ".json")

# In[257]:

w_df.printSchema()

sqlContext = SQLContext(spark)





# In[258]:


w_df = w_df.withColumn('t_index', F.concat(lit('N'), F.col('t_IDs')))
w_df = w_df.drop("t_IDs")

# site : kakao webtoon(0) or naver comic(1)
w_df = w_df.withColumn("site", lit(1))


# t_genre : co-author split to list
w_df = w_df.withColumn("t_genre", split(col("t_genre"), ', '))

# sort columns
rearanged_col = ['t_index','site','t_names','t_auth','t_genre','t_age','t_sum','t_thumbs','t_urls']
w_df = w_df.select(rearanged_col)
w_df = w_df.withColumn("story_line", col('t_sum'))


# In[105]:


w_df.head(5)



# In[259]:


w_tab = w_df.dropDuplicates(['t_index']) # except t_comments

# In[108]:


w_tab.take(5)


# In[260]:


w_tab.printSchema()



# In[265]:


n_info = w_tab.select(col("t_index").alias("t_id"), col("site"), col("t_names").alias("title"), col("t_auth").alias("author"), col("t_genre").alias("genre"), col("story_line"), col("t_thumbs").alias("thumb") , col("t_urls").alias("url"))


# In[266]:


n_info.printSchema()



# In[276]:


n_info.take(5)


# In[ ]:
import sys
import os
import boto3

dynamodb = boto3.resource('dynamodb',
                          region_name = 'ap-northeast-2',
                          aws_access_key_id = 'AWS_ACCESS_KEY',
                          aws_secret_access_key='SECRET_KEY')

table = dynamodb.Table('toonightDB')
n_info_count = n_info.count()
n_info = n_info.collect()

for number in range(1, n_info_count):

#    table.put_item(atem = n_info[number])
    table.put_item(Item = n_info[number].asDict())
#    print(n_info[number].asDict())




