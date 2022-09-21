from datetime import datetime, timezone, timedelta
import sys, os
import boto3
from bs4 import BeautifulSoup
import json
import requests #request+bs4 조합만으로도 crawling가능
import time
from botocore.client import Config

base_url = 'https://comic.naver.com/webtoon/finish?order=StarScore'
ACCESS_KEY_ID = 'ACCESS_KEY_ID'
ACCESS_SECRET_KEY = 'ACCESS_SECRET_KEY'
BUCKET_NAME = 'BUCKET_NAME'



def get_source(url):
    response = requests.get(url)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def get_link_source(url):
    soup = get_source(url)
    webtoon_list = soup.select('#content > div.list_area > ul > li > dl')
    url_link = [w.select('dt > a')[0] for w in webtoon_list if float(w.select('dd > div > strong')[0].text) >= 9.9]
    
    return list(map(lambda x: 'https://comic.naver.com'+x['href'],url_link))


def get_comic_info(url):
    soup = get_source(url)
    
    ids = url.split('/')[-1].split('=')[1] # id
    names = soup.select('#content > div.comicinfo > div.detail > h2 > span.title')[0].text # title
    auth = soup.select('#content > div.comicinfo > div.detail > h2 > span.wrt_nm')[0].text.strip() # author
    genre = soup.select('#content > div.comicinfo > div.detail > p.detail_info > span')[0].text # genre
    
    try:
        age = soup.select('#content > div.comicinfo > div.detail > p.detail_info > span.age')[0].text # age
    except:
        age = '연령가 없음'
    
    summary = soup.select('#content > div.comicinfo > div.detail > p:nth-child(2)')[0].text # summary
    thumbs = soup.select('#content > div.comicinfo > div.thumb > a')[0].find('img')['src'] # thumbnail
    return ids, names, auth, genre, age, summary, thumbs, url


def crawling(home_url):
    comic_info_json = []
    finish_list = get_link_source(home_url)
    
    for fin in finish_list:
        temp_dict = dict()
        t_IDs, t_names, t_auth, t_genre, t_age, t_sum, t_thumbs, t_urls = get_comic_info(fin)
        temp_dict['t_IDs']=t_IDs
        temp_dict['t_names']=t_names
        temp_dict['t_auth']=t_auth
        temp_dict['t_genre']=t_genre
        temp_dict['t_age']=t_age
        temp_dict['t_sum']=t_sum
        temp_dict['t_thumbs']=t_thumbs
        temp_dict['t_urls']=t_urls
        comic_info_json.append(temp_dict)
    return comic_info_json


def write_to_s3(date_str, news_type, data):
    file_name = f'{date_str}.json'
    s3_path = f'{news_type}/' + file_name
    columns = ['t_IDs', 't_names', 't_auth', 't_genre', 't_age', 't_sum', 't_thumbs', 't_urls']
    
    s3 = boto3.resource(
        's3',
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = ACCESS_SECRET_KEY,
        )
    
    body = json.dumps(data).encode('UTF-8')
    
    s3.Bucket(BUCKET_NAME).put_object(Key=s3_path, Body=body, ACL='public-read')

    
def lambda_handler(event, context):
    tz = timezone(timedelta(hours=9))
    kst_dt = datetime.now().astimezone(tz)
    date_str = kst_dt.strftime('%Y%m%d')
    
    contents = crawling(base_url)
    write_to_s3(date_str, 'fin_comic_info', contents)