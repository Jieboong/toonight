# 최신 lambda
from selenium.webdriver.chrome.options import Options 
from selenium import webdriver
from datetime import datetime, timezone, timedelta
import sys, os
import boto3
from bs4 import BeautifulSoup
import json
import requests #request+bs4 조합만으로도 crawling가능
import time
from botocore.client import Config

ACCESS_KEY_ID = 'ACCESS_KEY_ID'
ACCESS_SECRET_KEY = 'ACCESS_SECRET_KEY'
BUCKET_NAME = 'BUCKET_NAME'

base_url = 'https://comic.naver.com/webtoon/weekday.nhn'


def drive(url): 
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1280x1696')
    chrome_options.add_argument('--user-data-dir=/tmp/user-data')
    chrome_options.add_argument('--hide-scrollbars')
    chrome_options.add_argument('--enable-logging')
    chrome_options.add_argument('--log-level=0')
    chrome_options.add_argument('--v=99')
    chrome_options.add_argument('--single-process')
    chrome_options.add_argument('--data-path=/tmp/data-path')
    chrome_options.add_argument('--ignore-certificate-errors')
    chrome_options.add_argument('--homedir=/tmp')
    chrome_options.add_argument('--disk-cache-dir=/tmp/cache-dir')
    chrome_options.binary_location = "/opt/python/bin/headless-chromium" 
    
    driver = webdriver.Chrome('/opt/python/bin/chromedriver', chrome_options=chrome_options)#driver 객체 불러옴 
    driver.implicitly_wait(3) # 3초 후에 작동하도록 
    driver.get(url) #url에 접속
    
    html = driver.page_source #현재 driver에 나타난 창의 page_source(html) 가져오기 
    soup = BeautifulSoup(html, 'html.parser') #html 파싱(parsing)을 위해 BeautifulSoup에 넘겨주기 
    return driver, soup


def get_source(url):
    response = requests.get(url)
    html = response.text
    soup = BeautifulSoup(html, 'html.parser')
    return soup


def get_link_source(url):
    soup = get_source(url)
    url_link = soup.select('#content > div.list_area.daily_img > ul > li')#[0].find('a')['href']
    return list(map(lambda x: 'https://comic.naver.com'+x.find('a')['href'],url_link))


# 가장 최근 회차의 베스트댓글 크롤링
def best_comment_new(latest_url):
    base_url = 'https://comic.naver.com/webtoon/weekday.nhn'
    comments = []
    proceed = -1 #진행 상태 표시 위함, 처음에 0보다 작아야 0%가 표시 됨
    
    driver, _ = drive(base_url) #driver만 먼저 열어 놓음. for문 돌면서 url만 바꿔줄 것임
    time.sleep(1.5)
    driver.get(latest_url)
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
        
    comments += list(map(lambda x: x.text.replace('\n',' '), soup.select('.u_cbox_contents')))
        
    driver.quit()
    return comments

def find_weekday(t_names):
    soup = get_source(base_url)
    return [ele['href'].split('&')[1].split('=')[1] for ele in soup.find_all(True, text=t_names) if 'weekday' in ele['href']]

def get_comic_info(url):
    soup = get_source(url)
    
    ids = url.split('/')[-1].split('?')[1].split('&')[0].split('=')[1] # id
    names = soup.select('#content > div.comicinfo > div.detail > h2 > span.title')[0].text # title
    
    auth = soup.select('#content > div.comicinfo > div.detail > h2 > span.wrt_nm')[0].text.strip() # author
    genre = soup.select('#content > div.comicinfo > div.detail > p.detail_info > span')[0].text # genre
    
    try:
        age = soup.select('#content > div.comicinfo > div.detail > p.detail_info > span.age')[0].text # age
    except:
        age = '연령가 없음'
    
    summary = soup.select('#content > div.comicinfo > div.detail > p:nth-child(2)')[0].text # summary
    thumbs = soup.select('#content > div.comicinfo > div.thumb > a')[0].find('img')['src'] # thumbnail
    
    cnt = soup.select('td.title')[0].select('a')[0].get('href').split('no=')[1].split('&')[0] # 가장 최근 회차
    latest = 'https://comic.naver.com/comment/comment.nhn?titleId={0}&no={1}#'.format(ids,cnt) #각 웹툰 회차 url 생성
    
    comment = best_comment_new(latest)
    
    return ids, names, auth, genre, age, summary, thumbs, url, comment


def crawling(weekday):
    comic_info_json = []
    home_url = 'https://comic.naver.com/webtoon/weekdayList?week={}'.format(weekday)
    weeknow_list = get_link_source(home_url)
    
    for now in weeknow_list:
        temp_dict = dict()
        t_IDs, t_names, t_auth, t_genre, t_age, t_sum, t_thumbs, t_urls, t_comments = get_comic_info(now)
        t_weekdays = find_weekday(t_names)
        temp_dict['t_IDs']=t_IDs
        temp_dict['t_weekdays']=t_weekdays
        temp_dict['t_names']=t_names
        temp_dict['t_auth']=t_auth
        temp_dict['t_genre']=t_genre
        temp_dict['t_age']=t_age
        temp_dict['t_sum']=t_sum
        temp_dict['t_thumbs']=t_thumbs
        temp_dict['t_urls']=t_urls
        temp_dict['t_comments']=t_comments
        comic_info_json.append(temp_dict)
    return comic_info_json


def write_to_s3(date_str, news_type, data):
    file_name = f'{date_str}.json'
    s3_path = f'{news_type}/' + file_name
    
    s3 = boto3.resource(
        's3',
        aws_access_key_id = ACCESS_KEY_ID,
        aws_secret_access_key = ACCESS_SECRET_KEY,
        )
    
    # if you save csv : download s3 csv file to lambda tmp folder
    # local_file_name = '/tmp/'+file_name
    
    # with open(local_file_name,"w", encoding='utf-8', newline='') as f:
    #     wr = csv.writer(f)
    #     wr.writerow(columns)
    #     for d in data:
    #         wr.writerow(d)
    # f.close()
    # s3.Bucket(BUCKET_NAME).upload_file(local_file_name, s3_path,ExtraArgs={'ACL' : 'public-read'})
    
    body = json.dumps(data).encode('UTF-8')
    
    s3.Bucket(BUCKET_NAME).put_object(Key=s3_path, Body=body, ACL='public-read')
    
    
def lambda_handler(event, context):
    tz = timezone(timedelta(hours=9))
    kst_dt = datetime.now().astimezone(tz)
    date_str = kst_dt.strftime('%Y%m%d')
    weekday_value = kst_dt.strftime('%a').lower()
    
    contents = crawling(weekday_value)
    write_to_s3(date_str, 'best_comment', contents)