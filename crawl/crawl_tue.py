from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
import time
import csv
import boto3 
import datetime

ACCESS_KEY_ID = ''
ACCESS_SECRET_KEY = ''
BUCKET_NAME = ''

KAKAO_ID = ''
KAKAO_PW = ''

LOGIN_URL = 'https://webtoon.kakao.com/notification'
CONTENT_BASE = 'https://webtoon.kakao.com/original-%s?tab=mon'

options = webdriver.ChromeOptions()

options.add_argument("headless")

options.add_experimental_option('excludeSwitches', ['enable-logging'])
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

s = Service('/Users/juhee/Desktop/2022/toonight/chromedriver')

driver = webdriver.Chrome(service = s, options=options)

driver.get('https://webtoon.kakao.com/notification')

# 성인 인증 위한 로그인

driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div/div[2]/div/div[1]/div/div/div/div/div/a').click()
time.sleep(0.5)

driver.find_element(By.XPATH, '/html/body/div[3]/div/div/div/div[1]/div[1]/div/div/button').click()

time.sleep(1)

driver.find_element(By.ID, "id_email_2").send_keys(KAKAO_ID)
driver.find_element(By.ID, "id_password_3").send_keys(KAKAO_PW)

driver.find_element(By.XPATH, '//*[@id="login-form"]/fieldset/div[8]/button[1]').click()

time.sleep(2)

driver.find_element(By.XPATH, '/html/body/div[5]/div/div/div/div[2]/button[2]').click()
time.sleep(1)

originals = ['webtoon', 'novel']

filename = "kakao_tue.csv"

result = csv.writer(open(filename, "w"))
result.writerow(("serial", "url", "pic", "genre", "view", "like", "on_serial", "day", "title", "story_writer", "illustrator", "publisher", "story_line", "key_words", "best_comments"))


for original in originals :

    driver.get(CONTENT_BASE%(original))
    driver.implicitly_wait(time_to_wait=5)

    
    section = driver.find_elements(By.CLASS_NAME,'relative.day-section')[1]
    elements = section.find_elements(By.CLASS_NAME, 'relative.responsive-cell')


    for elem in elements :
        try :
            pic = elem.find_element(By.CLASS_NAME, 'w-full.h-full.object-cover.object-top').get_attribute('src')
        except :
            pic = ''
        # implicitly wait으로 동영상 사라질때까지 기다리기 -> 20초?
        elem.click()
        time.sleep(2)
    
        try :
            infos = driver.find_element(By.CLASS_NAME,'overflow-hidden.cursor-pointer')
            url = driver.current_url
            serial = int(url.split('/')[-1])

            genre = driver.find_element(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-3.opacity-85').text
            reactions = driver.find_elements(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-2.opacity-85')

            view = reactions[0].text
            like = reactions[1].text

            infos.click()

        except :
            time.sleep(20)
            infos = driver.find_element(By.CLASS_NAME, 'overflow-hidden.cursor-pointer')
            url = driver.current_url
            serial = int(url.split('/')[-1])
            genre = driver.find_element(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-3.opacity-85').text
            reactions = driver.find_elements(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-2.opacity-85')

            view = reactions[0].text
            like = reactions[1].text

            infos.click()

        time.sleep(2)
                    
        # 연재중 완결
        on_serial = driver.find_element(By.XPATH,'//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/p[1]').text
        # 연재일 
        day_elems = driver.find_elements(By.CLASS_NAME,'whitespace-pre-wrap.break-all.break-words.inline-flex.font-badge.mr-4.s11-bold-black.bg-white.px-6')
        day = [ days.text for days in day_elems]
        # 웹툰 제목
        title = driver.find_element(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.mt-8.s22-semibold-white').text
        # 그림
        story_writer = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/dl/div[1]/dd').text
        #그림
        illustrator = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/dl/div[2]/dd').text
        # 발행처
        publisher = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/dl/div[3]/dd').text

        #키워드 저장
        key_word_elem = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/div[2]/div[2]')
        key_words = [key.text for key in key_word_elem.find_elements(By.TAG_NAME, 'p')]

        # 줄거리 더보기 클릭
        try : 
            driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/div[1]/div[3]/button').click()
        except : 
            pass

        story_line = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/div[1]/div[2]/p').text
        

        driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[1]/div[1]/div/div[2]/ul/li[3]/p').click()
        
        select_comments = driver.find_elements(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.leading-21.s14-regular-white')
        
        best_comments = [ comment.text for comment in select_comments]

        result.writerow((
            serial, url, pic, genre, view, like, on_serial, day, title, story_writer, illustrator, publisher, story_line, key_words, best_comments
        ))
        print(title)
        driver.back()


s3 = boto3.resource('s3',
    aws_access_key_id = ACCESS_KEY_ID,
    aws_secret_access_key = ACCESS_SECRET_KEY,
    )
try :
	s3.meta.client.upload_file(filename, BUCKET_NAME, 'kakao/fri/'+datetime.datetime.strftime(datetime.date.today(),'%Y-%m-%d' )+'.csv', ExtraArgs={
		'ACL' : 'public-read'
	})
	result = True
except : 
	result = False

driver.quit()
