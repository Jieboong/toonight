import selenium
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as ec
from selenium import webdriver
import time
import csv


result = csv.writer(open("kakao_webtoon.csv", "w"))

KAKAO_ID = ''
KAKAO_PW = ''

LOGIN_URL = 'https://webtoon.kakao.com/notification'
CONTENT_BASE = 'https://webtoon.kakao.com'

options = webdriver.ChromeOptions()

# options.add_argument("headless")

options.add_experimental_option('excludeSwitches', ['enable-logging'])


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

tabs = ['mon', 'complete']
URLs = ['https://webtoon.kakao.com/original-webtoon?tab=%s','https://webtoon.kakao.com/original-novel?tab=%s']


for tab in tabs :
    for URL in URLs : 
        driver.get(URL%tab)
        driver.implicitly_wait(time_to_wait=5)

        elements = driver.find_elements(By.CLASS_NAME, 'relative.responsive-cell')

        for elem in elements :
            try :
                pic = elem.find_element(By.CLASS_NAME, 'w-full.h-full.object-cover.object-top').get_attribute('src')
            except :
                pass
            # implicitly wait으로 동영상 사라질때까지 기다리기 -> 20초?
            elem.click()
            time.sleep(1)

            url = driver.current_url
            serial = int(url.split('/')[-1])
        
            try :
                infos = driver.find_element(By.XPATH,'//*[@id="root"]/main/div/div[2]/div/div[1]/div[2]/div[1]/div[1]/div[3]/div[2]/div/p[1]')
                
                genre = driver.find_element(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-3.opacity-85').text
                reactions = driver.find_elements(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-2.opacity-85')

                view = reactions[0].text
                like = reactions[1].text

                infos.click()

            except :
                time.sleep(20)
                infos = driver.find_element(By.CLASS_NAME, 'overflow-hidden.cursor-pointer')
                
                genre = driver.find_element(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-3.opacity-85').text
                reactions = driver.find_elements(By.CLASS_NAME, 'whitespace-pre-wrap.break-all.break-words.s12-regular-white.ml-2.opacity-85')

                view = reactions[0].text
                like = reactions[1].text

                infos.click()

            time.sleep(1)
                        
            # 연재중 완결
            on_serial = driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[2]/div/div[1]/div[3]/div/div/div[1]/div/div[2]/div/div/div[2]/div/div/div/div/p[1]' ).text
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
            
driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div[1]/div[1]/div[2]/div[2]/div/a[2]').click()
driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div/div[3]/div[2]/div[1]/a[1]').click()
driver.find_element(By.XPATH, '//*[@id="root"]/main/div/div/div[2]/div/a[2]/div').click()
driver.find_element(By.XPATH, '/html/body/div[5]/div/div/div/div[2]/button[2]').click()

driver.close()

 