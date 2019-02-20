import logging
import time
import urllib
from urllib.parse import urlencode

import pymongo
from fake_useragent import UserAgent
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.wait import WebDriverWait

DRIVER_PATH = '/usr/local/bin/chromedriver'
FILENAME = '/Users/andrewzhan/Projects/Gitlab/taobao_image_crawler/goods.txt'

MONGODB_HOST = '10.16.169.177'
MONGODB_PORT = 27017
MONGODB_DB = 'scrapy_images'
MONGODB_USER = 'qidun'
MONGODB_PASS = 'Z0tPAyMToLSH'
MONGODB_COLLECTION = 'taobao'

COOKIES = 'miid=9059030392035869736; cna=9GnBE24ycD4CAd3dHSVkcLka; hng=CN%7Czh-CN%7CCNY%7C156; thw=cn; tg=0; t=038c33ba8aab8ca768d40a6eab66d828; _uab_collina=154907493821998864468959; _cc_=V32FPkk%2Fhw%3D%3D; enc=RRC8ncbpfOZMLPZ4BSFYrLIXir1vtgtBd1%2BKcHsVHWDFvogyr8IpgJINMgLZUEEseQnXf4x6ARIfLYdrLweo4w%3D%3D; mt=ci=0_0; _m_h5_tk=30a92ebafc749c4311cba309a31700c6_1549219346290; _m_h5_tk_enc=028bcf7811aeadf87efd0b57859fc344; v=0; cookie2=391c6d6c34d539fc0ef7417f0c8a7e88; _tb_token_=3ee7e1775d3e1; alitrackid=www.taobao.com; lastalitrackid=www.taobao.com; JSESSIONID=2D0A78F41775DEB1F9B7CAB5EFAD026C; x5sec=7b227365617263686170703b32223a223336376534663935373732333834376537323964663030623935626166643935434f2b70352b4946454a433876746a6b2f6337476f514561444459334e7a59794f4451334d4473794d413d3d227d; isg=BDEx7a0KyZlrs2KLbEkfa750QLsLtrrKGOWX9xNGA_gXOlCMW2xoYAlbXIb58j3I; l=bBIoM2gmvs3LDcv1BOCiquI81xbOdIRfguPRwGyei_5IK6L1x27OlR1QJFp6Vf5PttTB4cyBlueTfUggJPvN.'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


class RandomUserAgent(object):
    def __init__(self):
        super(RandomUserAgent, self).__init__()
        self.ua = UserAgent(verify_ssl=False)
        self.per_proxy = False
        self.ua_type = 'random'
        self.proxy2ua = {}

    def randomly_select(self):
        return getattr(self.ua, self.ua_type)


class MongoHelper(object):
    BUFFER_SIZE = 50
    write_buffer = None
    total = 0

    def __init__(self):
        super(MongoHelper, self).__init__()
        username = urllib.parse.quote_plus(MONGODB_USER)
        password = urllib.parse.quote_plus(MONGODB_PASS)
        if username == '' and password == '':
            url = 'mongodb://%s:%s' % (MONGODB_HOST, MONGODB_PORT)
        else:
            url = 'mongodb://%s:%s@%s:%s' % (username, password, MONGODB_HOST, MONGODB_PORT)
        self.client = pymongo.MongoClient(url)
        self.db = self.client[MONGODB_DB]
        self.collection = self.db[MONGODB_COLLECTION]
        self.write_buffer = []

    def flush(self):
        self.collection.insert_many(self.write_buffer)
        self.write_buffer = []

    def save_info(self, item):
        if isinstance(item, dict):
            self.write_buffer.append(item)
            self.total += 1
        if len(self.write_buffer) >= self.BUFFER_SIZE:
            logging.info('MongoHelper buffer reached threshold, flushing data to MongoDB. '
                         '%d items have been saved in total' % self.total)
            self.flush()

    def close(self):
        logging.info('MongoHelper is closing client, flushing data to MongoDB.')
        self.flush()
        self.client.close()


def read_keywords_from_file(filename):
    """
    从文件中读取关键词，用于淘宝搜索
    :param filename: 文件路径
    :return: 关键词列表
    """
    with open(filename, 'r') as file:
        lines = file.readlines()
        keywords = [line.split(':')[0] for line in lines]
    return keywords


def set_driver():
    options = webdriver.ChromeOptions()
    user_agent = RandomUserAgent()
    options.add_argument('user-agent={user_agent}'.format(user_agent=user_agent.randomly_select()))

    options.add_argument('--headless')
    # 配置不加载图片
    # prefs = {"profile.managed_default_content_settings.images": 2}
    # options.add_experimental_option("prefs", prefs)
    return webdriver.Chrome('{path}'.format(path=DRIVER_PATH), chrome_options=options)


def set_cookies(driver):
    driver.delete_all_cookies()
    for cookie in COOKIES.split(';'):
        name, value = cookie.strip().split('=', 1)
        domain = '.taobao.com'
        cookie = {'name': name, 'value': value, 'domain': domain}
        driver.add_cookie(cookie)


def search_by_keyword(driver, word):
    def parse_detail_page(url, title):
        driver.get(url)
        wrapper_xpath = '//div[@id="description"]/div[contains(@class, "content")]'
        image_xpath = wrapper_xpath + '//img'

        # 滚动到class=content的div标签，触发异步加载
        try:
            wrapper = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, image_xpath)))
            ActionChains(driver).move_to_element(wrapper).perform()
        except TimeoutException as e:
            logging.error('Seems there is no image in the page (%s)' % url)
        else:
            # 获取所有图片，依次曝光
            img_list = driver.find_elements_by_xpath(image_xpath)
            index = 0
            retry_times = 0
            while True:
                # print(index)
                # print(len(img_list))
                if index >= len(img_list):
                    # 等待更多的图片加载，如果等待3次图片数量没有增加，退出循环
                    retry_times = retry_times + 1
                    if retry_times >= 3:
                        break
                    time.sleep(1)
                else:
                    retry_times = 0
                    # 曝光每张图片
                    ActionChains(driver).move_to_element(img_list[index]).perform()
                    img_list = driver.find_elements_by_xpath(image_xpath)
                    index = index + 1

            # 取出当前页面的所有图片
            images = driver.find_elements_by_xpath('//div[@id="description"]/div[contains(@class, "content")]//img')
            logging.info('Found %d images from page %s.' % (len(images), url))
            for index, image in enumerate(images):
                image_info = {
                    'url': image.get_attribute('src'),
                    'title': title,
                    'index': index + 1,
                    'width': image.get_attribute('width'),
                    'height': image.get_attribute('height'),
                }
                item = {
                    'image_information': image_info,
                    'search': word,
                    'page_number': page + 1,
                    'time': time.strftime('%Y-%m-%d', time.localtime(time.time())),
                    'source_url': url
                }
                mongo.save_info(item)

    for page in range(3):
        logging.info('Keyword: %s, page: %d, start crawling images.' % (word, page + 1))
        paras = {
            'q': word,
            's': 44 * page
        }
        base_url = 'https://s.taobao.com/search?'
        url = base_url + urlencode(paras)
        driver.get(url)

        goods = driver.find_elements_by_xpath(
            '//div[@id= "mainsrp-itemlist"]//div[@class="items"]//div[@class="pic"]/a')
        good_urls = []
        for good in goods:
            good_urls.append((good.get_attribute('href'), good.find_element_by_xpath('./img').get_attribute('alt')))

        for url, title in good_urls:
            parse_detail_page(url, title)


if __name__ == '__main__':
    mongo = MongoHelper()

    keywords = read_keywords_from_file(FILENAME)
    logging.info('Read %d keywords from %s.' % (len(keywords), FILENAME))

    driver = set_driver()
    driver.get('https://www.taobao.com')
    set_cookies(driver)
    time.sleep(2)

    try:
        for index, word in enumerate(keywords):
            logging.info('Keyword: %s, start searching images.' % word)
            search_by_keyword(driver, word)
            logging.info('Keyword: %s, images crawling finished.' % word)
    except Exception as error:
        logging.error(error)
    finally:
        driver.close()
        mongo.close()