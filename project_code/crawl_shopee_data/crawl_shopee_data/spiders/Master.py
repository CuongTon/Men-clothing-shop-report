import scrapy
import undetected_chromedriver as webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import json
from scrapy import signals
from scrapy.signalmanager import dispatcher
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from scrapy.crawler import CrawlerProcess

class MasterSpider(scrapy.Spider):
    
    # distract scrapy
    name = "Master"
    allowed_domains = ["github.com"] # just fake
    start_urls = ["https://github.com"] # just fake

    # S3 file configuration
    # custom_settings = {
    #     'FEEDS':{f's3://shopeeproject/Kraftvn/{shop_name}.json': {
    #         'format': 'json',
    #         'encoding': 'utf8',
    #         'store_empty': False,
    #         'indent': 4}                
    #     }
    # }

    def __init__(self, *args, **kwargs):
        
        # send arguments into spider
        super().__init__(*args, **kwargs)

        # wait a signal of spider closing then do action
        dispatcher.connect(self.spider_closed, signal=signals.spider_closed)

        # -a para_1="Kraftvn" -a para_2=623651329 -a para_3='https://shopee.vn/kraftvn#product_list' -a para_4='crawl_shopee_data/ChromeProfile/default_Third_Profile'
        self.shop_name = kwargs['shop_name'] #"Kraftvn"
        self.shop_id = kwargs['shop_id'] #623651329
        self.google_search_shop_link_first_page = kwargs['shopee_url'] #'https://shopee.vn/kraftvn#product_list'
        self.chrome_profile_path = kwargs['profile']#'crawl_shopee_data/ChromeProfile/default_Third_Profile'
        
        # set up selenium
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(f"--user-data-dir={self.chrome_profile_path}")
        chrome_options.add_argument("--profile-directory=shopee_profile")
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})

        # use chrome driver path can help to run multiple chrome instance simultaneously
        self.driver = webdriver.Chrome(options=chrome_options, driver_executable_path='/home/cuongton/airflow/chromedriver-linux64/chromedriver-linux64/chromedriver')
        
        # Enable network tracking
        self.driver.execute_cdp_cmd("Network.enable", {})

        # navigate to a destinated website
        self.driver.get('https://www.google.com')

        WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
       
        search_input = WebDriverWait(self.driver, 20).until(
                    EC.presence_of_element_located((By.XPATH, '//textarea[@class="gLFyf"]'))                    )

        search_input.send_keys(self.google_search_shop_link_first_page)
        search_input.send_keys(Keys.ENTER)

        first_page = self.driver.find_element(By.XPATH, '(//h3[@class="LC20lb MBeuO DKV0Md"])[1]')
        first_page.click()

        time.sleep(5)

    def _get_response_XHR(self, api_url):

        # time.sleep(3) #TBU
        WebDriverWait(self.driver, 20).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(1)


        # get logs from chrome developer tools. And go to message part to find requestId
        logs_raw = self.driver.get_log("performance")
        logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
        body = []
        # get response body according to api_url
        for log in logs:
            try:
                request_url = log["params"]["response"]["url"]
                request_id = log["params"]["requestId"]
                if request_url == api_url:
                    xhr_response = self.driver.execute_cdp_cmd("Network.getResponseBody", {"requestId": request_id})
                    body = json.loads(xhr_response['body'])
            except:
                continue

        return body

    def parse(self, response):
        # set up variables, to move next page and control while loop
        off_set = 0
        items_crawled = 0
        self.total_items = 1
        page_number = 1
        first_page = True
        sub_url = ''

        # get api response in multiple pages
        while items_crawled < self.total_items:
            # crawl api response
            self.logger.info(f'{"-"*20} Page: {page_number} {"-"*20}')
            url = f'https://shopee.vn/api/v4/shop/rcmd_items?bundle=shop_page_category_tab_main&limit=30&offset={off_set}&shop_id={self.shop_id}&sort_type=1&upstream='
           
            # handle repsonse
            xhr_body = self._get_response_XHR(url)

            # in case there is no items, (shop is temporarily closed)
            try:
                items_list = xhr_body['data']['items']
            except:
                if self.total_items == 1:
                    self.total_items = -1
                    self.logger.info(f'{"-"*20} The shop is temporarily closed! {"-"*20}')
                    break

            if first_page:
                self.total_items = xhr_body['data']['total']
                first_page = False

            # move to next page, why 48, each page have maximum 48 items
            off_set += 30
            items_crawled += len(items_list)
            page_number += 1

            # save output
            for item in items_list:
                item['shop_name'] = self.shop_name
                yield item

            # move to the next page
            next_page = self.driver.find_element(By.XPATH, '//button[@class="shopee-icon-button shopee-icon-button--right "]')
            next_page.click()

            # log number of crawled items.
            self.logger.info(f"Page {page_number-1}: total number of items is {self.total_items}, total items have been crawled is {items_crawled}")

            # check the last page
            time.sleep(1) #TBU #??
            if sub_url != self.driver.current_url:
                sub_url = self.driver.current_url
            else:
                break

        self.logger.info(f'{"-"*20} Final Stats {"-"*20}')

        if self.total_items > 0:
            self.logger.info(f"Complete: total number of items is {self.total_items}, total items have been crawled is {items_crawled}")
        else:
            self.logger.info(f'{"-"*20} The shop is temporarily closed! {"-"*20}')

        self.logger.info(f'{"-"*20} Ending {"-"*20}')
       
        # close driver
        time.sleep(2)
        self.driver.close()

    def spider_closed(self, spider):
        # Need to print out total_items, it will automatically send to XCOM of Aiflow.
        # print(self.total_items)
        self.crawler.stats.set_value('total_items', self.total_items)

class ItemFilter(logging.Filter):
    def filter(self, record):
        # Suppress logs that contain item data
        return 'Scraped from <200 https://github.com>' not in record.getMessage() and "Enabled" not in record.getMessage()

# Apply the custom filter to the logger


#TBU
def main(**kwargs):

    settings = {
        'FEEDS':{kwargs['json_path']: {
            'format': 'json',
            'encoding': 'utf8',
            'overwrite': True
            }                
        },
        # 'LOG_LEVEL': 'CRITICAL'
    }
# 'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    loggers = [logging.getLogger(name) for name in logging.root.manager.loggerDict]
    for logger in loggers:
        logger.setLevel(logging.CRITICAL)

    logging.getLogger('scrapy.core.scraper').addFilter(ItemFilter())
    logging.getLogger('scrapy.middleware').addFilter(ItemFilter())

    # process = CrawlerProcess(settings=settings)
    # process.crawl(MasterSpider, **kwargs)
    # process.start()

    # Don't touch -------------------------------------------------------
    process = CrawlerProcess(settings=settings)
    crawler = process.create_crawler(MasterSpider)
    process.crawl(crawler, **kwargs)
    process.start()

    stats_dict = crawler.stats.get_stats()
    try:
        kwargs['ti'].xcom_push(key='total_items', value=stats_dict['total_items'])
    except:
        print(stats_dict['total_items'])

if __name__ == '__main__':

    op_kwargs = {
        'json_path': '/home/cuongton/airflow/project_code/crawl_shopee_data/4MEN.json',
        'shop_name': '4MEN',
        'shop_id': 277366270,
        'shopee_url': "https://shopee.vn/4menstores#product_list",
        'profile': '/home/cuongton/airflow/project_code/crawl_shopee_data/'+"crawl_shopee_data/ChromeProfile/default_Fifth_Profile"
    }

    main(**op_kwargs)