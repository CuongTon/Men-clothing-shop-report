import scrapy
import undetected_chromedriver as webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import json

class KraftvnSpider(scrapy.Spider):
    
    #general information
    name = "4MEN"
    shop_id = 277366270
    google_search_shop_link_first_page = 'https://shopee.vn/4menstores#product_list'
    chrome_profile_path = 'crawl_shopee_data/ChromeProfile/default_First_Profile'

    # distract scrapy
    allowed_domains = ["github.com"]
    start_urls = ["https://github.com"]

    # S3 file configuration
    # custom_settings = {
    #     'FEEDS':{f's3://shopeeproject/Kraftvn/{name}.json': {
    #         'format': 'json',
    #         'encoding': 'utf8',
    #         'store_empty': False,
    #         'indent': 4}                
    #     }
    # }

    def __init__(self):

        # set up selenium
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument(f"--user-data-dir={self.chrome_profile_path}")
        chrome_options.add_argument("--profile-directory=shopee_profile")
        chrome_options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
        self.driver = webdriver.Chrome(options=chrome_options, driver_executable_path='/home/cuongton/airflow/chromedriver-linux64/chromedriver-linux64/chromedriver')
        # Enable network tracking
        self.driver.execute_cdp_cmd("Network.enable", {})

        # navigate to a destinated website
        self.driver.get('https://www.google.com')

        search_input = self.driver.find_element(By.XPATH, '//textarea[@type="search"]')
        search_input.send_keys(self.google_search_shop_link_first_page)
        search_input.send_keys(Keys.ENTER)

        first_page = self.driver.find_element(By.XPATH, '(//h3[@class="LC20lb MBeuO DKV0Md"])[1]')
        first_page.click()

        time.sleep(5)


    def _get_response_XHR(self, api_url):

        time.sleep(5)
        # get logs from chrome developer tools. And go to message part to find requestId
        logs_raw = self.driver.get_log("performance")
        logs = [json.loads(lr["message"])["message"] for lr in logs_raw]
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
        total_items = 1
        page_number = 1

        # get api response in multiple pages
        while items_crawled < total_items:
            # crawl api response
            print(f'{"-"*10} Page: {page_number} {"-"*10}')
            url = f'https://shopee.vn/api/v4/shop/rcmd_items?bundle=shop_page_category_tab_main&limit=30&offset={off_set}&shop_id={self.shop_id}&sort_type=1&upstream='
            # handle repsonse
            xhr_body = self._get_response_XHR(url)
            total_items = xhr_body['data']['total']
            items_list = xhr_body['data']['items']

            # move to next page, why 30, each page have maximum 30 items
            off_set += 30
            items_crawled += len(items_list)
            page_number += 1

            # save output
            for item in items_list:
                item['shop_name'] = self.name
                yield item

            # move to the next page
            next_page = self.driver.find_element(By.XPATH, '//button[@class="shopee-icon-button shopee-icon-button--right "]')
            next_page.click()

        # close driver
        time.sleep(2)
        self.driver.close()