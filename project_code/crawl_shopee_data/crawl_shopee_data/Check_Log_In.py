import undetected_chromedriver as webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
import time
import logging
import sys
sys.path.append('/home/cuongton/airflow/')
from project_code.crawl_shopee_data.crawl_shopee_data import secret

def check_log_in_shopee_page(url, true_condition, true_condition_afer_log_in, profile_path, profile_name, google_password):
    
    # set up logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # set up chrome-driver run with custom profile
    chrome_options = webdriver.ChromeOptions()
    chrome_options.add_argument(f"--user-data-dir=/home/cuongton/airflow/project_code/crawl_shopee_data/crawl_shopee_data/ChromeProfile/{profile_path}") #test_First_Profile
    chrome_options.add_argument(f"--profile-directory={profile_name}") #shopee_profile
    driver = webdriver.Chrome(options=chrome_options, driver_executable_path='/home/cuongton/airflow/chromedriver-linux64/chromedriver-linux64/chromedriver')

    # access website
    driver.get(url)
    time.sleep(5)

    # check if last access time can be used this time, as the website may log out your account after a specific period.
    if driver.current_url == true_condition:
        driver.close()
        return logging.info(f"Your account in {profile_path} has been logged in")
    
    # in case your account has been logged out.
    logging.info(f"Your account in {profile_path} has been logged out")

    # choose to log in with a google account.
    google_account = driver.find_element(By.XPATH, ('(//button[@class="nGTAZw lyJbNT bQ2eCN"])[2]'))
    google_account.click()
    time.sleep(5)

    # there will be a pop up window to choose google accounts. Need to switch to the pop-up window.
    windows = driver.window_handles
    driver.switch_to.window(windows[-1])

    # sign in
    google_sign_in = driver.find_element(By.XPATH, '//div[@class="w1I7fb"]')
    google_sign_in.click()
    time.sleep(5)

    # if the website requires typing password
    try:
        password = driver.find_element(By.XPATH, '//input[@type="password"]')
        password.send_keys(google_password)
        password.send_keys(Keys.ENTER)
        time.sleep(5)
    except:
        pass

    # term of sharing account
    google_sharing_term = driver.find_element(By.XPATH, '(//span[@class="VfPpkd-vQzf8d"])[3]')
    if google_sharing_term:
        google_sharing_term.click()
        time.sleep(2)

    # switch back to the main window.
    driver.switch_to.window(windows[0])
    time.sleep(5)

    # check status
    if driver.current_url == true_condition_afer_log_in:
        driver.close()
        return logging.info(f"Your account in {profile_path} has been logged in")


if __name__ == "__main__":

    shopee_url = 'https://shopee.vn/buyer/login'
    log_in_page_success = 'https://shopee.vn/'
    profile_path = sys.argv[1]
    profile_name = 'shopee_profile'
    re_log_in = 'https://shopee.vn/?is_from_login=true'
    check_log_in_shopee_page(shopee_url, log_in_page_success, re_log_in, profile_path, profile_name, secret.google_password)
