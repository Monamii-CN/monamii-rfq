import scrapy
from bs4 import BeautifulSoup
from scrapy import signals
from scrapy.exceptions import CloseSpider
from scrapy.xlib.pydispatch import dispatcher
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os

import db
import rfq_detail
import webhook


class MainSpider(scrapy.Spider, db.MydbOperator):
    # instantiate a chrome options object so you can set the size and headless preference
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920x1080")

    # download the chrome driver from https://sites.google.com/a/chromium.org/chromedriver/downloads and put it in the
    # current directory
    chrome_driver = os.getcwd() + "\\chromedriver.exe"

    name = 'RFQ Spider'

    def __init__(self, keyword='', table_name='', webhook_url='', **kwargs):
        dispatcher.connect(self.spider_closed, signals.spider_closed)

        self.start_urls = [f'https://sourcing.alibaba.com/rfq_search_list.htm?spm=a2700.8073608.1998677541.1&searchText={keyword}&recently=Y&tracelog=newest']
        self.mydb = db.MydbOperator(table_name)
        self.webhook_service = webhook.WebHook(webhook_url)

        self.mydb.create_table()

        self.isInitialize = self.mydb.is_empty_table()
        self.page_limit = 5

        super().__init__(**kwargs)
        print(self.start_urls)

    def parse(self, response):
        print(self.chrome_driver)
        print(self.chrome_options)
        driver = webdriver.Chrome(chrome_options=self.chrome_options, executable_path=self.chrome_driver)
        driver.get("https://sourcing.alibaba.com/rfq_search_list.htm?spm=a2700.8073608.1998677541.1&searchText=led&recently=Y&tracelog=newest")

    def spider_closed(self, spider):
        self.mydb.close()
