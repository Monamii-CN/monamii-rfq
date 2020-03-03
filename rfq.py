import os

import scrapy
from bs4 import BeautifulSoup
from scrapy import signals
from scrapy.exceptions import CloseSpider
from scrapy.xlib.pydispatch import dispatcher
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

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

        self.start_urls = [
            f'https://sourcing.alibaba.com/rfq_search_list.htm?spm=a2700.8073608.1998677541.1&searchText={keyword}&recently=Y&tracelog=newest']
        self.mydb = db.MydbOperator(table_name)
        self.webhook_service = webhook.WebHook(webhook_url)

        self.mydb.create_table()

        self.isInitialize = self.mydb.is_empty_table()
        self.page_limit = 5

        super().__init__(**kwargs)

    def parse(self, response):
        driver = webdriver.Chrome(chrome_options=self.chrome_options, executable_path=self.chrome_driver)
        driver.get(self.start_urls[0])
        container_elements = driver.find_elements_by_xpath(
            "//div[@class='rfqSearchList']//div[contains(@class, 'alife-bc-brh-rfq-list__row')]")
        for container_element in container_elements:
            # RFQ Areas Mapping
            rfq_main_info = container_element.find_element_by_class_name("brh-rfq-item__main-info")
            rfq_other_info = container_element.find_element_by_class_name("brh-rfq-item__other-info")
            rfq_quote_info = container_element.find_element_by_class_name("brh-rfq-quote-now")
            # RFQ Title And URL Mapping
            rfq_title_raw = rfq_main_info.find_element_by_css_selector("a.brh-rfq-item__subject-link")
            soup = BeautifulSoup(rfq_title_raw.get_attribute("title"))
            rfq_title = soup.get_text()
            rfq_link = rfq_title_raw.get_attribute("href")
            # RFQ Quantity Mapping
            rfq_quantity = rfq_main_info.find_element_by_xpath(
                "//div[@class='brh-rfq-item__quantity']/span[@class='brh-rfq-item__quantity-num']").text
            # RFQ Unit Mapping
            rfq_unit = rfq_main_info.find_element_by_xpath(
                "//div[@class='brh-rfq-item__quantity']/span[position() = 3]").text
            # RFQ Star Mapping
            rfq_stars = len(rfq_main_info.find_elements_by_xpath("//div[@class='next-rating-overlay']//i"))
            # RFQ Open Time Mapping
            rfq_open_time = rfq_main_info.find_element_by_class_name("brh-rfq-item__open-time").text
            # RFQ Origin Mapping
            rfq_origin = rfq_main_info.find_element_by_class_name("brh-rfq-item__country").text
            # RFQ Buyer Mapping
            rfq_buyer = rfq_other_info.find_element_by_xpath("//div[@class='text']").text
            # RFQ Buyer Tag
            rfq_buyer_tag_str = ""
            rfq_buyer_tags = rfq_other_info.find_elements_by_css_selector(
                "div.bc-brh-rfq-flag--buyer div.next-tag-body")
            for rfq_buyer_tag in rfq_buyer_tags:
                rfq_buyer_tag_str = rfq_buyer_tag.text + "|"
            rfq_buyer_tag_str = rfq_buyer_tag_str[0:len(rfq_buyer_tag_str) - 1]
            # RFQ Quote Mapping
            rfq_quote = rfq_quote_info.find_element_by_css_selector("div.quote-left span").text
            # RFQ Desc Mapping
            rfq_desc = ""#rfq_main_info.find_element_by_class_name("brh-rfq-item__detail").text

            rfq_in_db = self.mydb.get_by_title_and_buyer(rfq_title, rfq_buyer)

            if rfq_in_db is None:
                # Save to DB
                rfq_object = rfq_detail.RfqDetail(rfq_title, rfq_quantity, rfq_unit, rfq_stars, rfq_open_time,
                                                  rfq_origin, rfq_buyer, rfq_buyer_tag_str, rfq_quote, rfq_desc,
                                                  rfq_link)
                self.mydb.save_rfq(rfq_object)
                # Send RFQ Webhook message
                if not self.isInitialize:
                    self.webhook_service.sendMessage()
            else:
                # Quit as reaching existing data records
                raise CloseSpider("There's no new record yet.")

    def spider_closed(self, spider):
        self.mydb.close()
