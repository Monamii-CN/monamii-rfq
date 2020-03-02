import scrapy
from bs4 import BeautifulSoup
from scrapy import signals
from scrapy.exceptions import CloseSpider
from scrapy.xlib.pydispatch import dispatcher

import db
import rfq_detail
import webhook


class MainSpider(scrapy.Spider, db.MydbOperator):
    #    keyword = sys.argv[1]
    #   table_name = sys.argv[2]
    #  webhook_url = sys.argv[3]
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

    def parse(self, response):
        print("response: " + response.text)
        for rfq_item in response.selector.xpath("//div[@class='rfqSearchList']//div[contains(@class, 'alife-bc-brh-rfq-list__row')]"):
            #RFQ Areas Mapping
            rfq_main_info = rfq_item.xpath("//div[contains(@class, 'brh-rfq-item__main-info')]").get()
            rfq_other_info = rfq_item.xpath("//div[contains(@class, 'brh-rfq-item__other-info')]").get()
            rfq_quote_info = rfq_item.xpath("//div[@class='brh-rfq-quote-now')]").get()
            #RFQ Title And URL Mapping
            rfq_title_raw = rfq_main_info.xpath("//a[@class='brh-rfq-item__subject-link']")
            soup = BeautifulSoup(rfq_title_raw.xpath("@title")).get()
            rfq_title = soup.get_text()
            rfq_link = "https:" + rfq_title_raw.xpath("@href").get()
            #RFQ Quantity Mapping
            rfq_quantity = rfq_main_info.xpath("//div[@class='brh-rfq-item__quantity']/span[@class='brh-rfq-item__quantity-num']").get()
            #RFQ Unit Mapping
            rfq_unit = rfq_main_info.xpath("//div[@class='brh-rfq-item__quantity']/span[position() = 3]").get()
            #RFQ Star Mapping
            rfq_stars = len(rfq_main_info.xpath("//div[@class='next-rating-overlay']//i").getall())
            #RFQ Open Time Mapping
            rfq_open_time = rfq_main_info.xpath("//div[@class='brh-rfq-item__open-time']").get()
            #RFQ Origin Mapping
            rfq_origin = rfq_main_info.xpath("//div[@class='brh-rfq-item__country']").get()
            #RFQ Buyer Mapping
            rfq_buyer = rfq_other_info.xpath("//div[@class='text']").get()
            #RFQ Buyer Tag
            rfq_buyer_tag = rfq_other_info.xpath("//div[contains(@class, 'bc-brh-rfq-flag--buyer')]//div[contains(@class, 'next-tag-body')]']").getall()
            rfq_buyer_tag = "|".join(rfq_buyer_tag)
            #RFQ Quote Mapping
            rfq_quote = rfq_quote_info.xpath("//div[@class='quote-left']/span").get()
            #RFQ Desc Mapping
            rfq_desc = rfq_main_info.xpath("//div[@class='brh-rfq-item__detail']").get()

            rfq_in_db = self.mydb.get_by_title_and_buyer(rfq_title, rfq_buyer)
            if rfq_in_db is None:
                #Save to DB
                rfqObj = rfq_detail.RfqDetail(rfq_title, rfq_quantity, rfq_unit, rfq_stars, rfq_open_time, rfq_origin, rfq_buyer, rfq_buyer_tag, rfq_quote, rfq_desc, rfq_link)
                self.mydb.save_rfq(rfqObj)
                #Send RFQ Webhook message
                if not self.isInitialize:
                    self.webhook_service.sendMessage()
            else:
                #Quit as reaching existing data records
                raise CloseSpider("There's no new record yet.")

        for next_page in response.selector.xpath("//div[@class='list-pagination']//a[@class='next']"):
            current_page = response.selector.xpath("//div[@class='list-pagination']//span[@class='current']/text()").get()
            if int(current_page) <= self.page_limit:
                yield response.follow(next_page, self.parse)

    def spider_closed(self, spider):
        self.mydb.close()
