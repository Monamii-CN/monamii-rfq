from hashlib import md5
from bs4 import BeautifulSoup
import sys
import scrapy
from scrapy import signals
from scrapy.xlib.pydispatch import dispatcher
from scrapy.exceptions import CloseSpider

import rfq_detail
import db
import webhook


class MainSpider(scrapy.Spider, db.MydbOperator):
    keyword = sys.argv[0]
    if keyword is None:
        sys.exit("Keyword is empty")
    table_name = sys.argv[1]
    if table_name is None:
        sys.exit("Table name is empty")
    webhook = sys.argv[2]
    if webhook is None:
        sys.exit("Webhook URL is empty")
    name = 'RFQ Spider'
    
    start_urls = ['https://sourcing.alibaba.com/rfq_search_list.htm?spm=a2700.8073608.1998677541.1' + '&searchText=' + keyword +'&recently=Y&tracelog=newest']
    mydb = db.MydbOperator(table_name)
    webhook_service = webhook.Webhook()
    
    isInitialize = False

    page_limit = 30

    def __init__(self):
        dispatcher.connect(self.spider_closed, signals.spider_closed)
        self.isInitialize = self.mydb.isEmptyTable()

    def parse(self,response):
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
            rfq_buyer_tag = " ".join(rfq_buyer_tag)
            #RFQ Quote Mapping
            rfq_quote = rfq_quote_info.xpath("//div[@class='quote-left']/span").get()
            #RFQ Desc Mapping
            rfq_desc = rfq_main_info.xpath("//div[@class='brh-rfq-item__detail']").get()
            

            rfq_in_db = self.mydb.getByTitleAndBuyer(rfq_title, rfq_buyer)
            if rfq_in_db is None:
                #Save to DB
                rfqObj = rfq_detail.RfqDetail(rfq_title, rfq_quantity, rfq_unit, rfq_stars, rfq_open_time, rfq_origin, rfq_buyer, rfq_buyer_tag, rfq_quote, rfq_desc, rfq_link)
                self.mydb.saveRFQ(rfqObj)
                #Send RFQ Webhook message
                if not self.isInitialize:
                    self.webhook_service.sendMessage()
            else:
                #Quit as reaching existing data records
                raise CloseSpider("There's no new record yet.")

        for next_page in response.selector.xpath("//div[@class='list-pagination']//a[@class='next']"):
            currentPage = response.selector.xpath("//div[@class='list-pagination']//span[@class='current']")
            if currentPage <= self.page_limit:
                yield response.follow(next_page, self.parse)
    
    def spider_closed(self, spider):
        self.mydb.close()
