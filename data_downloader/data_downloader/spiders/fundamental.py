# -*- coding: utf-8 -*-
import scrapy


class FundamentalSpider(scrapy.Spider):
    name = 'fundamental'
    allowed_domains = ['money.cnn.com']
    start_urls = ['http://money.cnn.com/']

    def parse(self, response):
        pass
