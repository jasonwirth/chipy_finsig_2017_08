# -*- coding: utf-8 -*-
import scrapy


class Sp500Spider(scrapy.Spider):
    name = 'sp500'
    allowed_domains = ['money.cnn.com']
    start_urls = ['http://money.cnn.com/']

    def parse(self, response):
        pass
