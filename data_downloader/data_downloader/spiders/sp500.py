# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
import os
import logging
from itertools import count
import pandas as pd

'''
http://money.cnn.com/data/markets/sandp/?page=200
'''

def link_to_dict(link):
    keys = ['text', 'url']
    return {k: getattr(link, k) for k in keys}


def links_to_table(links):
    data = [link_to_dict(l) for l in links]
    link_table = pd.DataFrame(data)
    return link_table


def constituents_table(response):
    table_selector = '#wsod_indexConstituents'

    el = response.css(table_selector)[0]
    tables = pd.read_html(el.extract())
    table = tables[0]

    # Company name includes the ticker symbol a link
    # Split the joined symbol/name string into two parts
    # eg.  'APD Air Products and Chemicals Inc'
    #      'APD',  'Air Products and Chemicals Inc'
    table['Symbol'] = table.Company.map(lambda s: s.split()[0])
    table['Company'] = table.Company.map(lambda s: ' '.join(s.split()[1:]))

    link_extractor = LinkExtractor(restrict_css=table_selector)
    links = link_extractor.extract_links(response)
    link_table = links_to_table(links)

    # Join the two tables on the symbol, drop the redundant `text` column
    table = table.merge(link_table, left_on='Symbol', right_on='text')
    del table['text']

    return table


def next_page(pageination, counter):
    '''
    :param pageination: a base url for pagination that can be incremented with the counter 
    :param counter: a generator that can produce the parameters for the pagination url
    '''
    for c in counter:
        yield pageination.format(c)


def default_save_path():
    return os.path.join(os.getcwd(), 'sp500.csv')


#TODO: This could be refactored into an "IndexSpider"
class Sp500Spider(scrapy.Spider):
    name = 'sp500'
    allowed_domains = ['money.cnn.com']
    start_urls = ['http://money.cnn.com/data/markets/sandp/']
    pageination = 'http://money.cnn.com/data/markets/sandp/?page={}'

    def __init__(self, filepath=None, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.filepath = filepath or default_save_path()
        self.constituents = {}
        self.next_page = next_page(self.pageination, counter=count(2))
        self.link_extractor = LinkExtractor(restrict_css='#wsod_indexConstituents')


    def parse(self, response):
        try:
            table = constituents_table(response)
        except Exception as e:
            table = None
        self.constituents[response.url] = table
        # Short circuit on None type, otherwise it throws error calling .
        if table is None:
            return
        if not table.empty:
            url = next(self.next_page)
            yield scrapy.Request(url, callback=self.parse)


    def closed(self, reason):
        logging.info('Downloaded pages: {}'.format(len(self.constituents)))
        for url in self.constituents:
            logging.info(url)

        # join all the pandas tables
        data = pd.concat(self.constituents, axis='index')
        data.to_csv(self.filepath, index=False)
        logging.info('Saving file: {}'.format(self.filepath))
