# -*- coding: utf-8 -*-
import scrapy
import logging
import pandas as pd
import os



def html_table(response, selector):
    table_el = response.xpath(selector)[0]
    return pd.read_html(table_el.extract())[0]

def html_div_table(response, selector):
    table_el = response.xpath(selector)[0]
    def extract(table_el, xpath):
        return table_el.xpath(xpath).xpath('string(.)').extract()

    xpaths = [
        './/div[@class="wsod_fLeft"]',
        './/div[@class="wsod_fRight wsod_bold"]',]
    return pd.DataFrame({i: extract(table_el, xp) for i, xp in enumerate(xpaths)})


class Table:
    def __init__(self, name, xpath, parse):
        self.name = name
        self.xpath = xpath
        self._parse = parse
        self.table = None

    def parse(self, response):
        return self._parse(response, self.xpath)


table_selectors = [
        Table(name='todays_trading', 
              xpath='//*[@id="wsod_snapshotView"]/div[3]/div[1]/div[1]/table',
              parse=html_table),

        Table(name='growth_and_valuation', 
              xpath='//*[@id="wsod_snapshotView"]/div[3]/div[2]/div/table',
              parse=html_table),

        Table(name='financials', 
              xpath='//*[@id="wsod_snapshotView"]/div[4]/div[2]/div/table',
              parse=html_table),

        Table(name='profile', 
              xpath='//*[@id="wsod_snapshotView"]/div[5]/div[1]/div/table',
              parse=html_div_table),

    ]


class DataSelector:

    # table selectors is a data category and an xpath
    table_selectors = {t.name: t for t in table_selectors}


    def __init__(self, symbol, response, filepath='', filepattern='{}_{}.csv'):
        self.symbol = symbol
        self.tables = {}
        self.response = response
        self._filepath = filepath
        self.filepattern = filepattern
        self.fetch_all_tables()

    @property
    def filepath(self):
        return os.path.join(self._filepath, self.filepattern)
    
    def fetch_all_tables(self):
        for table_name in self.table_selectors:
            self.fetch_table(table_name)

    def fetch_table(self, table_name):
        logging.debug('Fetching table: ' + table_name)
        selector = self.table_selectors[table_name]
        table = selector.parse(self.response)
        self.tables[selector.name] = selector.parse(self.response)

    def save_table(self, table_name, filename=None):
        if filename is None:
            filename = self.filepath.format(self.symbol, table_name)
        logging.debug("Saving: " + filename)

        # Rotate the columns / index to get it into the right format

        table = self.tables[table_name]
        table.set_index(0, inplace=True)
        table = table.transpose()
        table['Symbol'] = self.symbol
        table['Category'] = table_name
        table.to_csv(filename)

    def save_all_tables(self):
        for table_name in self.tables:
            self.save_table(table_name)






class FundamentalSpider(scrapy.Spider):
    name = 'fundamental'
    allowed_domains = ['money.cnn.com']
    start_urls = ['http://money.cnn.com/']
    symbols_file = 'sp500.csv'

    def start_data(self):
        return pd.read_csv(self.symbols_file).itertuples()

    def start_requests(self):
        for row in self.start_data():
            yield scrapy.Request(row.url, callback=self.parse, meta={'Symbol': row.Symbol})

    def parse(self, response):
        symbol = response.meta['Symbol']        
        base_path = self.settings.get('DOWNLOAD_PATH', '')
        filepath = os.path.join(base_path, 'fundamental')

        data = DataSelector(symbol, response, 
                    filepath=filepath)
        data.save_all_tables()
