
## Getting started

```
    $ scrapy startproject data_downloader

    $ scrapy genspider fundamental money.cnn.com
```

Created spider 'fundamental' using template 'basic' in module:
  data_downloader.spiders.fundamental


## Creating spiders

a bunch of writing in sublime and using the Scrapy shell to test a response. Copy/pasting the code with IPython's `%paste` magic


## Benefits of using scrapy

- Cache -- You hit the page one time, this is great for debugging your spider
- Shell -- `scrapy shell <URL>`


## Running the spider

    `scrapy crawl sp500`
    `scrapy crawl fundamental`


