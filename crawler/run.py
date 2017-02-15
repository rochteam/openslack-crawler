# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from crawler.spiders.cnblogs import CnblogsSpider
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
from scrapy.utils.log import configure_logging


def run_spider(spider, *args):
    spider = CnblogsSpider(*args)
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(spider, *args)
    process.start()

def run_spider2(spider, *args):
    configure_logging()
    spider = CnblogsSpider(*args)
    runner = CrawlerRunner()
    runner.crawl(spider, *args)
    runner.crawl(spider, *args)
    d = runner.join()
    # d = runner.crawl(spider, *args)
    d.addBoth(lambda _: reactor.stop())

    reactor.run() # the script will block here until all crawling jobs are finished

