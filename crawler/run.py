# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from spiders.cnblogs import CnblogsSpider
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
import logging
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
    runner = CrawlerRunner(get_project_settings())
    runner.crawl(spider, *args)
    runner.crawl(spider, *args)
    d = runner.join()
    # d = runner.crawl(spider, *args)
    d.addBoth(lambda _: reactor.stop())

    reactor.run()  # the script will block here until all crawling jobs are finished

if __name__ == '__main__':
    run_spider("default")
