# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from scrapy.crawler import CrawlerRunner
import sys
from scrapy.utils.log import configure_logging


def run_spider(spider, *args):
    print spider
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    process.crawl(spider, 0, 0, 0)
    process.start()


def run_spider2(spider, *args):
    configure_logging()
    runner = CrawlerRunner(get_project_settings())
    runner.crawl(spider, *args)
    runner.crawl(spider, *args)
    d = runner.join()
    # d = runner.crawl(spider, *args)
    d.addBoth(lambda _: reactor.stop())

    reactor.run()  # the script will block here until all crawling jobs are finished


if __name__ == '__main__':
    run_spider(sys.argv[1])
