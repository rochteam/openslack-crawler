# -*- coding: utf-8 -*-
from main import app
from celery import group
from crawler import run
from celery.utils.log import get_task_logger
from storage import STORAGE_MAPS
import storage
from crawler.utils.bloomfilter import BloomFilter

logger = get_task_logger(__name__)

bf = BloomFilter(host='localhost', port=6379, db=0)  # url 去重装置


@app.task
def crawler(url):
    """
    即是消费者也是产出者
    :param url:
    """
    wanted_urls = []
    need_store = []
    image_urls = []
    # print('crawling: {0}'.format(url))

    urls, need_store, images = run_spider(url)

    # filter not repeated url
    for _im in images:
        if not bf.isContains(_im):
            bf.insert(_im)
            image_urls.append(_im)

    # filter not repeated url
    for _url in urls:
        if not bf.isContains(_url):
            bf.insert(_url)
            wanted_urls.append(_url)
        wanted_urls.append(_url)

    # 数据库存储
    if need_store:
        storage_pipeline.delay(need_store)

    # 图片下载
    image_tasks = group(image_pipeline.s(image) for image in image_urls)
    image_tasks()

    sub_tasks = group(crawler.s(url) for url in wanted_urls).skew(start=1)
    sub_tasks()


def run_spider(spider, *args):
    run.run_spider(spider, *args)


def run_spider2(spider, *args):
    run.run_spider2(spider, *args)


@app.task
def storage_pipeline(name, data):
    if name in STORAGE_MAPS:
        class_name = STORAGE_MAPS[name]
        obj = getattr(storage, class_name)
        if isinstance([], list):
            obj().save_many("a", "b", "c")
        else:
            obj().save_many("a", "b", "c")
        return ""
    else:
        logger.warn("not find storage")


@app.task
def image_pipeline(url):
    pass
    # _spider.download(url)

    # @app.task(bind=True, default_retry_delay=10, max_reties=3, base=DebugTask)
    # def add(self, x, y):
    #     logger.info("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
    #     logger.info(self.request)
    #     try:
    #         return x + y
    #     except Exception as e:
    #         return self.retry(exc=e)


    # @periodic_task(run_every=timedelta(seconds=10), exchange="default", routing_key="default")
    # def every_monday_morning():
    #     print("This is run every Monday morning at 7:30")
    #     return 1

    # class Lmy(PeriodicTask):
    #     run_every = timedelta(seconds=60)
    #     options = {"exchange": "default", "routing_key": "default"}
    #     name = "xxxxx"
    #
    #     def run(self):
    #         pass


if __name__ == '__main__':
    pass
    # from crawler.spiders.cnblogs import CnblogsSpider
    #
    # crawl(CnblogsSpider)
