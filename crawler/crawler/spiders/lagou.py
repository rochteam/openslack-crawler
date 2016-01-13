# -*- coding: utf-8 -*-
import time
import sys
import re
from scrapy.http import Request
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider, Rule
import logging as log
from redis import StrictRedis

redis = StrictRedis()
reload(sys)
sys.setdefaultencoding('utf8')
NUM_RE = re.compile(r"(\d+)")


class CsdnSpider(CrawlSpider):
    name = "csdn"
    allowed_domains = ["csdn.net"]
    # start_urls = (
    #     'http://blog.csdn.net/ranking.html',
    #     'http://blog.csdn.net/ranking.html',
    # )
    def start_requests(self):
        yield Request("http://www.lagou.com/gongsi/0-0-0?sortField=3#filterBox", self.parse_list)
        # yield Request("http://blog.csdn.net/bole.html?&page=1", self.parse_blog_list)
        # yield Request("http://blog.csdn.net/experts.html?&page=1", self.parse_blog_list)
        # yield Request("http://blog.csdn.net/column.html?&page=1", self.parse_blog_list)
        # yield Request("http://blog.csdn.net/hot.html?&page=1", self.parse_blog_list)
        # yield Request("http://blog.csdn.net/honour/experts.html", self.parse_blog_user)
        # yield Request("http://blog.csdn.net/pan_tian/article/details/50359440", callback=self.parse_blog_detail)
        for url in redis.smembers(self.name + ":blog_user"):
            if url.startswith("http"):
                yield Request(url, self.parse_blog_user)
            else:
                redis.srem(self.name + ":blog_user", url)

                # for url in redis.smembers(self.name + ":blog_list"):
                #     if url.startswith("http"):
                #         yield Request(url, self.parse_blog_list)
                #     else:
                #         redis.srem(self.name + ":blog_list",url)

    download_delay = 2
    rules = (

    )

    def parse_list(self, response):
        items = response.xpath("//a[@class='item_title']/@href").extract()
        for link in items:
            yield Request(link, meta={}, callback=self.parse_detail)

    def parse_detail(self, response):
        sel = Selector(response)
        item = {}
        celebrity = {}
        project = {}
        site = {}
        social = {}
        url = response.url
        user_item = {}
        item["id"] = url.split("/")[-1].replace(".html", "")
        item["name"] = sel.xpath("//div[@class='company_main']/h1/a/text()").extract()[0].strip()
        item["full_name"] = sel.xpath("//div[@class='company_main']/h1/a/@title").extract()[0].strip()
        item["content"] = "".join(sel.xpath("//span[@class='content']/child::*").extract())
        item["logo"] = sel.xpath("//div[@class='top_info_wrap']/img/@src").extract()[0].strip()
        item['url'] = url
        item['tags'] = sel.xpath('//div[@class="item_content"]//li[1]/text()').extract()[0].strip()
        item["address"] = sel.xpath('//div[@class="item_content"]//li[4]/text()').extract()[0].strip()
        item["collection"] = "company"

        celebrity["photo"] = sel.xpath("//img[@class='item_manger_photo_show']/@src").extract()[0].strip()
        celebrity["name"] = sel.xpath('//p[@class="item_manager_name"]/span/text()').extract()
        celebrity["position"] = sel.xpath('//p[@class="item_manager_title"]/text()').extract()
        celebrity["collection"] = "celebrity"

        social["name"] = sel.xpath('//p[@class="item_manager_name"]/span/@title').extract()
        social["url"] = sel.xpath('//p[@class="item_manager_name"]/span/@href').extract()
        social["collection"] = "celebrity"

        for p in sel.xpath("//div[@class='product_content product_item clearfix']").extract():
            project["img"]=sel.xpath('./img/@src').extract()[0]
            user_item["fullname"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/text()").extract()[0].strip()
        user_item["url"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/@href").extract()[0].strip()
        user_item["collection"] = "user"
        user_item["image_urls"] = [user_item["avatar"]]
        user_item["file_urls"] = user_item["image_urls"]
        return [item, user_item]
