# coding=utf-8
from scrapy.spiders import Spider
from scrapy.selector import Selector
from pymongo import MongoClient
from scrapy import Request
import datetime,time

mongodb = MongoClient()
dockonedb = mongodb.dockone


class DockeOneSpider(Spider):
    name = "dockone"
    allowed_domains = ["dockone.io"]

    def start_requests(self):
        for i in xrange(1, 48):
            yield Request("http://dockone.io/day-0__is_recommend-1__page-%s" % i, self.parse_list)

    def parse_list(self, response):
        """
        The lines below is a spider contract. For more info see:
        http://doc.scrapy.org/en/latest/topics/contracts.html
        @url http://dockone.io/day-0__is_recommend-1
        @scrapes name
        """
        sel = Selector(response)
        sites = sel.xpath('//div[@class="aw-item article"]')
        print len(sites),response.url
        for site in sites:
            item = {}
            item['name'] = site.xpath('.//h4/a/text()').extract()[0]
            item['url'] = site.xpath('.//h4/a/@href').extract()[0]
            item['user_name'] = site.xpath('.//p/a/text()').extract()[0]
            item['user_id'] = site.xpath('.//a[@class="aw-user-name hidden-xs"]/@data-id').extract()[0]
            item['user_avatar'] = site.xpath('.//a[@class="aw-user-name hidden-xs"]/img/@src').extract()[0]
            desc = site.xpath('.//p/span/text()').extract()[0]
            desc_list = desc.split(" • ")
            item["comment_count"] = int(desc_list[1].replace("个评论", "").strip())
            item["view_count"] = int(desc_list[2].replace("次浏览", "").strip())
            item["updated"] = desc_list[3].strip()
            yield Request(item['url'], meta={"base_item": item}, callback=self.parse_detail)

    def parse_detail(self, response):
        sel = Selector(response)
        item = response.meta["base_item"]
        item["content"] = "".join(sel.xpath('//div[@class="content markitup-box"]/child::*').extract())
        item["tags"]=[]
        tags = sel.xpath('//span[@class="topic-tag"]')
        for t in tags:
                # print t.xpath('@data-id').extract()
            item["tags"].append(t.xpath('./a/text()').extract()[0])
        item["id"]=item["url"].replace("http://dockone.io/article/","")
        # print base_item
        item["spider"]=self.name
        item["db"]=self.name
        item["collection"]="article"
        item["category"] = "doc"
        item["created"] = int(time.time())
        item["image_urls"]=sel.xpath('//div[@class="content markitup-box"]//img/@src').extract()
        item["file_urls"]=item["image_urls"]
        # print item["file_urls"]
        # dockonedb.article.update({"id": item["id"]}, {"$set": item}, upsert=True)
        return item
