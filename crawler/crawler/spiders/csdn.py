# -*- coding: utf-8 -*-
import time
import sys
import scrapy
from scrapy.http import Request
from scrapy.contrib.linkextractors import LinkExtractor
from scrapy.selector import Selector
from scrapy.spiders import CrawlSpider, Rule
import logging as log
from redis import StrictRedis
redis=StrictRedis()
reload(sys)
sys.setdefaultencoding('utf8')


class CsdnSpider(CrawlSpider):
    name = "csdn"
    allowed_domains = ["csdn.net"]
    # start_urls = (
    #     'http://blog.csdn.net/hot.html',
    #     'http://blog.csdn.net/',
    #     'http://blog.csdn.net/column.html',
    #     'http://blog.csdn.net/bole.html',
    #     'http://blog.csdn.net/experts.html',
    #     'http://blog.csdn.net/ranking.html',
    #     'http://blog.csdn.net/ranking.html',
    # )
    def start_requests(self):
        yield Request("http://blog.csdn.net/?&page=1", self.parse_blog_list)

    download_delay = 2
    rules = (
        Rule(LinkExtractor(allow = ('article/details/[0-9]+'),allow_domains = ('blog.csdn.net')),callback = 'parse_item'),
        Rule(LinkExtractor(allow = ('index.html'),allow_domains = ('blog.csdn.net'))),
        Rule(LinkExtractor(allow = ('details.html'),allow_domains = ('blog.csdn.net'))),
        Rule(LinkExtractor(allow = ('experts.html'),allow_domains = ('blog.csdn.net'))),
        Rule(LinkExtractor(allow = ('article/list'),allow_domains = ('blog.csdn.net'))),
        Rule(LinkExtractor(allow = ('/[a-z_0-9]+$'),allow_domains = ('blog.csdn.net'))),
        Rule(LinkExtractor(allow=r'article/list/[0-9]{1,20}'), callback='parse_item', follow=True),
        Rule(LinkExtractor(allow=r'article/details/[0-9]{1,20}'), callback='parse_article', follow=True),
        # Rule(LinkExtractor(allow=r"^http://(news|cloud|mobile|sd|programmer)\.csdn.net($|(/[a-zA-Z]+/\d+$))"),callback="parse_next_page",follow=True),
        )

    def parse_blog_list(self, response):
        for sel in response.xpath("//div[@class='blog_list']"):
            item={}
            pagelink = sel.xpath('./h1/a[2]/@href').extract()[0].strip()
            item["id"] = sel.xpath('./h1/a[2]/@name').extract()[0].strip()
            item["category"] = sel.xpath('./h1/a[1]/text').extract()[0].strip().replace("[","").replace("]","")
            item["user_name"] = sel.xpath('.//dt/a/img/@alt').extract()[0].strip()
            item["user_avatar"] = sel.xpath('.//dt/a/img/@src').extract()[0].strip()
            link = "http://blog.csdn.net" + pagelink
            item["digg"] = sel.xpath('.//span[@class="fr digg"]/@digg').extract()[0].strip()
            item["bury"] = sel.xpath('.//span[@class="fr digg"]/@bury').extract()[0].strip()
            item["user_fullname"] = sel.xpath('.//a[@class="user_name"]/a/text()').extract()[0].strip()
            yield Request(link, meta={"base_item": item},callback=self.parse_blog_detail)
        page_nav=response.xpath("//div[@class='page_nav']/a/[@src]")
        for l in page_nav:
            redis.sadd(self.name+":blog_list","http://blog.csdn.net" + page_nav)

    def parse_blog_detail(self, response):
        sel = Selector(response)
        item = response.meta["base_item"]
        url = response.url
        item["title"] = sel.xpath("//span[@class='link_title']//text()").extract().strip()
        item["content"] = "".join(sel.xpath("//div[@id='article_content']/child::*").extract())
        item['url'] = url
        item['tags'] = sel.xpath('//span[@class="link_categories"]//text()').extract()
        item["postdate"] = sel.xpath("//span[@class='link_postdate']/text()").extract()[0].strip()
        item["view_count"] = sel.xpath("//span[@class='link_view']/text()").extract()[0].strip().replace(u"阅读","")
        item["comments_count"] = sel.xpath("//span[@class='link_comments']/text()").extract()[0].strip().replace("(","").replace(")","")
        blog_rank = sel.xpath('//url[@class="blog_rank"]/li/span/text()').extract()
        item["user_views"]=blog_rank[0].replace(u"次", "")
        item["user_credits"]=blog_rank[1]
        item["user_rank"]=blog_rank[2].replace(u"第", "").replace(u"名", "")
        item["image_urls"]=sel.xpath('//div[@id="article_content"]//img/@src').extract()
        item["file_urls"]=item["image_urls"]
        log.info("url is "+url)
        next_article=sel.xpath("//li[@class='next_article']/a/@href").extract()[0].strip()
        redis.sadd(self.name+":blog_detail","http://blog.csdn.net" + next_article)
        prev_article=sel.xpath("//li[@class='prev_article']/a/@href").extract()[0].strip()
        redis.sadd(self.name+":blog_detail","http://blog.csdn.net" + prev_article)
        return item

