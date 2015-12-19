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
    #     'http://blog.csdn.net/hot.html',
    #     'http://blog.csdn.net/',
    #     'http://blog.csdn.net/column.html',
    #     'http://blog.csdn.net/bole.html',
    #     'http://blog.csdn.net/experts.html',
    #     'http://blog.csdn.net/ranking.html',
    #     'http://blog.csdn.net/ranking.html',
    # )
    def start_requests(self):
        yield Request("http://blog.csdn.net/index.html?&page=1", self.parse_blog_list)

    download_delay = 2
    rules = (
        Rule(LinkExtractor(allow=('article/details/[0-9]+'), allow_domains=('blog.csdn.net')), callback='parse_item'),
        Rule(LinkExtractor(allow=('index.html'), allow_domains=('blog.csdn.net'))),
        Rule(LinkExtractor(allow=('details.html'), allow_domains=('blog.csdn.net'))),
        Rule(LinkExtractor(allow=('experts.html'), allow_domains=('blog.csdn.net'))),
        Rule(LinkExtractor(allow=('article/list'), allow_domains=('blog.csdn.net'))),
        Rule(LinkExtractor(allow=('/[a-z_0-9]+$'), allow_domains=('blog.csdn.net'))),
        Rule(LinkExtractor(allow=r'article/list/[0-9]{1,20}'), callback='parse_item', follow=True),
        Rule(LinkExtractor(allow=r'article/details/[0-9]{1,20}'), callback='parse_article', follow=True),
        # Rule(LinkExtractor(allow=r"^http://(news|cloud|mobile|sd|programmer)\.csdn.net($|(/[a-zA-Z]+/\d+$))"),callback="parse_next_page",follow=True),
    )

    def parse_blog_list(self, response):
        for sel in response.xpath("//div[@class='blog_list']"):
            item = {}
            link = sel.xpath('./h1/a[last()]/@href').extract()[0].strip()
            item["id"] = sel.xpath('./h1/a[last()]/@name').extract()[0].strip()
            item["categorys"] = sel.xpath('./h1/a[@class="category"]/text()').extract()[0].strip().replace("[",
                                                                                                           "").replace(
                "]", "")
            item["digg"] = sel.xpath('.//span[@class="fr digg"]/@digg').extract()[0].strip()
            item["bury"] = sel.xpath('.//span[@class="fr digg"]/@bury').extract()[0].strip()
            yield Request(link, meta={"base_item": item}, callback=self.parse_blog_detail)
            break
        page_nav = response.xpath("//div[@class='page_nav']/a/@href").extract()
        for l in page_nav:
            redis.sadd(self.name + ":blog_list", "http://blog.csdn.net" + l)

    def parse_blog_detail(self, response):
        sel = Selector(response)
        item = response.meta["base_item"]
        url = response.url
        user_item = {}
        item["title"] = sel.xpath("//span[@class='link_title']//text()").extract()[0].strip()
        item["content"] = "".join(sel.xpath("//div[@id='article_content']/child::*").extract())
        item['url'] = url
        item['tags'] = sel.xpath('//span[@class="link_categories"]//text()').extract()
        item["postdate"] = sel.xpath("//span[@class='link_postdate']/text()").extract()[0].strip()
        item["view_count"] = sel.xpath("//span[@class='link_view']/text()").re(NUM_RE)[0]
        item["comments_count"] = sel.xpath("//span[@class='link_comments']/text()").re(NUM_RE)[0].strip()
        item["image_urls"] = sel.xpath('//div[@id="article_content"]//img/@src').extract()
        item["file_urls"] = item["image_urls"]
        log.info("url is " + url)
        next_article = sel.xpath("//li[@class='next_article']/a/@href").extract()
        if next_article:
            redis.sadd(self.name + ":blog_detail", "http://blog.csdn.net" + next_article[0].strip())
        prev_article = sel.xpath("//li[@class='prev_article']/a/@href").extract()
        if prev_article:
            redis.sadd(self.name + ":blog_detail", "http://blog.csdn.net" + prev_article[0].strip())
        item["spider"] = self.name
        item["db"] = self.name
        item["collection"] = "blog"
        item["category"] = "doc"
        item["created"] = int(time.time())
        item["image_urls"] = sel.xpath('//div[@class="content markitup-box"]//img/@src').extract()
        item["file_urls"] = item["image_urls"]

        user_item["fullname"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/text()").extract()[
            0].strip()
        user_item["url"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/@href").extract()[0].strip()
        user_item["name"] = user_item["url"].replace("http://my.csdn.net/", "")
        item["username"] = user_item["name"]
        user_item["avatar"] = sel.xpath(".//div[@id='blog_userface']//img/@src").extract()[0].strip()
        blog_rank = sel.xpath('//ul[@id="blog_rank"]/li/span/text()').re(NUM_RE)
        user_item["views"] = blog_rank[0]
        user_item["credits"] = blog_rank[1]
        user_item["rank"] = blog_rank[2]
        user_item["spider"] = self.name
        user_item["db"] = self.name
        user_item["collection"] = "user"
        user_item["category"] = "doc"
        user_item["created"] = int(time.time())
        user_item["image_urls"] = [user_item["avatar"]]
        user_item["file_urls"] = user_item["image_urls"]
        return [item,user_item]
