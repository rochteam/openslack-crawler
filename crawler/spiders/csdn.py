# -*- coding: utf-8 -*-
import time
import sys
import re
from scrapy.http import Request
from scrapy.linkextractors import LinkExtractor
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

    def __init__(self, a=None, b=None, c=None):
        super(CsdnSpider, self).__init__()
        self.logger.info('spider init %s', self.name)

    def start_requests(self):
        yield Request("http://blog.csdn.net/index.html?&page=1", self.parse_blog_list)
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
        # Rule(LinkExtractor(allow=('article/details/[0-9]+'), allow_domains=('blog.csdn.net')), callback='parse_item'),
        # Rule(LinkExtractor(allow=('index.html'), allow_domains=('blog.csdn.net'))),
        # Rule(LinkExtractor(allow=('details.html'), allow_domains=('blog.csdn.net'))),
        # Rule(LinkExtractor(allow=('experts.html'), allow_domains=('blog.csdn.net'))),
        # Rule(LinkExtractor(allow=('article/list'), allow_domains=('blog.csdn.net'))),
        # Rule(LinkExtractor(allow=('/[a-z_0-9]+$'), allow_domains=('blog.csdn.net'))),
        # Rule(LinkExtractor(allow=r'article/list/[0-9]{1,20}'), callback='parse_item', follow=True),
        # Rule(LinkExtractor(allow=r'article/details/[0-9]{1,20}'), callback='parse_article', follow=True),
        # Rule(LinkExtractor(allow=r"^http://(news|cloud|mobile|sd|programmer)\.csdn.net($|(/[a-zA-Z]+/\d+$))"),callback="parse_next_page",follow=True),
    )

    def parse_blog_list(self, response):
        print "+++++++++++++++++++++++++++parse_blog_list +++++++++++++++++++++++++++"
        page_nav = response.xpath("//div[@class='page_nav']/a/@href").extract()
        for l in page_nav:
            redis.sadd(self.name + ":blog_list", "http://blog.csdn.net" + l)
        for sel in response.xpath("//dl[@class='blog_list clearfix']"):
            item = {}
            link = sel.xpath('.//h3/a/@href').extract()[0].strip()
            categorys = sel.xpath('.//div[@class="blog_list_b_l fl"]//a/text()').extract()
            if categorys:
                item["category"] = categorys[0].strip()

            # item["digg"] = int(sel.xpath('.//span[@class="fr digg"]/@digg').extract()[0].strip())
            # item["bury"] = int(sel.xpath('.//span[@class="fr digg"]/@bury').extract()[0].strip())
            yield Request(link, meta={"base_item": item}, callback=self.parse_blog_detail)

    def parse_blog_detail(self, response):
        print "+++++++++++++++++++++++++++parse_blog_detail+++++++++++++++++++++++++++"
        sel = Selector(response)
        item = response.meta["base_item"] if "base_item" in response.meta else {}
        url = response.url
        user_item = {}
        item["spider"] = self.name
        item["created"] = int(time.time())
        item["id"] = "csdn-"+url.split("/")[-1]
        item["title"] = sel.xpath("//span[@class='link_title']//text()").extract()[0].strip()
        item["content"] = "".join(sel.xpath("//div[@id='article_content']/child::*").extract())
        item['url'] = url
        item['tags'] = sel.xpath('//span[@class="link_categories"]//text()').extract()
        item["postdate"] = sel.xpath("//span[@class='link_postdate']/text()").extract()[0].strip()
        item["view_count"] = sel.xpath("//span[@class='link_view']/text()").re(NUM_RE)[0]
        item["comments_count"] = sel.xpath("//span[@class='link_comments']/text()").re(NUM_RE)[0].strip()
        item["image_urls"] = sel.xpath('//div[@id="article_content"]//img/@src').extract()
        item["file_urls"] = item["image_urls"]

        user_item["fullname"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/text()").extract()[0].strip()
        user_item["url"] = sel.xpath("//div[@id='blog_userface']//a[@class='user_name']/@href").extract()[0].strip()
        user_item["name"] = user_item["url"].replace("http://my.csdn.net/", "")
        item["username"] = user_item["name"]
        user_item["id"]="csdb-"+user_item["name"]
        redis.sadd(self.name + ":blog_user", "http://blog.csdn.net/" + user_item["name"])
        user_item["avatar"] = sel.xpath(".//div[@id='blog_userface']//img/@src").extract()[0].strip()
        blog_rank = sel.xpath('//ul[@id="blog_rank"]/li/span/text()').re(NUM_RE)
        user_item["views"] = blog_rank[0]
        user_item["credits"] = blog_rank[1]
        user_item["rank"] = blog_rank[2] if len(blog_rank) == 3 else 0
        user_item["image_urls"] = [user_item["avatar"]]
        user_item["file_urls"] = user_item["image_urls"]
        return [item, user_item]

    def parse_blog_user(self, response):
        print "+++++++++++++++++++++++++++parse_blog_user+++++++++++++++++++++++++++"
        for url in response.xpath("//ul[@class='list_4']//a/@href").extract():
            redis.sadd(self.name + ":blog_user", url.strip())
        for url in response.xpath("//div[@id='papelist']//a/@href").extract():  # 用户列表获取列表
            redis.sadd(self.name + ":blog_user", "http://blog.csdn.net" + url.strip())
        for url in response.xpath("//div[@id='article_list']//h1//a/@href").extract():  # 获取文章
            yield Request("http://blog.csdn.net" + url, callback=self.parse_blog_detail)
