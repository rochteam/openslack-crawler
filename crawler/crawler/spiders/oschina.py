# coding=utf-8
from scrapy.selector import Selector
import re
from scrapy.utils.response import get_base_url
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.http import Request
from redis import StrictRedis
import time

redis = StrictRedis()
NUM_RE = re.compile(r"(\d+)")
DATE_RE = re.compile(r"(\d+\-\d+)")


class OSChinaSpider(CrawlSpider):
    name = "oschina"
    download_delay = 2
    allowed_domains = ["oschina.net"]
    # start_urls = [
    # http://www.oschina.net/blog?type=428610#catalogs
    # http://www.oschina.net/blog/more?p=1#
    # ]
    def start_requests(self):
        for url in redis.smembers(self.name + ":translate_list"):
            yield Request(url, self.parse_translate)
        # yield Request("http://www.oschina.net/translate/list?type=2", self.parse_translate)

    rules = [
        # Rule(LinkExtractor(allow=("/translate/list")), follow=True, callback='parse_translate'),
        # Rule(LinkExtractor(allow=("/translate/\w+")), follow=True, callback='parse_translate_detail')
    ]

    def parse_blog_user(self, response):
        pass

    def parse_blog_detail(self, response):
        pass

    def parse_translate(self, response):
        page_nav = response.xpath("//ul[@class='pager']//a/@href").extract()
        for l in page_nav:
            redis.sadd(self.name + ":translate_list", "http://www.oschina.net/translate/list" + l)
        for url in response.xpath('//div[@class="article"]//dt/a/@href').extract():
            yield Request(url, callback=self.parse_translate_detail)

    def parse_translate_detail(self, response):
        sel = Selector(response)
        base_url = get_base_url(response)
        item = {}
        article_sel = sel.xpath("//div[@id='OSC_Content']/div[@class='Article']")
        item['vote_count'] = article_sel.xpath(".//em[@id='vote_count']/text()").extract()[0].strip()
        item["digg"] = item['vote_count']
        item['title'] = article_sel.xpath(".//h1/text()").extract()[0].strip()
        item['title_en'] = article_sel.xpath(".//h3/a/text()").extract()[0].strip()
        item['url_en'] = article_sel.xpath(".//h3/a/@href").extract()[0].strip()
        item['content'] = "".join(article_sel.xpath(".//div[@class='TextContent']/child::*").extract())
        item['tags'] = article_sel.xpath(".//h3/a/@href").extract()
        item['favorite'] = article_sel.xpath(".//em[@id='p_attention_count']/text()").extract()[0].strip()
        item['comment_count'] = article_sel.xpath(".//a[@href='#comments']/text()").re(NUM_RE)[0].strip()
        item['postdate'] = article_sel.xpath(".//div[@class='rec_user']/text()").re(DATE_RE)[0].strip()
        item['url'] = base_url
        item["collection"] = "blog"
        item["category"] = "doc"
        item["image_urls"] = sel.xpath('.//div[@class="TextContent"]//img/@src').extract()
        item["file_urls"] = item["image_urls"]
        user_url = article_sel.xpath(".//div[@class='rec_user']/a/@href").extract()[0].strip()
        user_name = article_sel.xpath(".//div[@class='rec_user']/a/text()").extract()[0].strip()
        item['userid'] = user_url.replace("http://my.oschina.net/", "").replace("u/", "")
        user_item = {
            "url": user_url,
            "name": user_name,
            "id": item['userid'],
            "collection": "user"
        }
        user_urls = [user_url]
        user_urls.extend(article_sel.xpath(".//div[@class='contributers']/a/@href").extract())
        for l in user_urls:
            redis.sadd(self.name + ":blog_user", l)
        # time.sleep(self.download_delay)
        # print item
        return [item, user_item]
