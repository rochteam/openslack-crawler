# -*- coding: utf-8 -*-

import redis
import scrapy
import time

from twcrawler.add_task import tasks_generator
from twcrawler.items import StackoverflowQuestionItem
from twcrawler.utils import extract_text, extract_text_null
from twcrawler import mysettings, settings
from twcrawler import metric


def build_url(task):
  return 'http://stackoverflow.com/feeds/question/%s' % task

class StackoverflowQuestionSpider(scrapy.Spider):

  handle_httpstatus_list = [
    201, 202, 203, 204, 205, 206,
    400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
    500, 501, 502, 503, 504, 505,
    ]

  name = "stackoverflow_question"
  allowed_domains = ["stackoverflow.com"]
  start = 4
  #end = 28000000
  end = 104
  #start_urls = (
  #    'http://stackoverflow.com/feeds/question/%d'%i for i in xrange(start, end)
  #)
  start_urls = tasks_generator(build_url)

  id_xpath = '//entry/id'
  rank_xpath = '//entry/rank'
  title_xpath = '//entry/title'
  tag_xpath = '//entry/category/@term'
  author_name_xpath = '//entry/author/name'
  author_uri_xpath = '//entry/author/uri'
  link_xpath = '//entry/link/@href'
  published_xpath = '//entry/published'
  updated_xpath = '//entry/updated'
  content_xpath = '//entry/summary'

  HTML_200_STRING = []
  HTML_404_STRING = []
  HTML_MOBILE_STRING = []

  def parse(self, response):
    if response.status in self.handle_httpstatus_list:
      return

    if 'StackExchange.ready' in response.body and "Page Not Found" in response.body:
      return

    response.selector.remove_namespaces()

    ids = extract_text_null(self, 'id', response)
    ranks = extract_text_null(self, 'rank', response)
    titles = extract_text_null(self, 'title', response)
    tags = response.xpath(self.tag_xpath).extract()
    author_names = extract_text_null(self, 'author_name', response)
    author_uris = extract_text_null(self, 'author_uri', response)
    links = response.xpath(self.link_xpath).extract()
    publisheds = extract_text_null(self, 'published', response)
    updateds = extract_text_null(self, 'updated', response)
    contents = extract_text_null(self, 'content', response)

    item = StackoverflowQuestionItem()
    item['uid'] = response.url.rstrip('/').split('/')[-1]
    item['rank'] = ranks[0]
    item['title'] = titles[0]
    item['tags'] = tags
    item['author_name'] = author_names[0]
    item['author_uri'] = author_uris[0]
    item['author_uid'] = author_uris[0].split('/')[-1]
    item['link'] = links[0]
    item['published'] = publisheds[0]
    item['updated'] = updateds[0]
    item['content'] = contents[0]
    item['answers'] = []

    pipeline = metric.get_redis().pipeline()
    for i in xrange(1, len(ids)):
      answer = {}
      answer['uid'] = ids[i].split('#')[-1]
      pipeline.hincrby(':'.join([metric.metric_key, 'answer']), answer['uid'], 1)
      answer['rank'] = ranks[i]
      answer['author_name'] = author_names[i]
      answer['author_uri'] = author_uris[i]
      answer['author_uid'] = author_uris[i].split('/')[-1]
      answer['link'] = links[i]
      answer['published'] = publisheds[i]
      answer['updated'] = updateds[i]
      answer['content'] = contents[i]
      item['answers'].append(answer)
    pipeline.execute()

    return item

  def parse_list(self, reponse):
    pass

  def parse_page(self, response):
    pass