
# -*- coding: utf-8 -*-

import exceptions
import scrapy

from scrapy import log

from twcrawler.items import StackoverflowUserItem
from twcrawler.utils import Colorizing, extract_text


class StackoverflowUserSpider(scrapy.Spider):
  #handle_httpstatus_list = [404, 500, 503, 204, 403]
  #handle_httpstatus_list = [204, 400, 403, 404, 410, 500, 501, 503, 504]
  handle_httpstatus_list = [
      201, 202, 203, 204, 205, 206,
      400, 401, 402, 403, 404, 405, 406, 407, 408, 409, 410, 411, 412, 413, 414, 415, 416, 417,
      500, 501, 502, 503, 504, 505,
      ]
  name = "stackoverflow_user"
  allowed_domains = ["stackoverflow.com"]
  #user_cnt = 5000000
  user_cnt = 1000000
  start_urls = (
    'http://stackoverflow.com/users/%d'%i for i in xrange(1, user_cnt)
  )

  #name_xpath = '//*[@id="user-displayname"]/a'
  name_xpath = '//div[@class="user-card"]//span[@class="name"]'
  #website_xpath = '//div[@id="large-user-info"]//a[@class="url"]'
  website_xpath = '//div[@class="user-links"]//a[@class="url"]'
  location_xpath = '//div[@id="large-user-info"]//td[@class="label adr"]'
  age_xpath = '//div[@id="large-user-info"]//td[text()="age"]/following-sibling::td'
  #member_time_xpath = '//*[@id="large-user-info"]//td[@class="cool"]'
  member_time_xpath = '//*[text()[contains(.,"Member for")]]/span[2]'
  #last_visit_time_xpath = '//div[@id="large-user-info"]//*[@class="relativetime"]'
  last_visit_time_xpath = '//div[@class="user-links"]//span[@class="relativetime"]'
  #visitors_xpath = '//div[@id="large-user-info"]//td[text()="profile views"]/following-sibling::td'
  visitors_xpath = '//span[@class="icon-eye"]/..'
  #about_me_xpath = '//div[@id="large-user-info"]//*[@class="user-about-me note"]'
  about_me_xpath = '//div[@class="bio"]'
  #reputation_xpath = '//div[@id="large-user-info"]//*[@class="reputation"]/span/a'
  reputation_xpath = '//div[@class="reputation"]'
  #gold_badge_xpath = '//div[@id="large-user-info"]//span[@class="badge1"]/following-sibling::*'
  gold_badge_xpath = '//span[@class="badge1-alternate"]/span[2]'
  #silver_badge_xpath = '//div[@id="large-user-info"]//span[@class="badge2"]/following-sibling::*'
  silver_badge_xpath = '//span[@class="badge2-alternate"]/span[2]'
  #bronze_badge_xpath = '//div[@id="large-user-info"]//span[@class="badge3"]/following-sibling::*'
  bronze_badge_xpath = '//span[@class="badge3-alternate"]/span[2]'

  # for mobile page
  #name_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="name"]/following-sibling::*'
  name_xpath_mobile = '//*[@id="mainbar-full"]//div[@class="name"]'
  #website_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="website"]/following-sibling::*/a'
  website_xpath_mobile = '//*[@id="mainbar-full"]//a[@class="url"]'
  #location_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="location"]/following-sibling::*'
  location_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="location"]/following-sibling::*'
  #age_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="age"]/following-sibling::*'
  age_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="age"]/following-sibling::*'
  #member_time_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="member for"]/following-sibling::*/span'
  member_time_xpath_mobile = '//*[@id="mainbar-full"]//span[text()="member for"]/following-sibling::*/span'
  last_visit_time_xpath_mobile = '//*[@id="mainbar-full"]//div[text()="seen"]/following-sibling::*/span/span'
  visitors_xpath_mobile = '/none_node'
  about_me_xpath_mobile = '//*[@id="user-about-me"]'
  reputation_xpath_mobile = '//*[@id="user-panel-reputation"]//span[@class="count"]'
  gold_badge_xpath_mobile = '//*[@id="mainbar-full"]//span[@class="badge1"]/following-sibling::*'
  silver_badge_xpath_mobile = '//*[@id="mainbar-full"]//span[@class="badge2"]/following-sibling::*'
  bronze_badge_xpath_mobile = '//*[@id="mainbar-full"]//span[@class="badge3"]/following-sibling::*'

  #answers_xpath = '//*[@id="user-panel-answers"]//span[@class="count"]'
  answers_xpath = '//div[@class="user-stats"]//span[1]'
  #top_answers_xpath = '//*[@id="user-panel-answers"]/div[2]/table/tbody//a'
  top_answers_xpath = '//div[@class="row post-container"]//a'
  #top_answers_votes_xpath = '//*[@id="user-panel-answers"]//table/tbody//td[@class="count-cell"]/div'
  top_answers_votes_xpath = '//div[@class="row post-container"]//span[contains(@class, "vote")]'
  #questions_xpath = '//*[@id="user-panel-questions"]//span[@class="count"]'
  questions_xpath = '//div[@class="user-stats"]//span[2]'
  top_questions_xpath = '//*[@id="user-panel-questions"]/div[2]/table/tbody//a'
  top_questions_votes_xpath = '//*[@id="user-panel-questions"]//table/tbody//td[@class="count-cell"]/div'

  # for mobile page
  answers_xpath_mobile = answers_xpath
  top_answers_xpath_mobile = '//*[@id="user-panel-answers"]//a[@class="answer-hyperlink "]' # BUG! note the last space!
  top_answers_votes_xpath_mobile = '//*[@id="user-panel-answers"]//div[@class="mini-counts" or @class="mini-counts answered-accepted"]'
  questions_xpath_mobile = questions_xpath
  top_questions_xpath_mobile = '//*[@id="user-panel-questions"]//a[@class="question-hyperlink"]' # As above BUG, no last space
  top_questions_votes_xpath_mobile = '//*[@id="user-panel-questions"]//div[@class="mini-counts" or @class="mini-counts answered-accepted"]'

  #top_tags_xpath = '//*[@id="user-panel-tags"]//a[@class="post-tag"]'
  top_tags_xpath = '//a[@class="post-tag"]'
  #top_tags_votes_xpath = '//*[@id="user-panel-tags"]//div[@class="answer-votes"]'
  top_tags_votes_xpath = '//div[@class="number" and contains(string(.), "Score")]/text()[2]'

  #for mobile page
  top_tags_xpath_mobile = top_tags_xpath
  top_tags_votes_xpath_mobile = top_tags_votes_xpath


  def parse(self, response):
    #log.msg( Colorizing.colorize(response) )
    #if not 'StackExchange.ready' in response.body:
      #return scrapy.Request(url=response.url)
    if response.status in self.handle_httpstatus_list:
      return None
    item = StackoverflowUserItem()
    item['uid'] = int(response.meta['origin_url'].split('/')[-1])
    for attr in ['name', 'website', 'location', 'age', 'member_time', 'last_visit_time', 'visitors',
                 'reputation', 'gold_badge', 'silver_badge', 'bronze_badge', 'answers', 'questions'
                 ]:
      ret_list = extract_text(self, attr, response)
      if ret_list:
        item[attr] = ret_list[0]
      else:
        item[attr] = None

    try:
        item['visitors'] = response.xpath('string(%s)'%self.visitors_xpath).extract()[0].strip()
    except:
        pass

    if "ios-mobile" in response.body:
      try:
        item['about_me'] = response.xpath(self.about_me_xpath_mobile).extract()[0]
      except exceptions.IndexError:
        item['about_me'] = None
      item['top_answers'] = zip( response.xpath(self.top_answers_xpath_mobile).extract(),
                               extract_text(self, 'top_answers_votes', response))
      item['top_questions'] = zip( response.xpath(self.top_questions_xpath_mobile).extract(),
                               extract_text(self, 'top_questions_votes', response))
      item['top_tags'] = zip( extract_text(self, 'top_tags', response), extract_text(self, 'top_tags_votes', response))
      item['source'] = 'Mobile'
    else:
      item['about_me'] = response.xpath(self.about_me_xpath).extract()[0]
      item['top_answers'] = zip( response.xpath(self.top_answers_xpath).extract(),
                               extract_text(self, 'top_answers_votes', response))
      item['top_questions'] = zip( response.xpath(self.top_questions_xpath).extract(),
                               extract_text(self, 'top_questions_votes', response))
      #item['top_tags'] = zip( extract_text(self, 'top_tags', response), extract_text(self, 'top_tags_votes', response))
      item['top_tags'] = zip( extract_text(self, 'top_tags', response), response.xpath(self.top_tags_votes_xpath).extract())
      item['source'] = 'Desktop'

    for key in item:
        if isinstance(item[key], basestring):
            item[key] = item[key].strip()
        elif isinstance(item[key], list):
            for i in xrange(len(item[key])):
                try:
                    item[key][i] = (item[key][i][0].strip(), item[key][i][1].strip())
                except IndexError:
                    pass

    return item


def test():
  pass

if __name__ == '__main__':
  test()