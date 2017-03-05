# -*- coding:utf-8 -*-

from scrapy.item import Field
from items import BaseItem

class ZhihuUserItem(BaseItem):
    _id = Field()
    url = Field()
    img = Field()
    username = Field()
    nickname = Field()
    location = Field()
    industry = Field()
    sex = Field()
    jobs = Field()
    educations = Field()
    description = Field()
    sinaweibo = Field()
    tencentweibo = Field()

    followee_num = Field()
    follower_num = Field()

    ask_num = Field()
    answer_num = Field()
    post_num = Field()
    collection_num = Field()
    log_num = Field()

    agree_num = Field()
    thank_num = Field()
    fav_num = Field()
    share_num = Field()

    view_num = Field()
    update_time = Field()


class ZhihuAskItem(BaseItem):
    _id = Field()
    username = Field()
    url = Field()
    view_num = Field()
    title = Field()
    answer_num = Field()
    follower_num = Field()


class ZhihuAnswerItem(BaseItem):
    _id = Field()
    username = Field()
    url = Field()
    ask_title = Field()
    ask_url = Field()
    agree_num = Field()
    summary = Field()
    content = Field()
    comment_num = Field()


class ZhihuFolloweesItem(BaseItem):
    _id = Field()
    username = Field()
    followees = Field()


class ZhihuFollowersItem(BaseItem):
    _id = Field()
    username = Field()
    followers = Field()
