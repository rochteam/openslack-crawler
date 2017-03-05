# -*- coding:utf-8 -*-
from scrapy.item import Field
from items import BaseItem

class GithubRepoItem(BaseItem):
    _id = Field()
    url = Field()
    username = Field()
    name = Field()
    description = Field()
    update_date = Field()
    star_num = Field()
    watch_num = Field()
    fork_num = Field()
    language = Field()
    type = Field()
    commit_num = Field()
    branch_num = Field()
    tag_num = Field()
    pull_num = Field()
    issue_num = Field()

class GithubUserItem(BaseItem):
    # 通用字段
    _id = Field()
    url = Field()
    username = Field()
    nickname = Field()
    user_id = Field()
    type = Field()

    company = Field()
    location = Field()
    website = Field()
    email = Field()
    update_time = Field()

    # 用户
    join_date = Field()

    followee_num = Field()
    follower_num = Field()
    star_num = Field()
    organizations = Field()

    # 单位
    member_num = Field()