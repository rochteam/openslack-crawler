# -*- coding: utf-8 -*-

from scrapy.item import Item, Field


class BaseItem(Item):  # 统一item
    id = Field()
    title = Field()
    url = Field()
    content = Field()
    postdate = Field()  # 2017-03-02 15:45
    tags = Field()
    username = Field()
    created = Field() # timestamp
    spider = Field()