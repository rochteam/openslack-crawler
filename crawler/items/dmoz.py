# -*- coding:utf-8 -*-
from items import BaseItem
from scrapy.item import Field


class DmozItem(BaseItem):
    name = Field()
    description = Field()
    url = Field()
