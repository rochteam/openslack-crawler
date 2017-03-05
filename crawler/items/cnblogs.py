# -*- coding: utf-8 -*-
from items import BaseItem
from scrapy.item import Field


class CnblogsItem(BaseItem):
    listUrl = Field()
