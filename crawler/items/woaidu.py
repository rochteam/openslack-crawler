# -*-coding:utf-8-*-
from items import BaseItem
from scrapy.item import Field


class WoaiduCrawlerItem(BaseItem):
    mongodb_id = Field()
    book_name = Field()
    alias_name = Field()
    author = Field()
    book_description = Field()
    book_covor_image_path = Field()
    book_covor_image_url = Field()
    book_download = Field()
    book_file_url = Field()
    book_file = Field()  # only use for save tho single mongodb
    book_file_id = Field()  # only use for save to shard mongodb
    original_url = Field()
