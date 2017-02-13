import time


class StandardFieldPipeline(object):
    def __init__(self):
        pass

    #     self.file = codecs.open('cnblogs.json', 'w', encoding='utf-8')

    def process_item(self, item, spider):
        if "digg" not in item:
            item["digg"]=0
        if "bury" not in item:
            item["bury"]=0
        item["spider"] = spider.name
        item["db"] = spider.name
        item["created"] = int(time.time())
        return item

    def spider_closed(self, spider):
        pass
