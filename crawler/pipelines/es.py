# -*-coding:utf-8-*-
import logging as log
import hashlib, json
from elasticsearch import Elasticsearch


class ElasticSearchPipeline(object):
    def __init__(self, settings):
        basic_auth = {'username': settings.get('ELASTICSEARCH_USERNAME'),
                      'password': settings.get('ELASTICSEARCH_PASSWORD')}
        if settings.get('ELASTICSEARCH_PORT'):
            uri = "%s:%d" % (settings.get('ELASTICSEARCH_SERVER'), settings.get('ELASTICSEARCH_PORT'))
        else:
            uri = "%s" % (settings.get('ELASTICSEARCH_SERVER'))
        self.es = Elasticsearch([uri], basic_auth=basic_auth)
        self.settings = settings

    @classmethod
    def from_crawler(cls, crawler):
        pipe = cls(crawler.settings)
        return pipe

    def process_item(self, item, spider):
        exists = self.es.exists(self.settings.get('ELASTICSEARCH_INDEX'), self.settings.get('ELASTICSEARCH_TYPE'), item['id'])
        # if self.__get_uniq_key() is None:
        # log.info("ELASTICSEARCH_UNIQ_KEY is NONE")
        if not exists:
            log.info("item not exists")
            print json.dumps(item)
            self.es.create(self.settings.get('ELASTICSEARCH_INDEX'), self.settings.get('ELASTICSEARCH_TYPE'), item["id"], item)
        else:
            self.es.index(self.settings.get('ELASTICSEARCH_INDEX'), self.settings.get('ELASTICSEARCH_TYPE'), item, item["id"])  # self._get_item_key(item)
        log.debug("Item send to Elastic Search %s" % (self.settings.get('ELASTICSEARCH_INDEX')))
        return item

    def _get_item_key(self, item):
        uniq = self.__get_uniq_key()
        if isinstance(uniq, list):
            values = [item[key] for key in uniq]
            value = ''.join(values)
        else:
            value = uniq
        return hashlib.sha1(value).hexdigest()

    def __get_uniq_key(self):
        if not self.settings.get('ELASTICSEARCH_UNIQ_KEY'):
            return self.settings.get('ELASTICSEARCH_UNIQ_KEY')
        else:
            return None
