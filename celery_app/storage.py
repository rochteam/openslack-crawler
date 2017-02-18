# -*- coding: utf-8 -*-
import datetime, json
import traceback, hashlib
from pymongo.mongo_client import MongoClient
from pymongo import errors
from pymongo.read_preferences import ReadPreference
import settings
from kafka import SimpleClient, SimpleProducer
from celery.utils.log import get_task_logger
from elasticsearch import Elasticsearch, RequestsHttpConnection

logger = get_task_logger(__name__)

STORAGE_MAPS = {}


class BasePipeline:
    name = ""

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)
        STORAGE_MAPS[self.name] = self.__class__.__name__

    @classmethod
    def from_settings(cls, settings):
        pass

    @classmethod
    def from_crawler(cls, crawler):
        pass

    def save(self, *args, **kwargs):
        pass

    def save_many(self, *args, **kwargs):
        pass


class ElasticSearchPipeline(BasePipeline):
    name = "elasticsearch"

    def __init__(self, es, doc_type, index):
        BasePipeline.__init__(self)
        self.es = es
        self.index = index
        self.doc_type = doc_type

    @classmethod
    def from_settings(cls, settings):
        basic_auth = {'username': settings.get('ELASTICSEARCH_USERNAME'),
                      'password': settings.get('ELASTICSEARCH_PASSWORD')}
        if settings.get('ELASTICSEARCH_PORT'):
            uri = "%s:%d" % (settings.get('ELASTICSEARCH_SERVER'), settings.get('ELASTICSEARCH_PORT'))
        else:
            uri = "%s" % (settings.get('ELASTICSEARCH_SERVER'))
        es = Elasticsearch(uri, basic_auth=basic_auth, connection_class=RequestsHttpConnection)
        return cls(es)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def save(self, body, doc_id):
        print "save"
        self.es.index(body=body, index=self.index, doc_type=self.doc_type, id=doc_id)
        if self.__get_uniq_key() is None:
            logger.info("ELASTICSEARCH_UNIQ_KEY is NONE")
            self.es.index(dict(body), self.index, self.doc_type, id=body['id'], op_type='create')
        else:
            self.es.index(dict(body), self.settings.get('ELASTICSEARCH_INDEX'), self.settings.get('ELASTICSEARCH_TYPE'),
                          self._get_item_key(body))
        logger.debug("Item send to Elastic Search %s" %
                     (self.settings.get('ELASTICSEARCH_INDEX')))

    def save_many(self, body):
        print "save many"
        self.es.bulk(body=body, index=self.index, doc_type=self.doc_type)

    def refresh(self, index):
        self.es.indices.refresh(index=index)

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


class KafkaPipeline(BasePipeline):
    name = "kafka"

    def __init__(self, producer, topic_prefix, aKafka):
        BasePipeline.__init__(self)
        self.producer = producer
        self.topic_prefix = topic_prefix
        self.topic_list = []
        self.kafka = aKafka

    @classmethod
    def from_settings(cls, settings):
        kafka = SimpleClient(settings['KAFKA_HOSTS'])
        producer = SimpleProducer(kafka)
        topic_prefix = settings['KAFKA_TOPIC_PREFIX']
        return cls(producer, topic_prefix, kafka)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        datum = dict(item)
        datum["timestamp"] = datetime.datetime.utcnow().isoformat()
        prefix = self.topic_prefix
        appid_topic = "{prefix}.crawled_{spider}".format(prefix=prefix, spider=spider.name)
        self.checkTopic(appid_topic)
        try:
            message = json.dumps(datum)
            self.producer.send_messages(appid_topic, message)
        except Exception, e:
            traceback.print_exc()
            message = 'json failed to parse'
        return None

    def checkTopic(self, topicName):
        if topicName not in self.topic_list:
            self.kafka.ensure_topic_exists(topicName)
            self.topic_list.append(topicName)


def not_set(string):
    """ Check if a string is None or ''
    :returns: bool - True if the string is empty
    """
    if string is None:
        return True
    elif string == '':
        return True
    return False


class MongoDBPipeline(object):
    """ MongoDB pipeline class """
    name = "mongo"
    # Default options
    config = {
        'uri': 'mongodb://localhost:27017',
        'fsync': False,
        'write_concern': 0,
        'database': 'openslack',
        'collection': 'doc',
        'replica_set': None,
        'unique_key': "url",
        'buffer': None,
        'append_timestamp': False,
        'stop_on_duplicate': 0,
    }

    # Item buffer
    current_item = 0
    item_buffer = []

    # Duplicate key occurence count
    duplicate_key_count = 0

    def __init__(self, producer, topic_prefix, aKafka):
        BasePipeline.__init__(self)

    def load_spider(self, spider):
        self.crawler = spider.crawler
        self.settings = spider.settings

        if not hasattr(spider, 'update_settings') and hasattr(spider, 'custom_settings'):
            self.settings.setdict(spider.custom_settings or {}, priority='project')

    def open_spider(self, spider):
        self.load_spider(spider)
        # Configure the connection
        self.configure()
        if hasattr(spider, 'conf'):
            self.conf = spider.conf
            self.config["collection"] = self.conf["COLLECTION"]

        if self.config['replica_set'] is not None:
            self.connection = MongoClient(
                self.config['uri'],
                replicaSet=self.config['replica_set'],
                w=self.config['write_concern'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY_PREFERRED)
        else:
            # Connecting to a stand alone MongoDB
            self.connection = MongoClient(
                self.config['uri'],
                fsync=self.config['fsync'],
                read_preference=ReadPreference.PRIMARY)

        # Set up the collection

        database = self.connection[self.config['database']]
        self.collection = database[self.config['collection']]
        logger.info(u'Connected to MongoDB {0}, using "{1}/{2}"'.format(
            self.config['uri'],
            self.config['database'],
            self.config['collection']))

        # Ensure unique index
        if self.config['unique_key']:
            self.collection.ensure_index(self.config['unique_key'], unique=True)
            logger.info('uEnsuring index for key {0}'.format(
                self.config['unique_key']))

        # Get the duplicate on key option
        if self.config['stop_on_duplicate']:
            tmpValue = self.config['stop_on_duplicate']
            if tmpValue < 0:
                logger.info(
                    (
                        u'Negative values are not allowed for'
                        u' MONGODB_STOP_ON_DUPLICATE option.'
                    ),
                    level=logger.ERROR
                )
                raise SyntaxError(
                    (
                        'Negative values are not allowed for'
                        ' MONGODB_STOP_ON_DUPLICATE option.'
                    )
                )
            self.stop_on_duplicate = self.config['stop_on_duplicate']
        else:
            self.stop_on_duplicate = 0

    def configure(self):
        """ Configure the MongoDB connection """
        # Handle deprecated configuration
        if not not_set(self.settings['MONGODB_HOST']):
            logger.info(
                u'DeprecationWarning: MONGODB_HOST is deprecated',
                level=logger.WARNING)
            mongodb_host = self.settings['MONGODB_HOST']

            if not not_set(self.settings['MONGODB_PORT']):
                logger.info(
                    u'DeprecationWarning: MONGODB_PORT is deprecated',
                    level=logger.WARNING)
                self.config['uri'] = 'mongodb://{0}:{1:i}'.format(
                    mongodb_host,
                    self.settings['MONGODB_PORT'])
            else:
                self.config['uri'] = 'mongodb://{0}:27017'.format(mongodb_host)

        if not not_set(self.settings['MONGODB_REPLICA_SET']):
            if not not_set(self.settings['MONGODB_REPLICA_SET_HOSTS']):
                logger.info(
                    (
                        u'DeprecationWarning: '
                        u'MONGODB_REPLICA_SET_HOSTS is deprecated'
                    ),
                    level=logger.WARNING)
                self.config['uri'] = 'mongodb://{0}'.format(
                    self.settings['MONGODB_REPLICA_SET_HOSTS'])

        # Set all regular options
        options = [
            ('uri', 'MONGODB_URI'),
            ('fsync', 'MONGODB_FSYNC'),
            ('write_concern', 'MONGODB_REPLICA_SET_W'),
            ('database', 'MONGODB_DATABASE'),
            ('collection', 'MONGODB_COLLECTION'),
            ('replica_set', 'MONGODB_REPLICA_SET'),
            ('unique_key', 'MONGODB_UNIQUE_KEY'),
            ('buffer', 'MONGODB_BUFFER_DATA'),
            ('append_timestamp', 'MONGODB_ADD_TIMESTAMP'),
            ('stop_on_duplicate', 'MONGODB_STOP_ON_DUPLICATE')
        ]

        for key, setting in options:
            if not not_set(self.settings[setting]):
                self.config[key] = self.settings[setting]

        # Check for illegal configuration
        if self.config['buffer'] and self.config['unique_key']:
            logger.info(
                (
                    u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                    u'and MONGODB_UNIQUE_KEY is not supported'
                ),
                level=logger.ERROR)
            raise SyntaxError(
                (
                    u'IllegalConfig: Settings both MONGODB_BUFFER_DATA '
                    u'and MONGODB_UNIQUE_KEY is not supported'
                ))

    def process_item(self, item, spider):
        """ Process the item and add it to MongoDB
        :type item: Item object
        :param item: The item to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        # item = dict(self._get_serialized_fields(item))
        spider.action_successful = True
        if self.config['buffer']:
            self.current_item += 1
            if self.config['append_timestamp']:
                item['timestamp'] = datetime.datetime.now()
            self.item_buffer.append(item)
            if self.current_item == self.config['buffer']:
                self.current_item = 0
                return self.insert_item(self.item_buffer, spider)
            else:
                return item
        return self.insert_item(item, spider)

    def close_spider(self, spider):
        """ Method called when the spider is closed
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: None
        """
        if self.item_buffer:
            self.insert_item(self.item_buffer, spider)

    def insert_item(self, item, spider):
        """ Process the item and add it to MongoDB
        :type item: (Item object) or [(Item object)]
        :param item: The item(s) to put into MongoDB
        :type spider: BaseSpider object
        :param spider: The spider running the queries
        :returns: Item object
        """
        database = self.connection[item["db"]]
        self.collection = database[item["collection"]]
        if not isinstance(item, list):
            item = dict(item)

            if self.config['append_timestamp']:
                item['updated'] = datetime.datetime.utcnow()

        if self.config['unique_key'] is None:
            try:
                self.collection.insert(item, continue_on_error=True)
                logger.info(
                    u'Stored item(s) in MongoDB {0}/{1}'.format(
                        self.config['database'], self.config['collection']),
                    level=logger.DEBUG,
                    spider=spider)
            except errors.DuplicateKeyError:
                logger.info(u'Duplicate key found', level=logger.DEBUG)
                if (self.stop_on_duplicate > 0):
                    self.duplicate_key_count += 1
                    if (self.duplicate_key_count >= self.stop_on_duplicate):
                        self.crawler.engine.close_spider(
                            spider,
                            'Number of duplicate key insertion exceeded'
                        )
                pass
        else:
            key = {}
            if isinstance(self.config['unique_key'], list):
                for k in dict(self.config['unique_key']).keys():
                    key[k] = item[k]
            else:
                key[self.config['unique_key']] = item[self.config['unique_key']]

            self.collection.update(key, {"$set": item}, upsert=True)
            logger.info(
                u'Stored item(s) in MongoDB {0}/{1}'.format(
                    self.config['database'], self.config['collection']))
            spider.log("Item saved.", logger.INFO)
        return item


#!/usr/bin/python
# -*-coding:utf-8-*-

import os
import itertools
import gridfs
import hashlib
import urlparse
import traceback
import datetime
import logging as log
from scrapy.item import Item
from urlparse import urlparse
import logging
from twisted.internet import defer
from scrapy.http import Request
from crawler.utils import color
from scrapy.utils.misc import arg_to_iter
from pymongo import MongoClient
from twisted.internet.defer import Deferred, DeferredList
from crawler.utils.select_result import list_first_item
from scrapy.pipelines.files import FileException, FilesPipeline, FSFilesStore
from scrapy.exceptions import DropItem, NotConfigured
import urllib
from scrapy.utils.log import failure_to_exc_info

logger = logging.getLogger(__name__)


class NofilesDrop(DropItem):
    """Product with no files exception"""

    def __init__(self, original_url="", *args):
        self.original_url = original_url
        self.style = color.color_style()
        DropItem.__init__(self, *args)

    def __str__(self):  #####for usage: print e
        print self.style.ERROR("DROP(NofilesDrop):" + self.original_url)

        return DropItem.__str__(self)


class MongodbFilesStore(FSFilesStore):
    """
        save book file to gridfs of mongodb.
    """

    ShardMONGODB_SERVER = "localhost"
    ShardMONGODB_PORT = 27017
    ShardMONGODB_DB = "openslack"
    GridFs_Collection = "fs"

    def __init__(self, shard_server, shard_port, shard_db, shard_gridfs_collection):
        self.style = color.color_style()
        self.shard_gridfs_collection = shard_gridfs_collection
        try:
            self.client = MongoClient(shard_server, shard_port)
            # self.inti_fs(shard_db)
        except Exception as e:
            print self.style.ERROR("ERROR(MongodbFilesStore): %s" % (str(e),))
            traceback.print_exc()

    def inti_fs(self, key):
        self.db = self.client[key]
        self.fs = gridfs.GridFS(self.db, self.shard_gridfs_collection)

    def persist_file(self, key, file_content, info, filename,url=None):
        self.inti_fs(key.split("_")[0])
        contentType = os.path.splitext(filename)[1][1:].lower()
        book_file_id = self.fs.put(file_content, _id=key, filename=filename, contentType=contentType,url=url)
        checksum = self.fs.get(book_file_id).md5
        return (book_file_id, checksum)

    def stat_file(self, key, info):
        """
            the stat is the file key dir,
            the last_modified is the file that saved to the file key dir.
        """
        self.inti_fs(key.split("_")[0])
        checksum = self.fs.get(key).md5
        last_modified = self.fs.get(key).upload_date

        return {'last_modified': last_modified, 'checksum': checksum}


class MongodbFilesPipeline(FilesPipeline):
    """
        This is for download the book file and then define the book_file_id
        field to the file's gridfs id in the mongodb.
    """

    MEDIA_NAME = 'mongodb_openslackfile'
    EXPIRES = 90
    FILE_CONTENT_TYPE = ['image/png','image/jpeg',"image/gif"]
    URL_GBK_DOMAIN = []
    ATTACHMENT_FILENAME_UTF8_DOMAIN = []
    FILES_RESULT_FIELD = "files"
    FILES_URLS_FIELD = "file_urls"
    STORE_SCHEMES = {
        '': MongodbFilesStore,
        'mongodb': MongodbFilesStore,
    }

    FILE_EXTENTION = ['.rar', '.zip', '.pdf', '.tar', '.tar.gz', '.tar.bz2',
                      '.xlsx', 'xls', 'ppt', 'pptx', '.doc', '.txt', '.docx',
                      '.png', '.jpg', '.jpeg', '.gif', '.bmp', '.ico',
                    ]

    def __init__(self, shard_server, shard_port, shard_db, shard_gridfs_collection, download_func=None):
        self.style = color.color_style()
        ##########from MediaPipeline###########
        self.spiderinfo = {}
        self.download_func = download_func
        ##########from MediaPipeline###########

        self.store = self._get_store(shard_server, shard_port, shard_db, shard_gridfs_collection)
        self.item_download = {}

    @classmethod
    def from_settings(cls, settings):
        cls.EXPIRES = settings.getint('BOOK_FILE_EXPIRES', 90)
        cls.BOOK_FILE_CONTENT_TYPE = settings.get('FILE_CONTENT_TYPE', [])
        cls.ATTACHMENT_FILENAME_UTF8_DOMAIN = settings.get('ATTACHMENT_FILENAME_UTF8_DOMAIN', [])
        cls.URL_GBK_DOMAIN = settings.get('URL_GBK_DOMAIN', [])
        cls.FILE_EXTENTION = settings.get('FILE_EXTENTION', cls.FILE_EXTENTION)
        shard_server = settings.get('ShardMONGODB_SERVER', "localhost")
        shard_port = settings.get('ShardMONGODB_PORT', 27017)
        shard_db = settings.get('ShardMONGODB_DB', "openslack")
        shard_gridfs_collection = settings.get('GridFs_Collection', 'fs')
        return cls(shard_server, shard_port, shard_db, shard_gridfs_collection)

    def _get_store(self, shard_server, shard_port, shard_db, shard_gridfs_collection):
        scheme = 'mongodb'
        store_cls = self.STORE_SCHEMES[scheme]
        return store_cls(shard_server, shard_port, shard_db, shard_gridfs_collection)

    def process_item(self, item, spider):
        """
            custom process_item func,so it will manage the Request result.
        """
        info = self.spiderinfo
        requests = arg_to_iter(self.get_media_requests(item, info))
        dlist = [self._process_request(r, info) for r in requests]
        dfd = DeferredList(dlist, consumeErrors=1)
        return dfd.addCallback(self.item_completed, item, info)

    def another_process_item(self, result, item, info):
        """
            custom process_item func,so it will manage the Request result.
        """

        assert isinstance(result, (Item, Request)), \
            "WoaiduBookFile pipeline' item_completed must return Item or Request, got %s" % \
            (type(result))
        if isinstance(result, Item):
            return result
        elif isinstance(result, Request):
            dlist = [self._process_request(r, info) for r in arg_to_iter(result)]
            dfd = DeferredList(dlist, consumeErrors=1)
            dfd.addCallback(self.item_completed, item, info)
            # XXX:This will cause one item maybe return many times,it depends on how many
            # times the download url failed.But it doesn't matter.Because when raise errors,
            # the items are no longer processed by further pipeline components.And when all
            # url download failed we can drop that item which book_file or book_file_url are
            # empty.
            return dfd.addCallback(self.another_process_item, item, info)
        else:
            raise NofilesDrop

    def get_media_requests(self, item, info):
        """
            Only download once per book,so it pick out one from all of the download urls.
        """
        return [Request(x) for x in item.get(self.FILES_URLS_FIELD, [])]

    def media_downloaded(self, response, request, info):
        """
            Handler for success downloads.
        """

        referer = request.headers.get('Referer')

        if response.status != 200:
            log.msg(
                format='%(medianame)s (code: %(status)s): Error downloading %(medianame)s from %(request)s referred in <%(referer)s>',
                level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                status=response.status, request=request, referer=referer)
            raise FileException(request.url, '%s: download-error' % (request.url,))

        if not response.body:
            log.msg(
                format='%(medianame)s (empty-content): Empty %(medianame)s from %(request)s referred in <%(referer)s>: no-content',
                level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                request=request, referer=referer)
            raise FileException(request.url, '%s: empty-content' % (request.url,))

        status = 'cached' if 'cached' in response.flags else 'downloaded'
        log.msg(
            format='%(medianame)s (%(status)s): Downloaded %(medianame)s from %(request)s referred in <%(referer)s>',
            level=log.DEBUG, spider=info.spider, medianame=self.MEDIA_NAME,
            status=status, request=request, referer=referer)

        if self.is_valid_content_type(response):
            raise FileException(request.url, '%s: invalid-content_type' % (request.url,))

        filename = self.get_file_name(request, response)

        if not filename:
            raise FileException(request.url, '%s: noaccess-filename' % (request.url,))

        self.inc_stats(info.spider, status)

        try:
            key = self.file_key(info.spider.name, request.url)  # return the SHA1 hash of the file url
            book_file_id, checksum = self.store.persist_file(key, response.body, info, filename,url=request.url)
        except FileException as exc:
            whyfmt = '%(medianame)s (error): Error processing %(medianame)s from %(request)s referred in <%(referer)s>: %(errormsg)s'
            log.msg(format=whyfmt, level=log.WARNING, spider=info.spider, medianame=self.MEDIA_NAME,
                    request=request, referer=referer, errormsg=str(exc))
            raise

        # print {'url': request.url, 'file_id': book_file_id, 'checksum': checksum}
        return {'url': request.url, 'file_id': book_file_id, 'checksum': checksum}

    def media_to_download(self, request, info):
        def _onsuccess(result):

            if not result:
                return  # returning None force download

            last_modified = result.get('last_modified', None)
            if not last_modified:
                return  # returning None force download

            timedelta_obj = datetime.datetime.now() - last_modified
            age_seconds = timedelta_obj.total_seconds()
            age_days = age_seconds / 60 / 60 / 24
            if age_days > self.EXPIRES:
                return  # returning None force download

            referer = request.headers.get('Referer')
            log.msg(
                format='%(medianame)s (uptodate): Downloaded %(medianame)s from %(request)s referred in <%(referer)s>',
                level=log.DEBUG, spider=info.spider,
                medianame=self.MEDIA_NAME, request=request, referer=referer)
            self.inc_stats(info.spider, 'uptodate')

            checksum = result.get('checksum', None)
            # print request.url,"----------------------"
            return {'url': request.url, 'file_id': key, 'checksum': checksum}

        key = self.file_key(info.spider.name, request.url)  # return the SHA1 hash of the file url
        dfd = defer.maybeDeferred(self.store.stat_file, key, info)
        dfd.addCallbacks(_onsuccess, lambda _: None)
        dfd.addErrback(log.err, self.__class__.__name__ + '.store.stat_file')
        return dfd

    def file_key(self, spider_name, url):
        """
            return the SHA1 hash of the file url
        """
        file_guid = hashlib.sha1(url).hexdigest()
        # return '%s_%s' % (urlparse(url).netloc,file_guid)
        return '%s_%s' % (spider_name, file_guid)

    def item_completed(self, results, item, info):
        if self.LOG_FAILED_RESULTS:
            for ok, value in results:
                if not ok:
                    logger.error(
                        '%(class)s found errors processing %(item)s',
                        {'class': self.__class__.__name__, 'item': item},
                        exc_info=failure_to_exc_info(value),
                        extra={'spider': info.spider}
                    )
        item["files"] = [{"file_id": x['file_id'], "url": x['url']} for ok, x in results if ok]
        return item

    def is_valid_content_type(self, response):
        """
            judge whether is it a valid response by the Content-Type.
        """
        content_type = response.headers.get('Content-Type', '')
        # return content_type not in self.FILE_CONTENT_TYPE
        return False

    def get_file_name(self, request, response):
        """
            Get the raw file name that the sever transfer to.

            It examine two places:Content-Disposition,url.
        """
        content_dispo = response.headers.get('Content-Disposition', '')
        filename = ""
        if content_dispo:
            for i in content_dispo.split(';'):
                if "filename" in i:
                    # XXX:use filename= for the specific case that = in the filename
                    filename = i.split('filename=')[1].strip(" \n\'\"")
                    break

        if filename:
            # XXX:it the result is:
            # MPLS TE Switching%E6%96%B9%E6%A1%88%E7%99%BD%E7%9A%AE%E4%B9%A6.pdf
            # use urllib.unquote(filename) instead
            if urlparse(request.url).netloc in self.ATTACHMENT_FILENAME_UTF8_DOMAIN:
                filename = filename.decode("utf-8")
            else:
                filename = filename.decode("gbk")
                # print "Content-Disposition:","*"*30,filename
        else:
            guessname = request.url.split('?')[0].split("/")[-1]
            # os.path.splitext:
            # Split the pathname path into a pair (root, ext) such that root + ext == path
            if os.path.splitext(guessname)[1].lower() in self.FILE_EXTENTION:
                if urlparse(request.url).netloc in self.URL_GBK_DOMAIN:
                    filename = urllib.unquote(guessname).decode("gbk").encode("utf-8")
                else:
                    filename = urllib.unquote(guessname)
                    # print "url:","*"*30,filename
            else:
                filename=guessname
            if filename.count(".")==0:
                filename+=".jpg"
        return filename


elasticsearch = ElasticSearchPipeline()
mongo = ElasticSearchPipeline()
kafka = KafkaPipeline()
