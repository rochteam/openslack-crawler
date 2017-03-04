# -*-coding:utf-8-*-

import pprint
from socket import socket
import time
import redis
import logging as log
# from scrapy.statscollectors import StatsCollector
from influxdb import InfluxDBClient
from crawler.utils import color


# default values
REDIS_HOST = 'localhost'
REDIS_PORT = 6379
STATS_KEY = 'scrapy:stats'


class RedisStatsCollector(object):
    """
        Save stats data in redis for distribute situation.
    """

    def __init__(self, crawler):
        self._dump = crawler.settings.getbool('STATS_DUMP')  # default: STATS_DUMP = True
        host = crawler.settings.get('REDIS_HOST', REDIS_HOST)
        port = crawler.settings.get('REDIS_PORT', REDIS_PORT)
        self.stats_key = crawler.settings.get('STATS_KEY', STATS_KEY)
        self.server = redis.Redis(host, port)

    def get_value(self, key, default=None, spider=None):
        if self.server.hexists(self.stats_key, key):
            return int(self.server.hget(self.stats_key, key))
        else:
            return default

    def get_stats(self, spider=None):
        return self.server.hgetall(self.stats_key)

    def set_value(self, key, value, spider=None):
        self.server.hset(self.stats_key, key, value)

    def set_stats(self, stats, spider=None):
        self.server.hmset(self.stats_key, stats)

    def inc_value(self, key, count=1, start=0, spider=None):
        if not self.server.hexists(self.stats_key, key):
            self.set_value(key, start)
        self.server.hincrby(self.stats_key, key, count)

    def max_value(self, key, value, spider=None):
        self.set_value(key, max(self.get_value(key, value), value))

    def min_value(self, key, value, spider=None):
        self.set_value(key, min(self.get_value(key, value), value))

    def clear_stats(self, spider=None):
        self.server.delete(self.stats_key)

    def open_spider(self, spider):
        pass

    def close_spider(self, spider, reason):
        if self._dump:
            log.info("Dumping Scrapy stats:\n" + pprint.pformat(self.get_stats()))
        self._persist_stats(self.get_stats(), spider)

    def _persist_stats(self, stats, spider):
        pass


class GraphiteStatsCollector(RedisStatsCollector):
    """
        send the stats data to graphite and save stats data in redis for distribute situation.

        The idea is from Julien Duponchelle,The url:https://github.com/noplay/scrapy-graphite

        How to use this:
            1.install graphite and configure it.For more infomation about graphite you can visit
        http://graphite.readthedocs.org/en/0.9.10/ and http://graphite.wikidot.com.
            2.edit /opt/graphite/webapp/content/js/composer_widgets.js,locate the ‘interval’
        variable inside toggleAutoRefresh function,Change its value from ’60′ to ’1′.
            3.add this in storage-aggregation.conf:
                [scrapy_min]
                pattern = ^scrapy\..*_min$
                xFilesFactor = 0.1
                aggregationMethod = min

                [scrapy_max]
                pattern = ^scrapy\..*_max$
                xFilesFactor = 0.1
                aggregationMethod = max

                [scrapy_sum]
                pattern = ^scrapy\..*_count$
                xFilesFactor = 0.1
                aggregationMethod = sum
            4.in settings set:
                STATS_CLASS = 'scrapygraphite.RedisGraphiteStatsCollector'
                GRAPHITE_HOST = '127.0.0.1'
                GRAPHITE_PORT = 2003

        The screenshot in woaidu_crawler/screenshots/graphite
    """

    GRAPHITE_HOST = '127.0.0.1'
    GRAPHITE_PORT = 2003
    GRAPHITE_IGNOREKEYS = []  # to ignore it,prevent to send data to graphite

    def __init__(self, crawler):
        super(GraphiteStatsCollector, self).__init__(crawler)
        host = crawler.settings.get("GRAPHITE_HOST", self.GRAPHITE_HOST)
        port = crawler.settings.get("GRAPHITE_PORT", self.GRAPHITE_PORT)
        self.ignore_keys = crawler.settings.get("GRAPHITE_IGNOREKEYS", self.GRAPHITE_IGNOREKEYS)
        self.style = color.color_style()
        self._sock = socket()
        self._sock.connect((host, port))

    def send(self, metric, value, timestamp=None):
        try:
            self._sock.send("%s %g %s\n\n" % (metric, value, timestamp or int(time())))
        except Exception as err:
            self.style.ERROR("SocketError(GraphiteClient): " + str(err))

    def _get_stats_key(self, spider, key):
        if spider is not None:
            return "scrapy.spider.%s.%s" % (spider.name, key)
        return "scrapy.%s" % (key)

    def set_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).set_value(key, value, spider)
        self._set_value(key, value, spider)

    def _set_value(self, key, value, spider):
        if isinstance(value, (int, float)) and key not in self.ignore_keys:
            k = self._get_stats_key(spider, key)
            self.send(k, value)

    def inc_value(self, key, count=1, start=0, spider=None):
        super(GraphiteStatsCollector, self).inc_value(key, count, start, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def max_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).max_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def min_value(self, key, value, spider=None):
        super(GraphiteStatsCollector, self).min_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def set_stats(self, stats, spider=None):
        super(GraphiteStatsCollector, self).set_stats(stats, spider)
        for key in stats:
            self._set_value(key, stats[key], spider)


class InfluxDBStatsCollector(RedisStatsCollector):
    INFLUX_HOST = '127.0.0.1'
    INFLUX_PORT = 2003
    INFLUX_DB = "scrapy"

    def __init__(self, crawler):
        super(InfluxDBStatsCollector, self).__init__(crawler)
        host = crawler.settings.get("INFLUX_HOST", self.INFLUX_HOST)
        port = crawler.settings.get("INFLUX_PORT", self.INFLUX_PORT)
        user = crawler.settings.get("INFLUX_USER", None)
        passwd = crawler.settings.get("INFLUX_PASSWORD", None)
        db = crawler.settings.get("INFLUX_DB", self.INFLUX_DB)
        self._client = InfluxDBClient(host, port, user, passwd, db, timeout=5)
        self.ignore_keys = []

    def _get_stats_key(self, spider, key):
        if spider is not None:
            return "scrapy.spider.%s.%s" % (spider.name, key)
        return "scrapy.%s" % (key)

    def send(self, measurement, value, timestamp=int(time.time()), tags={}):
        points = {
            "measurement": measurement,
            "tags": tags,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(int(timestamp))),
            "fields": {
                "value": value
            }
        }
        self._client.write_points(points)

    def set_value(self, key, value, spider=None):
        super(InfluxDBStatsCollector, self).set_value(key, value, spider)
        self._set_value(key, value, spider)

    def _set_value(self, key, value, spider):
        if isinstance(value, (int, float)) and key not in self.ignore_keys:
            k = self._get_stats_key(spider, key)
            self.send(k, value)

    def inc_value(self, key, count=1, start=0, spider=None):
        super(InfluxDBStatsCollector, self).inc_value(key, count, start, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def max_value(self, key, value, spider=None):
        super(InfluxDBStatsCollector, self).max_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def min_value(self, key, value, spider=None):
        super(InfluxDBStatsCollector, self).min_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def set_stats(self, stats, spider=None):
        super(InfluxDBStatsCollector, self).set_stats(stats, spider)
        for key in stats:
            self._set_value(key, stats[key], spider)


class StatsDStatsCollector(RedisStatsCollector):
    STATSD_HOST = '127.0.0.1'
    STATSD_PORT = 2003

    def __init__(self, crawler):
        super(StatsDStatsCollector, self).__init__(crawler)
        host = crawler.settings.get("GRAPHITE_HOST", self.GRAPHITE_HOST)
        port = crawler.settings.get("GRAPHITE_PORT", self.GRAPHITE_PORT)
        self.ignore_keys = crawler.settings.get("GRAPHITE_IGNOREKEYS", self.GRAPHITE_IGNOREKEYS)
        self._client = GraphiteClient(host, port)
        self.ignore_keys = []

    def _get_stats_key(self, spider, key):
        if spider is not None:
            return "scrapy.spider.%s.%s" % (spider.name, key)
        return "scrapy.%s" % (key)

    def send(self, measurement, value, timestamp=int(time.time()), tags=dict):
        points = {
            "measurement": measurement,
            "tags": tags,
            "time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.localtime(int(timestamp))),
            "fields": {
                "value": value
            }
        }
        self._client.write_points(points)

    def set_value(self, key, value, spider=None):
        super(StatsDStatsCollector, self).set_value(key, value, spider)
        self._set_value(key, value, spider)

    def _set_value(self, key, value, spider):
        if isinstance(value, (int, float)) and key not in self.ignore_keys:
            k = self._get_stats_key(spider, key)
            self.send(k, value)

    def inc_value(self, key, count=1, start=0, spider=None):
        super(StatsDStatsCollector, self).inc_value(key, count, start, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def max_value(self, key, value, spider=None):
        super(StatsDStatsCollector, self).max_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def min_value(self, key, value, spider=None):
        super(StatsDStatsCollector, self).min_value(key, value, spider)
        self.send(self._get_stats_key(spider, key), self.get_value(key))

    def set_stats(self, stats, spider=None):
        super(StatsDStatsCollector, self).set_stats(stats, spider)
        for key in stats:
            self._set_value(key, stats[key], spider)
