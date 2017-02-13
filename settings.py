# encoding:utf-8
import os

# import eventlet
# eventlet.monkey_patch()

DEBUG = True
SITE_ROOT = os.path.dirname(os.path.abspath(__file__))

CACHE_BACKEND = 'redis_cache.cache.RedisCache'
CACHE_LOCATION = '127.0.0.1:6379:0'
REDIS_PSSWD = "admin"

ALLOWED_HOSTS = ['*']

SOURCE_REDIS_KEY = "crawler_source_list"

ALERT_RULE_REDIS_KEY = "crawler_alert_rule_hset"

SYS_SETTINGS = "crawler_sys_settings"

GRAPH_REDIS_KEY = "crawler_graph_hset"

COLLECT_LOG_KEY = "crawler_collect_log"

GRAPHITE_HOST = "127.0.0.1"
CARBON_HOST = '127.0.0.1'
GRAPHITE_PORT = 2013

USER_AGENT = "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:2.0) Gecko/20100101 Firefox/4.0"

# CELERY_IMPORTS = ("service.tasks", )
CELERY_ACKS_LATE = True
# CELERY_ALWAYS_EAGER=True#主动delay
# using serializer name
CELERY_RESULT_BACKEND = 'djcelery.backends.cache:CacheBackend'  #
CELERY_TASK_RESULT_EXPIRES = 7200  # 3600秒
CELERY_RESULT_PERSISTENT = True
CELERY_DISABLE_RATE_LIMITS = False
CELERY_IGNORE_RESULT = True
CELERY_RESULT_EXCHANGE_TYPE = "topic"
CELERY_RESULT_EXCHANGE = "crawler.collect.topic"
CELERY_QUEUE_HA_POLICY = 'all'
CELERYBEAT_SCHEDULER = "djcelery.schedulers.DatabaseScheduler"
BROKER_URL = "amqp://admin:admin@127.0.0.1:5672/crawler"  #
#: Only add pickle to this list if your broker is secured
#: from unwanted access (see userguide/security.html)
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_DEFAULT_QUEUE = "crawler.collect.topic.queue"
CELERY_DEFAULT_EXCHANGE = "crawler.collect.topic"
CELERY_DEFAULT_EXCHANGE_TYPE = "topic"
CELERY_DEFAULT_ROUTING_KEY = "crawler.collect.topic.#"
CELERY_TIMEZONE = 'Asia/Shanghai'
CELERY_ENABLE_UTC = False
CELERY_ACCEPT_CONTENT = ['application/json']
CELERYD_MAX_TASKS_PER_CHILD = 1000
CELERYD_HIJACK_ROOT_LOGGER = False
CELERYD_PREFETCH_MULTIPLIER = 1
CELERY_RESULT_DB_SHORT_LIVED_SESSIONS = True
# CELERY_EAGER_PROPAGATES_EXCEPTIONS = False

from celery.schedules import crontab, timedelta

CELERYBEAT_SCHEDULE = {
    'add-every-30-seconds': {
        'task': 'crawler.collect.add',
        'schedule': timedelta(seconds=10),  # crontab(hour=12, minute=30),  #  day_of_week=1
        # 'expires': 60,
        'args': (16, 16)
    },
    'crawler-collect-schedule-second_data_hotel': {
        'task': 'crawler.collect.crawl',
        'schedule': timedelta(seconds=5),
        'args': ("database", "813_475")
    },
    'crawler-collect-schedule-second_data_flight': {
        'task': 'crawler.collect.crawl',
        'schedule': timedelta(seconds=5),
        'args': ("database", "812_474")
    },
    'crawler-collect-schedule-second_data_train': {
        'task': 'crawler.collect.crawl',
        'schedule': timedelta(seconds=5),
        'args': ("database", "814_476")
    }
}

CELERY_ANNOTATIONS = {
    # 'tasks.add': {'rate_limit': '10/m'}  #  每分钟限制执行10个任务
}
from kombu import Exchange, Queue

crawl_exchange = Exchange('crawler.collect.crawl', type='topic', durable=True, delivery_mode=2, auto_delete=False)
crawl_result_exchange = Exchange('crawler.collect.crawl.result', durable=True, type='topic', delivery_mode=2,
                                 auto_delete=False)
crawl_result_key = "crawler.collect.crawl.result."

CELERY_ROUTES = {
    'crawler.collect.add': {'exchange': 'crawler.collect.add', 'routing_key': 'crawler.collect.add'},
    'crawler.collect.schedule.delete.hotdata': {'exchange': 'crawler.collect.topic.queue', 'routing_key': 'crawler.collect.topic.queue'},
    'crawler.collect.crawl.recrawl': {'exchange': 'crawler.collect.crawl', 'routing_key': 'crawler.collect.crawl'},
    'crawler.collect.crawl.client': {'exchange': 'crawler.collect.crawl', 'routing_key': 'crawler.collect.crawl'},
    'crawler.collect.crawl.log': {'exchange': 'crawler.collect.crawl.log', 'routing_key': 'crawler.collect.crawl.log.alert'},
}

CELERY_QUEUES = [
    Queue("crawler.collect.topic.queue", exchange=Exchange('crawler.collect.topic.queue', type='direct'),
          routing_key="crawler.collect.topic.queue", durable=True, auto_delete=False),
    Queue("crawler.collect.add", exchange=Exchange('crawler.collect.add', type='direct'),
          routing_key="crawler.collect.add", durable=True, auto_delete=False),
    Queue("crawler.collect.crawl.log.alert", exchange=Exchange('crawler.collect.crawl.log', type='topic'),
          routing_key="crawler.collect.crawl.log.alert", durable=True, auto_delete=False),
]

keys = ["elasticsearch"]
for i in keys:
    CELERY_QUEUES.extend([
        Queue(crawl_result_key + "storage.graphite." + i, exchange=crawl_result_exchange,
              routing_key=crawl_result_key + "storage.*." + i, durable=True, auto_delete=False),  # 数据存入graphite队列
        Queue(crawl_result_key + "alert.rules." + i, exchange=crawl_result_exchange,
              routing_key=crawl_result_key + "alert.*." + i, durable=True, auto_delete=False),  # 数据告警crawler的rules队列
    ])
for i in range(0, 100):
    i = str(i)
    CELERY_QUEUES.extend([Queue(name="crawler.collect.crawl." + i, exchange=crawl_exchange, routing_key="crawler.collect.crawl." + i, durable=True, auto_delete=False)])

# Enables error emails.
CELERY_SEND_TASK_ERROR_EMAILS = False
