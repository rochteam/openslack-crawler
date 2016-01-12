import datetime as dt

from kafka.client import KafkaClient
from kafka.producer import SimpleProducer
import traceback
import json


class KafkaPipeline(object):
    """Pushes serialized item to appropriate Kafka topics."""

    def __init__(self, producer, topic_prefix, aKafka):
        self.producer = producer
        self.topic_prefix = topic_prefix
        self.topic_list = []
        self.kafka = aKafka

    @classmethod
    def from_settings(cls, settings):
        kafka = KafkaClient(settings['KAFKA_HOSTS'])
        producer = SimpleProducer(kafka)
        topic_prefix = settings['KAFKA_TOPIC_PREFIX']
        return cls(producer, topic_prefix, kafka)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def process_item(self, item, spider):
        datum = dict(item)
        datum["timestamp"] = dt.datetime.utcnow().isoformat()
        prefix = self.topic_prefix
        appid_topic = "{prefix}.crawled_{spider}".format(prefix=prefix, spider=spider.name)
        self.checkTopic(appid_topic)
        try:
            message = json.dumps(datum)
            self.producer.send_messages(appid_topic, message)
        except Exception,e:
            traceback.print_exc()
            message = 'json failed to parse'
        return None

    def checkTopic(self, topicName):
        if topicName not in self.topic_list:
            self.kafka.ensure_topic_exists(topicName)
            self.topic_list.append(topicName)
