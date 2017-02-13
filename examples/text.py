import redis
from pymongo import MongoClient
r=redis.Redis()
# for url in redis.smembers("csdn:blog_user"):
# redis.sadd(csdn:blog_user",url)
mongodb = MongoClient()
stackoverflowdb = mongodb.stackoverflow
with open('/Users/lifeifei/Documents/1.l', 'r') as f:
    for line in f:
        if not r.sismember("csdn:blog_user",line):
            r.sadd("csdn:blog_user",line)