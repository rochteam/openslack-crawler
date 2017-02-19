# 大数据搜索
基于scrapy的采集系统，采用reids、kafka、celery、rabbitmq、elasticsearch、mysql、django等技术
打造新生代的分布式数据搜索和数据基础平台

# scrapy cluster

* URL地址的状态、爬虫的状态，保存在redis
* 下载和存储等任务交给celery来执行
* 由celery heartbeat来提交爬虫任务

# celery要做的事情

* 存储支持 elasticsearch、mongodb、mysql、solr等nosql数据库
* 下载文件支持保存到 disk,mongodb,fastdfs
* celery heartbeat, 定时发布任务到celery，可以支持分布式处理
* 格式规整

# mesos+marathon

可以基于docker、mesos、marathon等构建云采集


# run celery work
```
celery -A main worker -l info
celery -A main beat -l info -s /var/run/celery/beat-schedule --detach
```。
