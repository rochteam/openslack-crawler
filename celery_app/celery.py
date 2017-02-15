# encoding=utf8
from __future__ import absolute_import, unicode_literals
from celery import Celery

app = Celery('celery_app', include=["celery_app.download"])

app.config_from_object('celery_app.settings')


if __name__ == '__main__':
    app.start()
