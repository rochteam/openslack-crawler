# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
import pymongo, datetime
from .celery import app


@app.task
def mongo_pipeline(data):
    for i in data:
        i.update({'created_at': datetime.datetime.utcnow()})
    mcollection.insert_many(data)
