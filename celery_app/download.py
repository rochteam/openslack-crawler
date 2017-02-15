# -*- coding: utf-8 -*-

from __future__ import absolute_import, unicode_literals
from .celery import app

@app.task
def image_pipeline(url):
    _spider.download(url)
