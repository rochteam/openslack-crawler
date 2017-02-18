# encoding=utf8
from celery import Celery

app = Celery('celery_app', include=["celery_app.tasks"])

app.config_from_object('celery_app.settings')


if __name__ == '__main__':
    app.start()
