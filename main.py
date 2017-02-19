# encoding=utf8
from celery import Celery

app = Celery('tasks', include=["tasks.pipeline"])

app.config_from_object('tasks.settings')


if __name__ == '__main__':
    app.start()
