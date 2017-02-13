# encoding=utf8
from celery import Celery

app = Celery('crawler', include=['tasks', "tasks1", "tasks2"])

app.config_from_object('settings')

if __name__ == '__main__':
    app.start()
