import logging as log


def warn(msg):
    log.warn(str(msg), level=log.WARNING)


def info(msg):
    log.info(str(msg), level=log.INFO)


def debug(msg):
    log.debug(str(msg), level=log.DEBUG)
