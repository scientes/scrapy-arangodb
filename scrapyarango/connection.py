# -*- coding: utf-8 -*-
__author__ = 'scientes'

try:
    from pyArango.connection import *
    import pyArango.theExceptions
except ImportError:
    raise ImportError("Please install pyarango before running scrapyarango.")


PYARANGO_DATABASE = '%(spider)spyarango'
PYARANGO_REQUESTS_TABLE= 'http_queue'

# state codes
SCHEDULED = 1
DOWNLOADING = 2
DOWNLOADED = 3

connections = {}

def from_crawler(crawler):
    """ Factory method that returns an instance of connection
        from crawler

        :return: Connection object
    """
    print("init connection")
    spider = crawler.spider
    print("spider")
    settings = crawler.settings
    host=settings.get('ARANGO_HOST', "localhost")
    port=int(settings.get('ARANGO_PORT', 8529))
    user=settings.get('ARANGO_USERNAME', None)
    password=settings.get('ARANGO_PASSWORD', None)
    pyarango_database = settings.get('ARANGO_DATABASE', PYARANGO_DATABASE)
    pyarango_database = pyarango_database % {'spider': spider.name}
    print("spider")
    table = settings.get('SCHEDULER_QUEUE_TABLE', PYARANGO_REQUESTS_TABLE)
    table = table % {'spider': spider.name}
    print("spider")

    global connections

    if pyarango_database not in connections:
        conn =Connection(username=user, password=password)
        if conn.hasDatabase(pyarango_database):
            db = conn[pyarango_database]
        else:
            db = conn.createDatabase(name=pyarango_database)
        connections[pyarango_database] =(conn,db)
        if not db.hasCollection(table):
            db.createCollection(name=table)

    print("last")
    return connections[pyarango_database]


def from_settings(settings):
    """ Factory method that returns an instance of connection
        from settings

        :return: Connection object
    """

    host=settings.get('ARANGO_HOST', "localhost")
    port=int(settings.get('ARANGO_PORT', 8529))
    user=settings.get('ARANGO_USERNAME', None)
    password=settings.get('ARANGO_PASSWORD', None)
    pyarango_database = settings.get('ARANGO_DATABASE', PYARANGO_DATABASE)

    global connections

    if pyarango_database not in connections:
        conn =Connection(username=user, password=password)
        db = conn.createDatabase(name=pyarango_database)
        connections[pyarango_database] =conn

    return connections[pyarango_database]
