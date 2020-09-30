__author__ = 'scientes'

import scrapyarango.connection as connection

from scrapy.utils.misc import load_object, create_instance
from scrapyarango.dupefilter import RFPDupeFilter
from scrapy.utils.request import request_fingerprint
from pyArango.connection import *
import pyArango.theExceptions
from time import time

try:
    import cPickle as pickle
except ImportError:
    import pickle

import logging
logger = logging.getLogger(__name__)

# default values
SCHEDULER_PERSIST = False
QUEUE_TABLE = 'http'
QUEUE_CLASS = 'scrapyarango.queue.SpiderQueue'
IDLE_BEFORE_CLOSE = 0

class DownloaderInterface:

    def __init__(self, crawler):
        self.downloader = crawler.engine.downloader

    def stats(self, possible_slots):
        return [(self._active_downloads(slot), slot)
                for slot in possible_slots]

    def get_slot_key(self, request):
        return str(self.downloader._get_slot_key(request, None))

    def _active_downloads(self, slot):
        """ Return a number of requests in a Downloader for a given slot """
        if slot not in self.downloader.slots:
            return 0
        return len(self.downloader.slots[slot].active)

class Scheduler:
    """ A arango Scheduler for Scrapy.
    """

    def __init__(self,crawler, conndict, persist, table, queue_cls, idle_before_close, stats,dupefilter, *args, **kwargs):
        if crawler.settings.getint('CONCURRENT_REQUESTS_PER_IP') != 0:
            raise ValueError('"%s" does not support CONCURRENT_REQUESTS_PER_IP'
                             % (self.__class__,))
        conn,db=conndict
        self.conn = conn
        self.db=db
        if not self.db.hasCollection(table):
            self.table=db.createCollection(name=table)
        else:
            self.table=self.db.collections[table]
        self.persist = persist
        self.idle_before_close = idle_before_close
        self.stats = stats
        self.queue_cls = queue_cls(conndict,crawler.spider,table)
        self._downloader_interface = DownloaderInterface(crawler)
        self.crawler = crawler
        self.slots=[]
        self.slotage=0
        self.dupefilter=dupefilter


    @classmethod
    def from_crawler(cls, crawler):

        settings = crawler.settings
        persist = settings.get('SCHEDULER_PERSIST', SCHEDULER_PERSIST)
        table = settings.get('SCHEDULER_QUEUE_TABLE', QUEUE_TABLE)
        table = table % {'spider': crawler.spider.name}

        queue_cls = load_object(settings.get('SCHEDULER_QUEUE_CLASS', QUEUE_CLASS))
        idle_before_close = settings.get('SCHEDULER_IDLE_BEFORE_CLOSE', IDLE_BEFORE_CLOSE)
        conn = connection.from_crawler(crawler)
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])

        dupefilter = create_instance(dupefilter_cls, settings, crawler)

        instance = cls(crawler,conn, persist, table, queue_cls, idle_before_close, crawler.stats,dupefilter,)

        return instance

    def open(self, spider):
        self.spider = spider

        if self.has_pending_requests():
            spider.log("Resuming crawl (%d requests scheduled)" % len(self))

    def close(self, reason):
        if not self.persist:
            self.table.truncate()

    def enqueue_request(self, request):
        if not request.dont_filter and self.dupefilter.request_seen(request):
            self.queue_cls.push(request,self._downloader_interface.get_slot_key(request))
            if self.stats:
                self.stats.inc_value('scheduler/enqueued/arango', spider=self.spider)
        else:
            self.queue_cls.push(request,self._downloader_interface.get_slot_key(request))
            if self.stats:
                self.stats.inc_value('scheduler/enqueued/arango', spider=self.spider)



    def next_request(self):
        if self.slotage>int(time())+10 or not self.slots:
            self.slotage=int(time())+10
            self.slots=self.get_all_slots()
        stats=self._downloader_interface.stats(self.slots)
        if not stats:
            return
        request=self.queue_cls.pop(min(stats)[1])
        if request:
            self.stats.inc_value('scheduler/dequeued/arango', spider=self.spider)
            return request


    def has_pending_requests(self):
        return len(self) is not None

    def __len__(self):
        return self.table.count()

    def get_all_slots(self):
        aql="FOR a in "+self.table.name+" RETURN DISTINCT a.domain"
        return self.db.AQLQuery(aql,rawResults=True)
