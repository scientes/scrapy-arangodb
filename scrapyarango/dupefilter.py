from time import time
import scrapyarango.connection as connection
try:
    import pyArango.theExceptions
except ImportError:
    raise ImportError("Please install pyarango before running scrapyarango.")
from scrapy.dupefilters import BaseDupeFilter
from scrapy.utils.request import request_fingerprint


class RFPDupeFilter(BaseDupeFilter):
    """SQLite-based request duplication filter"""

    def __init__(self, conn, table,db):
        """Initialize duplication filter

        Parameters
        ----------
        conn : arango connection
        table : str
            Where to store fingerprints
        """
        print("init dupefilter")
        self.conn = conn
        self.table = table
        self.db=db

    @classmethod
    def from_crawler(cls, crawler):
        # create one-time table. needed to support to use this
        # class as standalone dupefilter with scrapy's default scheduler
        # if scrapy passes spider on open() method this wouldn't be needed
        conn,db = connection.from_crawler(crawler)
        table = "dupefilter"
        if not db.hasCollection(table):
            table=db.createCollection(name=table)
        else:
            table=db.collections[table]

        return cls(conn, table,db)

    def request_seen(self, request):
        fp = request_fingerprint(request)
        try:
            self.table.fetchDocument(fp,rawResults=True)
            return True
        except pyArango.theExceptions.DocumentNotFoundError:
            try:
                self.table.createDocument({"_key":fp})
            except pyArango.theExceptions.CreationError:
                return True
            return False


    def close(self, reason):
        """Delete data on close. Called by scrapy's scheduler"""
        self.clear()

    def clear(self):
        """Clears fingerprints data"""
        self.table.delete()
