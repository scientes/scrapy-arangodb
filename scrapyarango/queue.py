from scrapy.utils.reqser import request_to_dict, request_from_dict
from scrapy.utils.request import request_fingerprint
import zlib,base64
from urllib.parse import urlparse
try:
    import cPickle as pickle
except ImportError:
    import pickle
try:
    from pyArango.connection import *
    import pyArango.theExceptions
except ImportError:
    raise ImportError("Please install pyarango before running scrapyarango.")


class Base(object):
    """Per-spider queue/stack base class
    """

    def __init__(self, conn, spider, table):
        """Initialize per-spider SQLite queue.

        Parameters:
            conn -- sqlite connection
            spider -- spider instance
            table -- table for this queue (e.g. "%(spider)s_queue")
        """
        print("init queue")
        self.conn = conn[0]
        self.db=conn[1]
        self.spider = spider
        table=table % {'spider': spider.name}
        if not self.db.hasCollection(table):
            self.table=db.createCollection(name=table)
        else:
            self.table=self.db.collections[table]
        self.table.ensurePersistentIndex(fields=["domain"],name="domain")
        self.table.ensurePersistentIndex(fields=["priority"],name="priority")


    def _encode_request(self, request):
        """Encode a request object"""
        dic=request_to_dict(request, self.spider)
        if dic.get("body"):
            dic["body"]=base64.urlsafe_b64encode(zlib.compress(request.body)).decode("utf-8")
        if dic.get("body")==b"":
            dic["body"]=None
        return dic
    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        if encoded_request.get("body") and encoded_request.get("body") is not None:
            encoded_request["body"]=zlib.decompress(base64.urlsafe_b64decode(encoded_request["body"].encode("utf-8")))
        return request_from_dict(encoded_request, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """Push a request"""
        raise NotImplementedError

    def pop(self, timeout=0):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear table"""
        self.table.delete()


class SpiderQueue(Base):
    """Per-spider FIFO queue"""

    def __len__(self):
        """Return the length of the queue"""
        return int(self.db.count())

    def push(self, request,slot):
        """Push a request"""
        request_dump = self._encode_request(request)
        fingerprint = request_fingerprint(request)
        # INSERT OR IGNORE acts as dupefilter, because column fingerprint is UNIQUE
        doc={
            "_key":fingerprint,
            "domain":slot,
            "priority":request.priority,
            "body": request_dump
        }
        try:
            self.table.createDocument(doc).save()
        except pyArango.theExceptions.CreationError:
            pass
        except Exception as e:
            print (doc)
            raise e

    def pop(self,domain):
        """Pop a request"""
        aql="WITH "+self.table.name+" FOR a IN "+self.table.name+" FILTER a.domain==@domain LIMIT 1 SORT a.priority DESC REMOVE { _key: a._key } IN "+self.table.name+" return a"
        result=self.db.AQLQuery(aql,bindVars={"domain":domain},rawResults=True)
        if result:
            result=result[0]
            if result.get("body"):
                return self._decode_request(result.get("body"))

__all__ = ['SpiderQueue']
