"""
arangodb Cache Storage
A arangodb backend for HTTP cache storage. It stores responses using GridFS.
To use it, set the following Scrapy setting in your project:
    HTTPCACHE_STORAGE = 'scrapy_arangodb.httpcache.ArangoCacheStorage'
"""
import os
from time import time

from scrapy.responsetypes import responsetypes
from scrapy.exceptions import NotConfigured
from scrapy.utils.request import request_fingerprint
from scrapy.http import Headers
from scrapy.utils.gz import gunzip
import base64
import zlib


try:
    from pyArango.connection import *
    import pyArango.theExceptions
except ImportError:
    Connection=None

def get_database(settings):
    """Return Arango database based on the given settings, also pulling the
    ARANGO host, port and database form the environment variables if they're not
    defined in the settings..
    If user and password parameters are specified and also passed in a
    ARANGOdb URI, the call to authenticate() later (probably) overrides the URI
    string's earlier login.
    """

    conf = {
        'host': settings['HTTPCACHE_ARANGO_HOST'] \
                or settings['ARANGO_HOST'] \
                or os.environ.get('ARANGO_HOST')
                or 'localhost',
        'port': settings.getint('HTTPCACHE_ARANGO_PORT') \
                or settings.getint('ARANGO_PORT') \
                or int(os.environ.get('ARANGO_PORT', '8529')) \
                or 8529,
        'db': settings['HTTPCACHE_ARANGO_DATABASE'] \
                or settings['ARANGO_DATABASE'] \
                or os.environ.get('ARANGO_DATABASE') \
                or settings['BOT_NAME'],
        'user': settings['HTTPCACHE_ARANGO_USERNAME'] \
                or settings['ARANGO_USERNAME'] \
                or os.environ.get('ARANGO_USERNAME'),
        'password': settings['HTTPCACHE_ARANGO_PASSWORD'] \
                or settings['ARANGO_PASSWORD'] \
                or os.environ.get('ARANGO_PASSWORD'),
    }
    # Support passing any other options to ARANGOClient;
    # options passed as "positional arguments" take precedence
    kwargs = settings.getdict('HTTPCACHE_ARANGO_CONFIG', None) \
                or settings.getdict('ARANGO_CONFIG')
    conf.update(kwargs)
    return conf


class ArangoCacheStorage(object):
    """Storage backend for Scrapy HTTP cache, which stores responses in ARANGODB
    GridFS.
    If HTTPCACHE_SHARDED is True, a different collection will be used for
    each spider, similar to FilesystemCacheStorage using folders per spider.
    """

    def __init__(self, settings, **kw):
        if Connection is None:
            raise NotConfigured('%s is missing pyarango module.' %
                                self.__class__.__name__)

        self.expire = settings.getint('HTTPCACHE_EXPIRATION_SECS')
        self.sharded = settings.getbool('HTTPCACHE_SHARDED', False)
        kwargs = get_database(settings)
        kwargs.update(kw)
        db = kwargs.pop('db')
        port=kwargs.pop("port")
        host=kwargs.pop("host")
        user = kwargs.pop('user', None)
        password = kwargs.pop('password', None)
        self.client = Connection(arangoURL="http://"+str(host)+":"+str(port),username=user,password=password)
        self.db = self.client[db]
        self.fs = {}


    def open_spider(self, spider):
        _shard = 'httpcache'
        if self.sharded:
            _shard = 'httpcache.%s' % spider.name
        if not self.db.hasCollection(_shard):
            print("create")
            self.fs[spider] = self.db.createCollection('Collection', name=_shard)
        else:
            self.fs[spider] = self.db.collections[_shard]

    def close_spider(self, spider):
        del self.fs[spider]

    def __del__(self):
        pass

    def retrieve_response(self, spider, request):
        key = self._request_key(spider, request)
        try:
            gf = self.fs[spider].fetchDocument(key,rawResults=True)
            print("found\t"+str(request.url),request.priority)
        except pyArango.theExceptions.DocumentNotFoundError:
            print("not found\t"+str(request.url),request.priority)
            return
        url = str(gf["url"])
        status = str(gf["status"])

        bod=gf["body"].encode("utf-8")
        body = zlib.decompress(base64.urlsafe_b64decode(bod))
        try:
            if gf["headers"].get("content-encoding")=="gzip" and not gzip_magic_number(body):
                del gf["headers"]["content-encoding"]
            elif not gf["headers"].get("content-encoding") and gzip_magic_number(body):
                gf["headers"]["content-encoding"]="gzip"
            headers = Headers([(x, str(y)) for x, y in gf["headers"].items()])
        except AttributeError as e:
            if gzip_magic_number(body):
                #print("added headers")
                headers=Headers((("Content-Encoding","gzip"),))
            else:
                #print("headers=None")
                headers=None


        respcls = responsetypes.from_args(headers=headers, url=url,body=body)

        response = respcls(url=url, headers=headers, status=status, body=body)
        return response



    def store_response(self, spider, request, response):
        key = self._request_key(spider, request)
        metadata = {
            '_key': str(key),
            'time': int(time()),
            'status': int(response.status),
            'url': response.url,
            'headers': response.headers.to_unicode_dict(),
            'body': base64.urlsafe_b64encode(zlib.compress(response.body)).decode("utf-8"),
            'found': 1
        }
        #print(type(metadata["body"]))
        try:
            print("store",metadata["url"])
            doc=self.fs[spider].createDocument(metadata)
            doc.save()
        except pyArango.theExceptions.CreationError:
            print("update",metadata["url"])
            document=self.fs[spider].fetchDocument(key)
            document.set(metadata)
            document.patch()


    def _request_key(self, spider, request):
        rfp = request_fingerprint(request)
        # We could disable the namespacing in sharded mode (old behaviour),
        # but keeping it allows us to merge collections later without
        # worrying about key conflicts.
        #if self.sharded:
        #    return rfp
        return rfp
def gzip_magic_number(body):
    return body[:3] == b'\x1f\x8b\x08'
