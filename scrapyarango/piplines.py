

from pyArango.connection import *
import pyArango.theExceptions
class arangoPipeline:


    def open_spider(self, spider):
        self.client =Connection(arangoURL="http://localhost:8529",username="root", password="password")

        self.db = self.client["test_db"]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        if item.collection is not None:
            try:
                self.db[item.collection].createDocument(item['return_dict']).save()
            except pyArango.theExceptions.CreationError as e:
                pass

        #return Item
