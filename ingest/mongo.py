from gevent import monkey
monkey.patch_all()
from pymongo import MongoClient, uri_parser
from datetime import datetime


class Mongo(object):
    def __init__(self, db_url, collection_name):
        client = MongoClient(db_url)
        parsed = uri_parser.parse_uri(db_url)
        if 'database' not in parsed:
            raise ValueError('database not in uri: {}', db_url)
        db = client[parsed['database']]
        self.c = db[collection_name]

    def insert(self, info):
        record = info.copy()
        record['modified'] = datetime.utcnow()
        record['created'] = datetime.utcnow()
        result = self.c.insert_one(record)
        return result.inserted_id

    def update(self, info):
        record = {'$set': info, '$currentDate': {'modified': True}}
        result = self.c.update_one({'complete_name': info['complete_name']}, record)
        return result.modified_count

    def exists(self, filename):
        return self.c.find_one({'complete_name': filename})

