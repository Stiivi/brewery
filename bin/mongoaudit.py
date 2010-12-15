import argparse
import pymongo
import json
import bson
from sets import Set
import brewery


connection = None
db = None
collection = None
key_stats = {}
distinct_count_threshold = 20

    
def set_collection(database_name, collection_name):
    global db
    global collection
    db = connection[database_name]
    collection = db[collection_name]
    
def audit(database, collection, connection = None):
    if not connection:
        connection = pymongo.Connection("localhost", 27017)
    elif issubclass(connection, dict):
        host = connection.get("host", "localhost")
        port = connection.get("port", 27017)
        connection = pymongo.Connection(host, port)
        
    _database = connection[database]
    _collection = _database[collection]

    keys = Set()
    count = 0
    for record in _collection.find():
        for key, value in record.items():
            count += 1
            probe(key, value)

    result = {}
    for key, stat in key_stats.items():
        stat.record_count = count
        stat.finalize()
        result[key] = stat.to_dict()

    return result


def probe(key, value):
    # print "Probe key: '%s' value: '%s' storage: '%s'" % (key, value, value.__class__)
    global key_stats
    if not key in key_stats:
        stat = brewery.dq.FieldStatistics(key)
        key_stats[key] = stat
    else:
        stat = key_stats[key]
    stat.probe(value)
    
result = audit('wdmmg', 'classifier')
print result
