import pymongo
import json
import bson
from sets import Set

connection = None
db = None
collection = None
key_stats = {}
distinct_count_threshold = 20

class KeyStat(object):
    def __init__(self, key):
        self.key = key
        self.value_count = 0
        self.record_count = 0
        self.value_ratio = 0
        
        self.distinct_values = Set()
        self.distinct_overflow = False
        self.storage_types = Set()

        self.null_count = 0
        self.null_value_ratio = 0
        self.null_record_ratio = 0
        self.empty_string_count = 0
        
    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        self.value_count += 1
        
        if value == None:
            self.null_count += 1
        if value == '':
            self.empty_string_count += 1

        self.probe_distinct(value)

    def probe_distinct(self, value):
        """Find distinct values, if theyr count is less than required threshold. If there are
        more distinct values than the threshold, then distinct_overflow flag is set and
        list of distinct values will be empty"""
        if self.distinct_overflow:
            return

        # We are not testing lists, dictionaries and object IDs
        storage_type = value.__class__
        if issubclass(storage_type, dict) \
                    or issubclass(storage_type, bson.objectid.ObjectId) \
                    or issubclass(storage_type, list):
            return

        if len(self.distinct_values) < distinct_count_threshold:
            self.distinct_values.add(value)
        else:
            self.distinct_overflow = True
    def finalize(self):
        self.value_ratio = self.value_count / self.record_count
        self.null_value_ratio = self.null_count / self.value_count
        self.null_record_ratio = self.null_count / self.record_count
        
    def to_dict(self):
        d = {}
        d["key"]= self.key
        d["value_count"]= self.value_count
        d["value_ratio"]= self.value_ratio
        d["record_count"]= self.record_count
        if self.distinct_overflow:
            d["distinct_overflow"] = self.distinct_overflow,
            d["distinct_values"] = []
        else:
            d["distinct_values"] = list(self.distinct_values)

        d["storage_types"]= list(self.storage_types)

        d["null_count"] = self.null_count
        d["null_value_ratio"] = self.null_value_ratio
        d["null_record_ratio"] = self.null_record_ratio
        d["empty_string_count"] = self.empty_string_count

        return d
    
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
        stat = KeyStat(key)
        key_stats[key] = stat
    else:
        stat = key_stats[key]
    stat.probe(value)
    
result = audit('wdmmg', 'classifier')
print result
