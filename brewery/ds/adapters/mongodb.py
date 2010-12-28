"""MongoDB Datastore Adapter
"""
from __future__ import absolute_import
import brewery
import pymongo

allowed_connection_keys = ["host", "port" , "pool_size", 
                           "auto_start_request", "timeout", "slave_okay",
                           "network_timeout", "document_class", "tz_aware"]
def datastore(description):
    ds = MongoDBDatastore(description)
    return ds

class MongoDBDatastore(brewery.ds.Datastore):
    def __init__(self, description):
        kargs = {}
        
        if "database" not in description:
            raise ValueError("Database not specified for MongoDB adapter")
        self.database_name = description["database"]
        
        # Pass only allowed keys
        for key, value in description.items():
            if key in allowed_connection_keys:
                kargs[key] = value
        
        self.connection = pymongo.connection.Connection(**kargs)
        self.database = self.connection[self.database_name]
        
    @property
    def adapter_name(self):
        return "mongodb"
        
    def dataset(self, name):
        collection = self.database[name]
        dataset = MongoDBDataset(collection)
        return dataset
        
    def has_dataset(self, name):
        return name in self.dataset_names

    @property
    def dataset_names(self):
        return self.database.collection_names()

class MongoDBDataset(object):
    """docstring for MongoDBDataset"""
    def __init__(self, collection):
        super(MongoDBDataset, self).__init__()
        self.collection = collection
        self.fields = None
    
    @property
    def field_names(self):
        names = [field.name for field in self.fields]
        return names
        
    def rows(self):
        return MongoDBCursorWrapper(self, self.collection.find())
        
    def records(self):
        return self.collection.find()
        
    def read_fields(self, limit = 0):
        keys = []
        key_stats = {}
        for record in self.collection.find(limit = limit):
            for key, value in record.items():
                if not key in key_stats:
                    stat = brewery.dq.FieldStatistics(key, distinct_threshold = 1)
                    # Ignore distinct values
                    # FIXME: disable distinct probe
                    stat.distinct_overflow = True
                    key_stats[key] = stat
                    keys.append(key)
                else:
                    stat = key_stats[key]
                stat.probe(value)

        fields = []

        for key in keys:
            stat = key_stats[key]
            stat.finalize()
            field = brewery.Field(stat.field)

            storage_type = stat.unique_storage_type
            if not storage_type:
                field.storage_type = "unknown"
            elif storage_type == "unicode":
                field.storage_type = "string"
            else:
                field.storage_type = "unknown"
            # FIXME: Set analytical type

            fields.append(field)

        self.fields = fields
        return list(fields)

class MongoDBCursorWrapper(object):
    """Wrapper for pymongo.cursor.Cursor to be able to return rows() as tuples and records() as 
    dictionaries"""
    def __init__(self, dataset, cursor):
        self.dataset = dataset
        self.cursor = cursor
        self.fields = dataset.field_names

    def __getitem__(index):
        record = cursor.__getitem__(index)
        array = []
        for field in self.fields:
            array.append(record.get(field))
        return tuple(array)
        
    def count():
        return cursor.count()
        