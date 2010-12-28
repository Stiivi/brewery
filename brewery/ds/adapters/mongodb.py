"""MongoDB Datastore Adapter
"""
from __future__ import absolute_import
import brewery
import pymongo

def datastore(description):
    ds = MongoDBDatastore(description)
    return ds

class MongoDBDatastore(brewery.ds.Datastore):
    def __init__(self, description):
        
        if not options:
            options = {}
        
        self.connection = pymongo.connection.Connection(**description)

    @property
    def adapter_name(self):
        return "sqlalchemy"
        
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
        
    @property
    def field_names(self):
        raise NotImplementedError
        
    def rows(self):
        return MongoDBCursorWrapper(self, self.collection.find())
        
    def records(self):
        return self.collection.find()

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
        