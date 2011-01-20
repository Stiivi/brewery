import base
import brewery.dq
try:
    import pymongo
except: 
    pass


class MongoDBDataSource(base.DataSource):
    """docstring for ClassName
    """
    def __init__(self, collection, database = None, host = None, port = None,
                 expand = False, **mongo_args):
        """Creates a MongoDB data source stream.
        
        :Attributes:
            * collection: mongo collection name
            * database: database name
            * host: mongo database server host, default is ``localhost``
            * port: mongo port, default is ``27017``
            * expand: expand dictionary values and treat children as top-level keys with dot '.'
                separated key path to the child..
        """
        self.collection_name = collection
        self.database_name = database
        self.host = host
        self.port = port
        self.mongo_args = mongo_args
        self.expand = expand

        self.collection = None
        self._fields = None
        
    def initialize(self):
        """Initialize Mongo source stream:
        """

        args = self.mongo_args.copy()
        if self.host:
            args["host"] = self.host
        if self.port:
            args["port"] = self.port

        self.connection = pymongo.connection.Connection(**args)
        self.database = self.connection[self.database_name]
        self.collection = self.database[self.collection_name]


    def read_fields(self, limit = 0):
        keys = []
        probes = {}

        def probe_record(record, parent = None):
            for key, value in record.items():
                if parent:
                    full_key = parent + "." + key
                else:
                    full_key = key

                if self.expand and type(value) == dict:
                    probe_record(value, full_key)
                    continue
                    
                if not full_key in probes:
                    probe = brewery.dq.FieldTypeProbe(full_key)
                    probes[full_key] = probe
                    keys.append(full_key)
                else:
                    probe = probes[full_key]
                probe.probe(value)

        for record in self.collection.find(limit = limit):
            probe_record(record)

        fields = []

        for key in keys:
            probe = probes[key]
            field = base.Field(probe.field)

            storage_type = probe.unique_storage_type
            if not storage_type:
                field.storage_type = "unknown"
            elif storage_type == "unicode":
                field.storage_type = "string"
            else:
                field.storage_type = "unknown"
                field.concrete_storage_type = storage_type
                
            # FIXME: Set analytical type

            fields.append(field)

        self._fields = list(fields)
        return self._fields
        
    def rows(self):
        if not self.collection:
            raise RuntimeError("Stream is not initialized")
        fields = self.field_names
        iterator = self.collection.find(fields = fields)
        return MongoDBRowIterator(iterator, fields)

    def records(self):
        if not self.collection:
            raise RuntimeError("Stream is not initialized")
        # return MongoDBRowIterator(self.field_names, self.collection.find())
        if self._fields:
            fields = self.field_names
        else:
            fields = None
        iterator = self.collection.find(fields = fields)
        return MongoDBRecordIterator(iterator, self.expand)

class MongoDBRowIterator(object):
    """Wrapper for pymongo.cursor.Cursor to be able to return rows() as tuples and records() as 
    dictionaries"""
    def __init__(self, cursor, field_names):
        self.cursor = cursor
        self.field_names = field_names

    def __getitem__(self, index):
        record = self.cursor.__getitem__(index)

        array = []

        for field in self.field_names:
            value = record
            for key in field.split('.'):
                if key in value:
                    value = value[key]
                else:
                    break
            array.append(value)

        return tuple(array)

class MongoDBRecordIterator(object):
    """Wrapper for pymongo.cursor.Cursor to be able to return rows() as tuples and records() as 
    dictionaries"""
    def __init__(self, cursor, expand = False):
        self.cursor = cursor
        self.expand = expand

    def __getitem__(self, index):
        def expand_record(record, parent = None):
            ret = {}
            for key, value in record.items():
                if parent:
                    full_key = parent + "." + key
                else:
                    full_key = key

                if type(value) == dict:
                    expanded = expand_record(value, full_key)
                    ret.update(expanded)
                else:
                    ret[full_key] = value
            return ret

        record = self.cursor.__getitem__(index)
        if not self.expand:
            return record
        else:
            return expand_record(record)

class MongoDBDataTarget(base.DataTarget):
    """docstring for ClassName
    """
    def __init__(self, collection, database = None, host = None, port = None,
                 truncate = False, expand = False, **mongo_args):
        """Creates a MongoDB data target stream.

        :Attributes:
            * collection: mongo collection name
            * database: database name
            * host: mongo database server host, default is ``localhost``
            * port: mongo port, default is ``27017``
            * expand: expand dictionary values and treat children as top-level keys with dot '.'
                separated key path to the child..
            * truncate: delete existing data in the collection. Default: False
        """
        self.collection_name = collection
        self.database_name = database
        self.host = host
        self.port = port
        self.mongo_args = mongo_args
        self.expand = expand
        self.truncate = truncate

        self.collection = None
        self._fields = None

    def initialize(self):
        """Initialize Mongo source stream:
        """

        args = self.mongo_args.copy()
        if self.host:
            args["host"] = self.host
        if self.port:
            args["port"] = self.port

        self.connection = pymongo.connection.Connection(**args)
        self.database = self.connection[self.database_name]
        self.collection = self.database[self.collection_name]
        
        if self.truncate:
            self.collection.remove()

    def append(self, obj):
        if type(obj) == dict:
            record = obj
        else:
            record = dict(zip(self.field_names, obj))

        if self.expand:
            record = expand_record(record)
            
        self.collection.insert(record)
        