"""Data stores, data sets and data sources
"""

# Data sources
# ============
# 
# Should implement:
# * fields
# * prepare()
# * rows() - returns iterable with value tuples
# * records() - returns iterable with dictionaries of key-value pairs
# 
# Data targets
# ============
# Should implement:
# * fields
# * prepare()
# * append(object) - appends object as row or record depending whether it is a dictionary or a list
# Optional (for performance):
# * append_row(row) - row is tuple of values, raises exception if there are more values than fields
# * append_record(record) - record is a dictionary, raises exception if dict key is not in field list

import sys
import brewery

# from brewery.ds.csv_data_source import *
# from brewery.ds.rdb_data_source import *

datastore_dictionary = {}
datastore_adapters = {}

def datastore(description):
    """Opens a datastore and returns datastore instance. If the datastore is relational database, 
    connection is created. The datastore description dictionary should contain adapter
    specific connection information.

    Args:
    	description: datastore description either as string or as dictionary. If string is used, then
    	    default datastores with given name are searched for description dictionary.

    Returns:
    	Datastore object.
    """
    if type(description) == str:
        obj = datastore_dictionary.get(description)
        if not obj:
            raise KeyError("No datastore with name '%s'" % obj)
        description = obj

    if "adapter" not in description:
        raise ValueError("No adapter provided for datastore.")

    adapter_name = description["adapter"]

    adapter = __datastore_adapter(adapter_name)

    adapter_desc = description.copy()
    del adapter_desc["adapter"]

    return adapter.datastore(adapter_desc)

def __datastore_adapter(adapter_name):
    global datastore_adapters
    if adapter_name in datastore_adapters:
    	adapter = datastore_adapters[adapter_name]
    else:
        module_name = "brewery.ds.adapters." + adapter_name
        try:
            __import__(module_name)
        except ImportError:
            raise KeyError("Adapter '%s' not found" % adapter_name)

        adapter = sys.modules[module_name]
        datastore_adapters[adapter_name] = adapter
    return adapter

def split_table_schema(table_name):
    """Get schema and table name from table reference.
    
    Retunrs: Tuple in form (schema, table)
    """

    split = table_name.split('.')
    if len(split) > 1:
        return (split[0], split[1])
    else:
        return (None, split[0])
    
    
def fieldlist(fields):
    """Create a list of Field object from a list of strings, dictionaries or tuples
    
    How fields are consutrcuted:
    * string: field name, storage_type is unknown, analytical type is typeless
    * tuple: (field_name, storaget_type, analytical_type), the field name is obligatory, rest is optional
    * dict: contains keys for initializing
    
    
    """
    a_list = []
    for obj in fields:
        d = {}
        d["storage_type"] = "unknown"
        d["analytical_type"] = "typeless"

        if type(obj) == str:
            d["name"] = obj
        elif type(obj) == tuple:
            d["name"] = obj[0]
            if len(obj) > 1:
                d["storage_type"] = obj[1]
                if len(obj) > 2:
                    d["analytical_type"] = obj[2]
        elif type(obj) == dict:
            d["name"] = obj["name"]
            if "label" in obj:
                d["label"] = obj["label"]
            if "storage_type" in obj:
                d["storage_type"] = obj["storage_type"]
            if "analytical_type" in obj:
                d["analytical_type"] = obj["analytical_type"]
            if "adapter_storage_type" in obj:
                d["adapter_storage_type"] = obj["adapter_storage_type"]
        else:
            raise ValueError("Unknown type of field object '%s'" % obj)
        
        if "analytical_type" not in d:
            deftype = brewery.Field.default_analytical_types[d["storage_type"]]
            d["analytical_type"] = deftype
        
        a_list.append(brewery.Field(**d))
    return list(a_list)
        
        
class Datastore(object):
    """Object representing container such as relational database, document based collection, CSV
    file or directory with structured files
    
    Functionality of a datastore is provided by datastore adapter.
    
    Built-in adapters:
    
    +----------------+------------------------------------------------+-----------------------------+
    | Adapter        | Description                                    | Parameter keys              |
    +================+================================================+=============================+
    | sqlalchemy     | Many relational databases with SQL, based on   | ``url`` (sqlalchemy         |
    |                | sqlalchemy_                                    | connection URL)             |
    +----------------+------------------------------------------------+-----------------------------+
    | mongodb        | Document based database - MongoDB_             | ``host``, ``port``,         |
    |                |                                                | ``database``                |
    +----------------+------------------------------------------------+-----------------------------+
    
    .. _MongoDB: http://www.mongodb.org/
    .. _sqlalchemy: http://www.sqlalchemy.org/
    
    """
    def __init__(self, arg):
        super(Datastore, self).__init__()
        self.arg = arg

    @property
    def adapter_name(self):
        """Return name of adapter for datastore"""
        raise NotImplementedError()

    def dataset(self, name):
        """Get a dataset with name ``name``"""
        raise NotImplementedError()

    def __getitem__(self, key):
        """Shortcut for named dataset, see dataset()"""
        return self.dataset(key)

    @property
    def dataset_names(self):
        """Return list of dataset names"""
        raise NotImplementedError()

    def has_dataset(self, name):
        """Return True if dataset with given name exists"""
        return name in self.dataset_names

    def create_dataset(self, name, fields, replace = False):
        """Create a new dataset
        
        Arguments:
            * name: new dataset name
            * fields: list of Field objects
        """
        raise NotImplementedError()

    def destroy_dataset(self, name, checkfirst = False):
        """Destroy dataset in the receiving datastore.
        
        Arguments:
            * name: dataset name to be destroyed
            * checkfirst: if ``False`` and dataset does not exist an exception is raised. Set to ``True``
                if you want to destroy dataset whether it exists or not (``checkfirst = True`` is 
                equivalent to ``DROP TABLE IF EXISTS`` in SQL datastores)
        """

class Dataset(object):
    """Object representing a dataset in a datastore"""

    def truncate(self):
        """Remove all rows/records from the dataset."""
        raise NotImplementedError()
    
    def append(self, object):
        """Append an object into dataset. Object can be a tuple, array or a dict object. If tuple
        or array is used, then value position should correspond to field position in the field list,
        if dict is used, the keys should be valid field names."""
        raise NotImplementedError()

    @property
    def field_names(self):
        """Return names of fields in the dataset"""
        names = [column.name for column in self.table.columns]
        return names

    def read_fields(self, limit = None):
        """Read field descriptions from dataset. You should use this for datasets that do not provide
        metadata directly, such as CSV files or document bases databases. Does nothing for relational
        databases, as fields are represented by table columns and table metadata can obtained from
        database easily. 
        
        Note that this method can be quite costly, as by default all records within dataset are read
        and analysed.
        
        After executing this method, dataset ``fields`` is set to the newly read field list.
        
        Arguments:
            * limit: read only specified number of records from dataset to guess field properties
            
        Returns: tuple with Field objects. Order of fields is datastore adapter specific.
        """

    def rows(self):
        """Return iterable object with tuples."""
        return self.table.select().execute()
        
        