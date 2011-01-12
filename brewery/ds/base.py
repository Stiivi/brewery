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
import urllib2
import urlparse

datastore_dictionary = {}
datastore_adapters = {}

def datastore(description):
    """Opens a datastore and returns datastore instance. If the datastore is relational database, 
    connection is created. The datastore description dictionary should contain adapter
    specific connection information.

    :Args:
    	- description: datastore description either as string or as dictionary. If string is used,
    	then default datastores with given name are searched for description dictionary.

    :Returns:
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
    
def fieldlist(fields):
    """Create a list of :class:`Field` objects from a list of strings, dictionaries or tuples
    
    How fields are consutrcuted:
        * string: `field name` is set 
        * tuple: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is obligatory,
            rest is optional
        * dict: contains key-value pairs for initializing a :class:`Field` object
    
    For strings and in if not explicitly specified in a tuple or a dict case, then following rules
    apply:
        * `storage_type` is set to ``unknown``
        * `analytical_type` is set to ``typeless``
    """

    a_list = []
    for obj in fields:
        d = {}
        d["storage_type"] = "unknown"
        d["analytical_type"] = "typeless"

        if type(obj) == str or type(obj) == unicode:
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
            raise ValueError("Unknown object type ('%s' ) of field description object '%s'" \
                                % (type(obj), obj))
        
        if "analytical_type" not in d:
            deftype = Field.default_analytical_types[d["storage_type"]]
            d["analytical_type"] = deftype
        
        a_list.append(Field(**d))
    return list(a_list)
    
def open_resource(resource, mode = None):
    """Get file-like handle for a resource. Conversion:
    
    * if resource is a string and it is not URL or it is file:// URL, then opens a file
    * if resource is URL then opens urllib2 handle
    * otherwise assume that resource is a file-like handle
    
    Returns tuple: (handle, should_close) where `handle` is file-like object and `should_close` is
        a flag whether returned handle should be closed or not. Closed should be resources which
        where opened by this method, that is resources referenced by a string or URL.
    """
    handle = None
    should_close = False
    if type(resource) == str or type(resource) == unicode:
        parts = urlparse.urlparse(resource)
        if parts.scheme == '' or parts.scheme == 'file':
            if mode:
                handle = file(resource, mode)
                should_close = True
            else:
                handle = file(resource)
                should_close = True
        else:
            handle = urllib2.urlopen(resource)
            should_close = True
    else:
        handle = resource
    return (handle, should_close)
    
class Field(object):
    """Metadata - information about a field in a dataset or in a datastream.

    :Attributes:
        * `name` - field name
        * `label` - optional human readable field label
        * `storage_type` - Normalized data storage type. The data storage type is abstracted
        * `concrete_storage_type` (optional, recommended) - Data store/database dependent storage
            type - this is the real name of data type as used in a database where the field
            comes from or where the field is going to be created (this might be null if unknown)
        * `analytical_type` - data type used in data mining algorithms
        * `missing_values` (optional) - Array of values that represent missing values in the
            dataset for given field

    **Storage types:**
    
        * `string` - names, labels, short descriptions; mostly implemeted as ``VARCHAR`` type in 
            database, or can be found as CSV file fields
        * `text` - longer texts, long descriptions, articles
        * `integer` - discrete values
        * `float`
        * `boolean` - binary value, mostly implemented as small integer
        * `date`

    **Analytical types:**

        +-------------------+-------------------------------------------------------------+
        | Type              | Description                                                 |
        +===================+=============================================================+
        | `set`             | Values represent categories, like colors or contract .      |
        |                   | types. Fields of this type might be numbers which represent |
        |                   | for example group numbers, but have no mathematical         |
        |                   | interpretation. For example addition of group numbers 1+2   |
        |                   | has no meaning.                                             |
        +-------------------+-------------------------------------------------------------+
        | `ordered_set`     | Similar to `set` field type, but values can be ordered in a |
        |                   | meaningful order.                                           |
        +-------------------+-------------------------------------------------------------+
        | `discrete`        | Set of integers - values can be ordered and one can perform |
        |                   | arithmetic operations on them, such as:                     |
        |                   | 1 contract + 2 contracts = 3 contracts                      |
        +-------------------+-------------------------------------------------------------+
        | `flag`            | Special case of `set` type where values can be one of two   |
        |                   | types, such as 1 or 0, 'yes' or 'no', 'true' or 'false'.    |
        +-------------------+-------------------------------------------------------------+
        | `range`           | Numerical value, such as financial amount, temperature      |
        +-------------------+-------------------------------------------------------------+
        | `default`         | Analytical type is not explicitly set and default type for  |
        |                   | fields storage type is used. Refer to the table of default  |
        |                   | types.                                                      |
        +-------------------+-------------------------------------------------------------+
        | `typeless`        | Field has no analytical relevance.                          |
        +-------------------+-------------------------------------------------------------+

        Default analytical types:
            * `integer` is `discrete`
            * `float` is `range`
            * `unknown`, `string`, `text`, `date` are typeless
        
    """

    storage_types = ["unknown", "string", "text", "integer", "float", "boolean", "date"]
    analytical_types = ["default", "typeless", "flag", "discrete", "range", 
                        "set", "ordered_set"]

    default_analytical_type = {
                    "unknown": "typeless",
                    "string": "typeless",
                    "text": "typeless",
                    "integer": "discrete",
                    "float": "range",
                    "date": "typeless"
                }

    def __init__(self, name, label = None, storage_type = "unknown", analytical_type = None, 
                    concrete_storage_type = None, missing_values = None):
        self.name = name
        self.label = label
        self.storage_type = storage_type
        self.analytical_type = analytical_type
        self.concrete_storage_type = concrete_storage_type
        self.missing_values = missing_values

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        d = {}
        d["name"] = self.name
        d["label"] = self.label
        d["storage_type"] = self.storage_type
        d["analytical_type"] = self.analytical_type
        d["concrete_storage_type"] = self.concrete_storage_type
        d["missing_values"] = self.missing_values
        return "<%s(%s)>" % (self.__class__, d)
  
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
        
        :Arguments:
            * `name`: new dataset name
            * `fields`: tuple (or a list) of :class:`Field` objects. For better field type accuracy
                it is recommended that fields have `concrete_storage_type` set.
            * `replace`: call :meth:`Datastore.destroy_dataset` before creating.
        """
        raise NotImplementedError()

    def destroy_dataset(self, name, checkfirst = False):
        """Destroy dataset in the receiving datastore.
        
        :Arguments:
            - `name`: dataset name to be destroyed
            - `checkfirst`: if ``False`` and dataset does not exist an exception is raised. Set to ``True``
                if you want to destroy dataset whether it exists or not (``checkfirst = True`` is 
                equivalent to ``DROP TABLE IF EXISTS`` in SQL datastores)
        """

class DataStream(object):
    """Shared methods for data targets and data sources"""
    
    def initialize(self):
        """Delayed stream initialisation code. Subclasses might override this method to implement
        file or handle opening, connecting to a database, doing web authentication, ... By
        default this method does nothing.
        
        The method does not take any arguments, it expects pre-configured object.
        """
        pass

    def finalize(self):
        """Subclasses might put finalisation code here, for example:
        
        * closing a file stream
        * sending data over network
        * writing a chart image to a file
        
        Default implementation does nothing.
        """
        pass

    def get_fields(self):
        """Return stream field metadata: tuple of :class:`Field` objects representing fields passed
        through the receiving stream - either read from data source (:meth:`DataSource.rows`) or written
        to data target (:meth:`DataTarget.append`).

        Subclasses should implement `fields` property getter. Implementing `fields` setter is optional.

        Implementation of `fields` setter is recommended for :class:`DataSource` subclasses such as CSV
        files or typeless document based database. For example: explicitly specify field names for CSVs
        without headers or for specify field analytical or storage types for further processing. Setter
        is recommended also for :class:`DataTarget` subclasses that create datasets (new CSV file,
        non-existing tables).
        """
        raise NotImplementedError()

    def set_fields(self, value):
        raise Exception("Data stream %s does not support setting fields." % str(self.__class__))

    fields = property(get_fields, set_fields)

    @property
    def field_names(self):
        """Returns list of field names. This is shourt-cut for extracting field.name attribute from
        list of field objects returned by :meth:`fields`.
        """
        return [field.name for field in self.fields]

class DataSource(DataStream):
    """Input data stream - for reading."""

    def rows(self):
        """Return iterable object with tuples. This is the main method for reading from
        data source. Subclasses should implement this method.
        """
        raise NotImplementedError()

class DataTarget(DataStream):
    """Output data stream - for writing.
    """

    def append(self, object):
        """Append an object into dataset. Object can be a tuple, array or a dict object. If tuple
        or array is used, then value position should correspond to field position in the field list,
        if dict is used, the keys should be valid field names.        
        """
        raise NotImplementedError()
     
class Dataset(object):
    """Object representing a dataset in a datastore"""

    def truncate(self):
        """Remove all rows/records from the dataset."""
        raise NotImplementedError()
    
    def append(self, object):
        """Append an object into dataset. Object can be a tuple, array or a dict object. If tuple
        or array is used, then value position should correspond to field position in the field list,
        if dict is used, the keys should be valid field names.
        
        .. seealso:: :meth:`DataSource.append`
        
        """
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
        
        :Arguments:
            - `limit`: read only specified number of records from dataset to guess field properties
            
        Returns: tuple with Field objects. Order of fields is datastore adapter specific.
        """

    def rows(self):
        """Return iterable object with tuples.
        
        .. seealso:: :meth:`DataSource.rows`
        
        """
        return self.table.select().execute()
