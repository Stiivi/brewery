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
import brewery.dq
import copy

def fieldlist(fields):
    """Create a :class:`FieldList` from a list of strings, dictionaries or tuples.

    How fields are consutrcuted:

    * string: `field name` is set 
    * tuple: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
      obligatory, rest is optional
    * dict: contains key-value pairs for initializing a :class:`Field` object

    For strings and in if not explicitly specified in a tuple or a dict case, then following rules
    apply:

    * `storage_type` is set to ``unknown``
    * `analytical_type` is set to ``typeless``
    """

    return FieldList(fields)

def field_names(fields):
    """Return field names from list of fields.
    
    :Parameters:
        * `fields` - `FieldList` object, list of `Field` objects or list of strings
        
    Returns a list of strings containing names of fields.
    
    """
    names = []
    if type(fields) == FieldList:
        fields = fields.fields

    for field in fields:
        if type(field) == str or type(field) == unicode:
            name = field
        else:
            name = field.name
        names.append(name)

    return names
    
def field_name(field):
    """Return a field name. If the `field` is a string object, return just the string. If 
    the `field` is `Field` instance then return `field.name` """
    if type(field) == str or type(field) == unicode:
        return field
    else:
        return field.name
    
def expand_record(record, separator = '.'):
    """Expand record represented as dict object by treating keys as key paths separated by
    `separator`, which is by default ``.``. For example: ``{ "product.code": 10 }`` will become
    ``{ "product" = { "code": 10 } }``
    
    See :func:`brewery.ds.collapse_record` for reverse operation.
    """
    result = {}
    for key, value in record.items():
        current = result
        path = key.split(separator)
        for part in path[:-1]:
            if part not in current:
                current[part] = {}
            current = current[part]
        current[path[-1]] = value
    return result

def collapse_record(record, separator = '.', root = None):
    """See :func:`brewery.ds.expand_record` for reverse operation.
    """

    result = {}
    for key, value in record.items():
        if root:
            collapsed_key = root + separator + key
        else:
            collapsed_key = key
        
        if type(value) == dict:
            collapsed = collapse_record(value, separator, collapsed_key)
            result.update(collapsed)
        else:
            result[collapsed_key] = value
    return result

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

    def __init__(self, name, label = None, storage_type = "unknown", analytical_type = "typeless", 
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

    def __eq__(self, other):
        if self is other:
            return True
        if self.name != other.name or self.label != other.label:
            return False
        elif self.storage_type != other.storage_type or self.analytical_type != other.analytical_type:
            return False
        elif self.concrete_storage_type != other.concrete_storage_type:
            return False
        elif self.missing_values != other.missing_values:
            return False
        else:
            return True
            
    def __ne__(self,other):
        return not self.__eq__(other)

class FieldList(object):
    """List of fields"""
    def __init__(self, fields = None):
        """
        Create a list of :class:`Field` objects from a list of strings, dictionaries or tuples

        How fields are consutrcuted:

        * string: `field name` is set 
        * tuple: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
          obligatory, rest is optional
        * dict: contains key-value pairs for initializing a :class:`Field` object

        For strings and in if not explicitly specified in a tuple or a dict case, then following rules
        apply:

        * `storage_type` is set to ``unknown``
        * `analytical_type` is set to ``typeless``
        """
        super(FieldList, self).__init__()

        self._fields = []
        self._field_dict = {}
        self._field_names = []

        if fields:
            for field in fields:
                self.append(field)
        
    def append(self, field):
        """Add field to list of fields.
        
        :Parameters:
            * `field` - :class:`Field` object, ``str``, ``tuple`` or ``dict`` object 

        If field is not a `Field` object, then construction of new field is as follows:

        * ``str``: `field name` is set 
        * ``tuple``: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
          obligatory, rest is optional
        * ``dict``: contains key-value pairs for initializing a :class:`Field` object

        For strings and in if not explicitly specified in a tuple or a dict case, then following rules
        apply:

        * `storage_type` is set to ``unknown``
        * `analytical_type` is set to ``typeless``
        """


        d = {}
        d["storage_type"] = "unknown"
        d["analytical_type"] = "typeless"

        if type(field) == Field:
            # FIXME: should be a copy?
            new_field = field
        else:
            if type(field) == str or type(field) == unicode:
                d["name"] = field
            elif type(field) == tuple or type(field) == list:
                d["name"] = field[0]
                if len(field) > 1:
                    d["storage_type"] = field[1]
                    if len(field) > 2:
                        d["analytical_type"] = field[2]
            elif type(field) == dict:
                d["name"] = field["name"]
                if "label" in field:
                    d["label"] = field["label"]
                if "storage_type" in field:
                    d["storage_type"] = field["storage_type"]
                if "analytical_type" in field:
                    d["analytical_type"] = field["analytical_type"]
                if "adapter_storage_type" in field:
                    d["adapter_storage_type"] = field["adapter_storage_type"]
            else:
                raise ValueError("Unknown field object type ('%s' ) of field description object '%s'" \
                                    % (type(field), field))

            if "analytical_type" not in d:
                deftype = Field.default_analytical_types[d["storage_type"]]
                d["analytical_type"] = deftype

            new_field = Field(**d)
            
        self._fields.append(new_field)
        self._field_dict[new_field.name] = new_field
        self._field_names.append(new_field.name)
        
    def names(self, indexes = None):
        """Return names of fields in the list.

        :Parameters:
            * `indexes` - list of indexes for which field names should be collected. If set to
              ``None`` then all field names are collected - this is default behaviour.
        """
        
        if indexes:
            names = []
            for i in indexes:
                names.append(self._field_names[i])
            return names
        else:
            return self._field_names

    def indexes(self, fields):
        """Return a tuple with indexes of fields from ``fields`` in a data row. Fields
        should be a list of ``Field`` objects or strings"""

        names = field_names(fields)
        indexes = []
        for field in names:
            indexes.append(self.index(field))

        return tuple(indexes)

    def index(self, field):
        """Return index of a field"""
        
        try:
            index = self._field_names.index(field_name(field))
        except ValueError:
            raise KeyError("Field list has no field with name '%s'" % field_name(field))

        return index

    def fields(self, names = None):
        """Return a tuple with indexes of fields from ``fieldlist`` in a data row."""
        if not names:
            return self._fields

        fields = []
        for name in names:
            if name in self._field_dict:
                fields.append(self._field_dict[name])
            else:
                raise KeyError("Field list has no field with name '%s'" % name)

        return fields

    def field(self, name):
        """Return a field with name `name`"""
        if name in self._field_dict:
            fields.append(self._field_dict[name])
        else:
            raise KeyError("Field list has no field with name '%s'" % name)
    
    def __len__(self):
        return len(self._fields)
        
    def __getitem__(self, index):
        return self._fields[index]
        
    def __setitem__(self, index, new_field):
        field = self._fields[index]
        del self._field_dict[field.name]
        self._fields[index] = new_field
        self._field_names[index] = new_field.name
        self._field_dict[new_field.name] = new_field
        
    def __delitem__(self, index):
        field = self._fields[index]
        del self._field_dict[field.name]
        del self._fields[index]
        del self._field_names[index]
        
    def __iter__(self):
        return self._fields.__iter__()
        
    def __contains__(self, field):
        if type(field) == str or type(field) == unicode:
            return field in self._field_names
            
        return field in self._fields

    def __iconcat__(self, array):
        for field in array:
            self.append(field)

    def __concat__(self, array):
        fields = self.copy()
        fields += array
        return fields
        
    def copy(self, fields = None):
        """Return a shallow copy of the list.
        
        :Parameters:
            * `fields` - list of fields to be copied.
        """
        if fields is not None:
            copy_fields = self.fields(fields)
            return FieldList(copy_fields)
        else:
            return FieldList(self._fields)
            
class FieldMap(object):
    """Filters fields in a stream"""
    def __init__(self, rename = None, drop = None):
        super(FieldMap, self).__init__()
        if rename:
            self.rename = rename
        else:
            self.rename = {}
        if drop:
            self.drop = drop
        else:
            self.drop = []
        
    def map(self, fields):
        """Map `fields` according to the FieldMap: rename or drop fields as specified. Returns
        a FieldList object."""
        output_fields = FieldList()
        
        for field in fields:
            if field.name in self.rename:
                # Create a copy and rename field if it is mapped
                new_field = copy.copy(field)
                new_field.name = self.rename[field.name]
            else:
                new_field = field

            if field.name not in self.drop:
                # Pass field if it is not in dropped field list
                output_fields.append(new_field)
            
        return output_fields


    def row_filter(self, fields):
        """Returns an object that will convert rows with structure specified in `fields`."""
        indexes = []
        
        for i, field in enumerate(fields):
            if field.name not in self.drop:
                indexes.append(i)
                
        return RowFieldFilter(indexes)
        
class RowFieldFilter(object):
    def __init__(self, indexes = None):
        super(RowFieldFilter, self).__init__()
        if indexes is not None:
            self.indexes = indexes
        else:
            self.indexes = []
        
    def filter(self, row):
        nrow = []
        for i in self.indexes:
            nrow.append(row[i])
        return nrow
        
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

    def __get_fields(self):
        """Information about fields: tuple of :class:`Field` objects representing fields passed
        through the receiving stream - either read from data source (:meth:`DataSource.rows`) or written
        to data target (:meth:`DataTarget.append`).

        Subclasses should implement `fields` property getter. Implementing `fields` setter is optional.

        Implementation of `fields` setter is recommended for :class:`DataSource` subclasses such as CSV
        files or typeless document based database. For example: explicitly specify field names for CSVs
        without headers or for specify field analytical or storage types for further processing. Setter
        is recommended also for :class:`DataTarget` subclasses that create datasets (new CSV file,
        non-existing tables).
        """
        return self._fields

    def __set_fields(self, fields):
        self._fields = fields
        # raise Exception("Data stream %s does not support setting fields." % str(self.__class__))

    fields = property(__get_fields, __set_fields)

    @property
    def field_names(self):
        """Returns list of field names. This is shourt-cut for extracting field.name attribute from
        list of field objects returned by :meth:`fields`.
        """
        return [field.name for field in self.fields]

class DataSource(DataStream):
    """Input data stream - for reading."""

    def rows(self):
        """Return iterable object with tuples. This is one of two methods for reading from
        data source. Subclasses should implement this method.
        """
        raise NotImplementedError()

    def records(self):
        """Return iterable object with dict objects. This is one of two methods for reading from
        data source. Subclasses should implement this method.
        """
        raise NotImplementedError()

    def read_fields(self, limit = 0, collapse = False):
        """Read field descriptions from data source. You should use this for datasets that do not
        provide metadata directly, such as CSV files, document bases databases or directories with
        structured files. Does nothing in relational databases, as fields are represented by table
        columns and table metadata can obtained from database easily.
        
        Note that this method can be quite costly, as by default all records within dataset are read
        and analysed.
        
        After executing this method, stream ``fields`` is set to the newly read field list and may
        be configured (set more appropriate data types for example).
        
        :Arguments:
            - `limit`: read only specified number of records from dataset to guess field properties
            - `collapse`: whether records are collapsed into flat structure or not
            
        Returns: tuple with Field objects. Order of fields is datastore adapter specific.
        """

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

        count = 0
        for record in self.records():
            if collapse:
                record = collapse_record(record)
            print record
            probe_record(record)
            if limit and count >= limit:
                break
            count += 1

        fields = []

        for key in keys:
            probe = probes[key]
            field = Field(probe.field)

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

class DataTarget(DataStream):
    """Output data stream - for writing.
    """

    def append(self, object):
        """Append an object into dataset. Object can be a tuple, array or a dict object. If tuple
        or array is used, then value position should correspond to field position in the field list,
        if dict is used, the keys should be valid field names.        
        """
        raise NotImplementedError()
     
