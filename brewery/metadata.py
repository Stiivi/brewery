import copy
# from collections import OrderedDict

__all__ = [
    "Field",
    "FieldList",
    "fieldlist", # FIXME remove this
    "expand_record",
    "collapse_record",
    "FieldMap",
    "storage_types",
    "analytical_types"
]

"""Abstracted field storage types"""
storage_types = ("unknown", "string", "text", "integer", "float", 
                 "boolean", "date", "array")

"""Analytical types used by analytical nodes"""
analytical_types = ("default", "typeless", "flag", "discrete", "range", 
                    "set", "ordered_set")

"""Mapping between storage types and their respective default analytical 
types"""
# NOTE: For the time being, this is private
default_analytical_types = {
                "unknown": "typeless",
                "string": "typeless",
                "text": "typeless",
                "integer": "discrete",
                "float": "range",
                "date": "typeless",
                "array": "typeless"
            }

_valid_retype_attributes = ("storage_type", 
                     "analytical_type", 
                     "concrete_storage_type",
                     "missing_values")

# FIXME: Depreciated - why it is here, if we have FieldList class?!
def fieldlist(fields):
    """Create a :class:`FieldList` from a list of strings, dictionaries or tuples.

    How fields are constructed:

    * string: `field name` is set 
    * tuple: (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is
      obligatory, rest is optional
    * dict: contains key-value pairs for initializing a :class:`Field` object

    For strings and in if not explicitly specified in a tuple or a dict case, then following rules
    apply:

    * `storage_type` is set to ``unknown``
    * `analytical_type` is set to ``typeless``
    """
    # FIXME: print some warning here
    return FieldList(fields)

def expand_record(record, separator = '.'):
    """Expand record represented as dict object by treating keys as key paths separated by
    `separator`, which is by default ``.``. For example: ``{ "product.code": 10 }`` will become
    ``{ "product" = { "code": 10 } }``
    
    See :func:`brewery.collapse_record` for reverse operation.
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
    """See :func:`brewery.expand_record` for reverse operation.
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

def to_field(obj):
    """Converts `obj` to a field object. `obj` can be ``str``, ``tuple`` 
    (``list``), ``dict`` object or :class:`Field` object. If it is `Field` 
    instance, then same object is passed.
    
    If field is not a `Field` instance, then construction of new field is as follows:

    ``str``:
        `field name` is set 

    ``tuple``:
        (`field_name`, `storaget_type`, `analytical_type`), the `field_name` is 
        obligatory, rest is optional

    ``dict``
        contains key-value pairs for initializing a :class:`Field` object

    Attributes of a field that are not specified in the `obj` are filled as: 
    `storage_type` is set to ``unknown``, `analytical_type` is set to 
    ``typeless``
    """


    if isinstance(obj, Field):
        field = obj
    else:
        d = { "storage_type": "unknown" }

        if isinstance(obj, basestring):
            d["name"] = obj
        elif type(obj) == tuple or type(obj) == list:
            d["name"] = obj[0]
            try:
                d["storage_type"] = obj[1]
                try:
                    d["analytical_type"] = obj[2]
                except:
                    pass
            except:
                pass
        else: # assume dictionary
            d["name"] = obj["name"]
            d["label"] = obj.get("label")
            d["storage_type"] = obj.get("storage_type")
            d["analytical_type"] = obj.get("analytical_type")
            d["adapter_storage_type"] = obj.get("adapter_storage_type")

        if "analytical_type" not in d:
            storage_type = d.get("storage_type")
            if storage_type:
                deftype = default_analytical_types.get(storage_type)
                d["analytical_type"] = deftype or "typeless"
            else:
                d["analytical_type"] = "typeless"

        field = Field(**d)
    return field

class Field(object):
    """Metadata - information about a field in a dataset or in a datastream.

    :Attributes:
        * `name` - field name
        * `label` - optional human readable field label
        * `storage_type` - Normalized data storage type. The data storage type 
          is abstracted
        * `concrete_storage_type` (optional, recommended) - Data store/database 
          dependent storage type - this is the real name of data type as used 
          in a database where the field comes from or where the field is going 
          to be created (this might be null if unknown)
        * `analytical_type` - data type used in data mining algorithms
        * `missing_values` (optional) - Array of values that represent missing 
          values in the dataset for given field
    """

    def __init__(self, name, storage_type="unknown",
                 analytical_type="typeless", concrete_storage_type=None,
                 missing_values=None, label=None):
        self.name = name
        self.label = label
        self.storage_type = storage_type
        self.analytical_type = analytical_type
        self.concrete_storage_type = concrete_storage_type
        self.missing_values = missing_values

    def to_dict(self):
        """Return dictionary representation of the field."""
        d = {
                "name": self.name,
                "label": self.label,
                "storage_type": self.storage_type,
                "analytical_type": self.analytical_type,
                "concrete_storage_type": self.concrete_storage_type,
                "missing_values": self.missing_values
            }
        return d

    def __str__(self):
        """Return field name as field string representation."""
        
        return self.name

    def __repr__(self):
        return "<%s(%s)>" % (self.__class__, self.to_dict())

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

        # FIXME: use OrderedDict (Python 2.7+)
        self._fields = []
        self._field_dict = {}
        self._field_names = []

        if fields:
            # Convert input to Field instances
            # This is convenience, so one can pass list of strsings, for example

            fields = [to_field(f) for f in fields]
            for field in fields:
                self.append(field)
        
    def append(self, field):
        """Appends a field to the list. This method requires `field` to be 
        instance of `Field`"""
    
        field = to_field(field)
        self._fields.append(field)
        self._field_dict[field.name] = field
        self._field_names.append(field.name)
        
    def names(self, indexes = None):
        """Return names of fields in the list.

        :Parameters:
            * `indexes` - list of indexes for which field names should be collected. If set to
              ``None`` then all field names are collected - this is default behaviour.
        """
        
        if indexes:
            names = [self._field_names[i] for i in indexes]
            return names
        else:
            return self._field_names

    def indexes(self, fields):
        """Return a tuple with indexes of fields from ``fields`` in a data row. Fields
        should be a list of ``Field`` objects or strings.
        
        This method is useful when it is more desirable to process data as rows (arrays), not as
        dictionaries, for example for performance purposes.
        """

        indexes = [self.index(field) for field in fields]

        return tuple(indexes)

    def index(self, field):
        """Return index of a field"""
        
        try:
            index = self._field_names.index(str(field))
        except ValueError:
            raise KeyError("Field list has no field with name '%s'" % str(field))

        return index

    def fields(self, names = None):
        """Return a tuple with fields. `names` specifies which fields are returned. When names is
        ``None`` all fields are returned.
        """

        if not names:
            return self._fields

        fields = [self._field_dict[name] for name in names]

        return fields

    def field(self, name):
        """Return a field with name `name`"""

        if name in self._field_dict:
            return self._field_dict[name]
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
        
    def __str__(self):
        return "[" + ", ".join(self.names()) + "]"
    
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
                
    def retype(self, dictionary):
        """Retype fields according to the dictionary. Dictionary contains
        field names as keys and field attribute dictionary as values."""
        
        for name, retype in dictionary.items():
            field = self._field_dict[name]
            for key, value in retype.items():
                if key in _valid_retype_attributes:
                    field.__setattr__(key, value)
                else:
                    raise Exception("Should not use retype to change field attribute '%s'", key)
            
class FieldMap(object):
    """Filters fields in a stream"""
    def __init__(self, rename = None, drop = None, keep=None):
        """Creates a field map. `rename` is a dictionary where keys are input
        field names and values are output field names. `drop` is list of 
        field names that will be dropped from the stream. If `keep` is used,
        then all fields are dropped except those specified in `keep` list."""
        if drop and keep:
            raise Exception('Configuration error in FieldMap: you cant specify both keep and drop options.')
        super(FieldMap, self).__init__()

        if rename:
            self.rename = rename
        else:
            self.rename = {}

        self.drop = drop or []
        self.keep = keep or []
        
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

            if (self.drop and field.name not in self.drop) or \
                (self.keep and field.name in self.keep) or \
                not (self.keep or self.drop):
                output_fields.append(new_field)
            
        return output_fields


    def row_filter(self, fields):
        """Returns an object that will convert rows with structure specified in `fields`. You can
        use the object to filter fields from a row (list, array) according to this map."""
        indexes = []
        
        for i, field in enumerate(fields):
            if (self.drop and field.name not in self.drop) or \
                (self.keep and field.name in self.keep) or \
                not (self.keep or self.drop):
                indexes.append(i)
                
        return RowFieldFilter(indexes)

class RowFieldFilter(object):
    """Class for filtering fields in array"""

    def __init__(self, indexes = None):
        """Create an instance of RowFieldFilter. `indexes` is a list of indexes that are passed
        to output."""
        super(RowFieldFilter, self).__init__()
        if indexes is not None:
            self.indexes = indexes
        else:
            self.indexes = []
        
    def filter(self, row):
        """Filter a `row` according to ``indexes``."""
        return [row[i] for i in self.indexes]
