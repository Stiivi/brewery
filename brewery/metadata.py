# -*- coding: utf-8 -*-
import copy
import itertools
import functools
import re
from .errors import *
import inspect
# from collections import OrderedDict

__all__ = [
    "to_field",
    "Field",
    "FieldList",
    "FieldFilter",
    "storage_types",
    "analytical_types"
]

"""Abstracted field storage types"""
storage_types = (
        "unknown",  # Unspecified storage type, processing behavior is undefined
        "string",   # names, labels, up to hundreds of hundreds of chars
        "text",     # bigger text storage
        "integer",  # integer numeric types
        "float",    # floating point types
        "boolean",
        "date",
        "array"     # ordered collection type
    )



"""Analytical types used by analytical nodes"""
analytical_types = ("default", "typeless", "flag", "discrete", "measure",
                    "nominal", "ordinal")

"""Mapping between storage types and their respective default analytical
types"""
# NOTE: For the time being, this is private
default_analytical_types = {
                "unknown": "typeless",
                "string": "typeless",
                "text": "typeless",
                "integer": "discrete",
                "float": "measure",
                "date": "typeless",
                "array": "typeless"
            }

_valid_retype_attributes = ("storage_type",
                     "analytical_type",
                     "concrete_storage_type",
                     "missing_values")

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
        # Set defaults first
        d = { }

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
            d["concrete_storage_type"] = obj.get("concrete_storage_type")

        if "analytical_type" not in d:
            storage_type = d.get("storage_type")
            if storage_type:
                deftype = default_analytical_types.get(storage_type)

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
        * `info` – user specific field information, might contain formatting
          information for example
        * `origin` – object that provides contents for the field (for example
          another field)
        * `owner` – object owning the field (for example: a node)
    """

    attributes = ["name", "storage_type", "analytical_type",
                  "concrete_storage_type", "missing_values",
                  "label", "info", "origin", "owner"]

    attribute_defaults = {
                "storage_type":"unknown",
                "analytical_type": None
            }

    def __init__(self, *args, **kwargs):
        super(Field,self).__init__()

        object.__setattr__(self, "_frozen", False)

        not_set = set(self.attributes)

        for name, value in zip(self.attributes, args):
            setattr(self, name, value)
            not_set.remove(name)

        for name, value in kwargs.items():
            if name in not_set:
                setattr(self, name, value)
                not_set.remove(name)
            else:
                raise ValueError("Argument %s specified more than once" % name)

        for name in not_set:
            setattr(self, name, self.attribute_defaults.get(name, None))

    def freeze(self):
        """Freezes the field so the attributes can not be changed."""
        self._frozen = True

    @property
    def is_frozen(self):
        return self._frozen

    def to_dict(self):
        """Return dictionary representation of the field."""
        d = {}
        for name in self.attributes:
            d[name] = getattr(self, name)
        return d

    def __copy__(self):
        field = Field(**self.to_dict())
        return field

    def __str__(self):
        """Return field name as field string representation."""

        return self.name

    def __repr__(self):
        return "%s(%s)" % (self.__class__.__name__, self.to_dict())

    def __eq__(self, other):
        if self is other:
            return True
        if not isinstance(other, Field):
            return False

        for name in self.attributes:
            # FIXME: this is temporary hack
            if name == "origin":
                continue
            if getattr(self, name) != getattr(other, name):
                print "failed comparison on %s" % name
                return False
        return True

    def __ne__(self,other):
        return not self.__eq__(other)

    def __hash__(self):
        if self._frozen:
            if isinstance(self.origin, Field):
                return hash(self.origin)
            else:
                return self.name.__hash__()
        else:
            raise TypeError("Unfrozen field is not hashable")

    def __setattr__(self, name, value):
        if name != "_frozen" and self._frozen:
            raise AttributeError("Field attributes can not be changed (trying to "
                             "change attribute %s)" % name)
        else:
            object.__setattr__(self, name, value)

class FieldList(object):
    """List of fields"""
    def __init__(self, fields=None):
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

            for field in fields:
                self.append(field)

    def append(self, field):
        """Appends a field to the list. This method requires `field` to be
        instance of `Field`"""

        field = to_field(field)
        self._fields.append(field)
        self._field_dict[field.name] = field
        self._field_names.append(field.name)

    def names(self, indexes=None):
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

    def index_map(self):
        """Returns a map of field name to field index"""
        return dict( (f, i) for f, i in enumerate(self._field_names))

    def mask(self, fields=None):
        """Return a list representing field selector - which fields are
        selected from a row."""

        sel_names = [str(field) for field in fields]

        mask = [unicode(name) in sel_names for name in self.names()]
        return mask

    def index(self, field):
        """Return index of a field"""

        try:
            index = self._field_names.index(unicode(field))
        except ValueError:
            raise NoSuchFieldError("Field list has no field with name '%s'" % unicode(field))

        return index

    def fields(self, names=None):
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
        raise NoSuchFieldError("Field list has no field with name '%s'" % name)

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
        if isinstance(field, basestring):
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

    def __repr__(self):
        frepr = [repr(field) for field in self._fields]
        return "%s([%s])" % (self.__class__.__name__, ",".join(frepr))

    def copy(self, fields=None):
        """Return a shallow copy of the list.

        :Parameters:
            * `fields` - list of fields to be copied.
        """
        # FIXME: depreciated
        if fields is not None:
            copy_fields = self.fields(fields)
            return FieldList(copy_fields)
        else:
            return FieldList(self._fields)

    def clone(self, fields=None, origin=None, freeze=False):
        """Creates a copy of the list and copy of the fields. Copied fields
        are unfrozen and origin is set to the cloned field, if not
        specified otherwise. If `freeze` is true, then newly created
        fields are immediately frozen, disallowing any changes to them."""

        fields = self.fields(fields)

        cloned_fields = FieldList()
        for field in fields:
            new_field = copy.copy(field)
            new_field.origin = origin or field
            cloned_fields.append(new_field)

            if freeze:
                new_field.freeze()
        return cloned_fields

class FieldFilter(object):
    """Filters fields in a stream"""
    def __init__(self, rename=None, drop=None, keep=None):
        """Creates a field map. `rename` is a dictionary where keys are input
        field names and values are output field names. `drop` is list of
        field names that will be dropped from the stream. If `keep` is used,
        then all fields are dropped except those specified in `keep` list."""
        if drop and keep:
            raise MetadataError("You can nott specify both 'keep' and 'drop' "
                                "options in FieldFilter.")

        super(FieldFilter, self).__init__()

        self.rename = rename or {}
        self.drop = drop or []
        self.keep = keep or []

    def filter(self, fields):
        """Map `fields` according to the FieldFilter: rename or drop fields as
        specified. Returns a new FieldList object.

        .. note::

            For each renamed field a new copy is created. Not renamed fields
            are the same as in `fields`. To use filtered fields in a node
            you have to clone the field list.
        """
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
        """Returns an object that will convert rows with structure specified in
        `fields`. You can use the object to filter fields from a row (list,
        array) according to this map.
        """
        return RowFieldFilter(self.field_mask(fields))

    def field_mask(self, fields):
        """Returns a list where ``True`` value is set for field that is selected
        and ``False`` for field that has to be ignored. Selectors of fields can
        be used by `itertools.compress()`. This is the preferred way of field
        filtering.
        """

        selectors = []

        for field in fields:
            flag = (self.drop and field.name not in self.drop) \
                    or (self.keep and field.name in self.keep) \
                    or not (self.keep or self.drop)
            selectors.append(flag)

        return selectors


class RowFieldFilter(object):
    """Class for filtering fields in array"""

    def __init__(self, mask=None):
        """Create an instance of RowFieldFilter. `mask` is a list of indexes
        that are passed to output."""
        super(RowFieldFilter, self).__init__()
        self.mask = mask or []

    def __call__(self, row):
        return self.filter(row)

    def filter(self, row):
        """Filter a `row` according to ``indexes``."""
        return list(itertools.compress(row, self.mask))

