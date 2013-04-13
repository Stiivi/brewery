# -*- Encoding: utf8 -*-

"""Data Objects"""

# FIXME: add this
# from .ops.iterator import as_records
from .errors import *
from .extensions import *
from . import ops
from .util import *
from .metadata import *

__all__ = [
        "DataObject",
        "IterableDataSource",
        "RowListDataObject",
        "IterableRecordsDataSource",

        "shared_representations",
        "data_object",
        ]

def data_object(type_, *args, **kwargs):
    """Returns a data object of specified type. Arguments are passed to
    respective data object factory.

    Available data objects:

    * sql_table
    * sql_statement
    * csv_source
    * csv_target
    * iterable
    * iterable_records
    * row_list
    * mdb_source

    For more information please refer to the documentation of data objects.
    """


    ns = get_namespace("object_types")
    if not ns:
        ns = initialize_namespace("object_types", root_class=DataObject,
                                    suffix=None)

    try:
        factory = ns[type_]
    except KeyError:
        raise BreweryError("Unable to find factory for object of type %s" %
                                type_)
    return factory(*args, **kwargs)


class DataObject(object):
    def representations(self):
        """Returns list of representation names of this data object. Default
        implementation returns only `rows` as this representation should be
        implemented by all data objects. """
        return ["rows"]

    def is_compatible(self, obj, required=None, ignored=None):
        """Returns `True` when the receiver and the `object` share
        representations. `required` contains list of representations that at
        least one of them is required. If not present, then returns `False`.
        `ignored` is a list of representations that are not relevant for the
        comparison."""
        required = set(required or [])
        ignored = set(ignored or [])
        ours = set(self.representations() or [])
        theirs = set(obj.representations or [])

        reprs = (ours - ignored) & (theirs - ignored)
        if required:
            reprs = reprs & required

        return len(reprs) > 0

    @required
    def can_compose(self, obj):
        """Returns `True` when any of the representations can be naturally
        (without a proxy) composed by any of the representations of `obj` to
        form new representation.  Example of composable objects are SQL
        statement object from the same engine. Subclasses should implement
        this method. Default implementation returns `False`, which means that
        the only suggested composition is to use iterators.

        The method should be transient. That is, if A can be naturally
        composed with B and B with C, then A can be naturally composed with C
        as well. This property is for simplification of finding whether a list
        of objects can be composed together or not.
        """

        return False

    def truncate(self):
        """Removes all records from the target table"""
        raise NotImplementedError

    def flush(self):
        """Flushes oustanding data. Default implementation does nothing.
        Subclasses might implement this method if necessary."""
        pass

    @experimental
    def dup(self, copies=1):
        """Returns `copies` duplicates of the object. This method does not
        create physical copy of the object, just returns duplicate Python
        object with representations that can be used independently of the
        original object.

        For example, if the target object is an iterator, using `dup()` will
        cause to return an object with same description but copy of the
        iterator, therefore allowing to iterate same source twice.

        .. note::

            Not all objects or all representations might be duplicated. Refer
            to particular object implementation for more information.

        .. note::

            Calling `dup()` might cause the recevier to change state,
            depending on requirements imposed on the duplication. It is
            recommended to create object's duplicates before actual evaluation
            of the object's representations.

        If you are implementing a data object, it is hightly recommended to
        provide duplicates of all representations.
        """
        raise NotImplementedError

    def __iter__(self):
        return self.rows()

    def best_representation(self, reps, required_store=None):
        """Returns best representation from list of representations `reps`. If
        store is provided, then target must be from the same store, otherwise
        an exception is raised.
        """
        pass

    def records(self):
        """Returns an iterator of records - dictionary-like objects that can
        be acessed by field names. Default implementation returns
        dictionaries, however other objects mith return their own structures.
        For example the SQL object returns the same iterator as for rows, as
        it can serve as key-value structure as well."""

        names = [str(field) for field in fields]

        for row in self.rows():
            yield dict(zip(names, row))

    def append(self, row):
        """Appends `row` to the object. Subclasses should implement this."""
        raise NotImplementedError

    def append_from(self, obj):
        """Appends data from object `obj` which might be a `DataObject`
        instance or an iterable. Default implementation uses iterable and
        calls `append()` for each element of the iterable.
        """
        for row in iter(obj):
            self.append(row)

    def initialize(self):
        """Backward compatibility with the ds module streams. Does nothing."""
        # FIXME: issue warning at some point
        pass

    def as_source(self):
        """Returns version of the object that can be used as source. Subclasses
        might return an object that will raise exception on attempt to use
        target-only methods such as appending.

        Default implementation returns the receiver."""
        return self

    def as_target(self):
        """Returns version of the object that can be used as target. Subclasses
        might return an object that will raise exception on attempt to use
        source-only methods such as iterating rows.

        Default implementation returns the receiver."""
        return self

    def is_reusable(self):
        """Returns `True` if the object's representations can be used more
        than once yielding the same result. Default is `False`"""
        return False

    @required
    def filter(self, keep=None, drop=None, rename=None):
        """Returns an object with filtered fields"""
        return NotImplementedError

    def as_dict(self, key=None):
        """Returns a dictionary-like representation of the receiver. `key` is
        a list of fields or field names that will be used as a lookup key.

        Default implementation iterates through whole data object and creates
        the dictionary. This might be very costly on large datasets.

        ... warning::

            This method might change/move.
        """

        return ops.iterator.to_dict(self.rows(), self.fields, key)

def shared_representations(objects):
    """Returns representations that are shared by all `objects`"""
    objects = objects.values()
    reps = set(objects[0].representations())
    for obj in objects[1:]:
        reps &= set(obj.representations())

    return reps

class IterableDataSource(DataObject):
    """Wrapped Python iterator that serves as data source. The iterator should
    yield "rows" – list of values according to `fields` """

    _ns_object_name = "iterable"

    _brewery_info = {
        "attributes": [
            {"name":"iterable", "description": "Python iterable object"},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }


    def __init__(self, iterable, fields):
        """Create a data object that wraps an iterable."""
        self.fields = fields
        self.iterable = iterable

    def representations(self):
        """Returns the only representation of iterable object, which is
        `rows`"""
        return ["rows", "records"]

    def dup(self, copies=1):
        iterables = itertools.tee(self.iterable, copies + 1)
        self.iterable = iterables[0]

        dups = []
        for i in iterables[1:]:
            dups.append(self.__class__(iterables[1:], self.fields))
        return dups

    def rows(self):
        return iter(self.iterable)

    def records(self):
        return as_records(self.iterable, self.fields)


    def filter(self, keep=None, drop=None, rename=None):
        """Returns another iterable data source with filtered fields"""

        ffilter = FieldFilter(keep=keep, drop=drop, rename=rename)
        fields = ffilter.filter(self.fields)

        if keep or drop:
            iterator = ops.iterator.field_filter(self.iterable, self.fields,
                                                 ffilter)
        else:
            # No need to filter if we are just renaming, reuse the iterator
            iterator = self.iterable

        return IterableDataSource(iterator, fields)

class IterableRecordsDataSource(IterableDataSource):
    """Wrapped Python iterator that serves as data source. The iterator should
    yield "records" – dictionaries with keys as specified in `fields` """

    _ns_object_name = "iterable_records"

    _brewery_info = {
        "attributes": [
            {"name":"iterable", "description": "Python iterable object"},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }

    def rows(self):
        names = [str(field) for field in self.fields]
        for record in self.iterable:
            yield [record[f] for f in names]

    def records(self):
        return iter(self.iterable)

class RowListDataObject(DataObject):
    """Wrapped Python list that serves as data source or data target. The list
    content are "rows" – lists of values corresponding to `fields`.

    If list is not provided, one will be created.
    """

    _ns_object_name = "list"

    _brewery_info = {
        "attributes": [
            {"name":"data", "description": "List object."},
            {"name":"fields", "description":"fields of the iterable"}
        ]
    }

    def __init__(self, fields, data=None):
        """Create a data object that wraps an iterable. The object is dumb,
        does not perform any field checking, accepts anything passed to it.
        `data` should be appendable list-like object, if not provided, empty
        list is created."""
        self.fields = fields
        if data is None:
            self.data = []
        else:
            self.data = data

    def representations(self):
        """Returns the only representation of iterable object, which is
        `rows`"""
        return ["rows", "records"]

    def rows(self):
        return iter(self.data)

    def records(self):
        return as_records(self.rows(), self.fields)

    def append(self, row):
        self.data.append(row)

    def truncate(self):
        self.data = []


