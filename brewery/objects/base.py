"""Data Stores"""

from ..ops.iterator import as_records

__all__ = [
        "DataStore",
        "SimpleMemoryStore",

        "DataObject",
        "IterableDataSource",
        "RowListDataObject",
        "IterableRecordsDataSource",

        "shared_representations"
        ]

_data_stores = {
            "sql":"sql.SQLDataStore",
            "csv_directory": "text.CSVDirectoryDataStore",
            "mdb":"mdb.MDBDataStore"
        }

def open_datastore(type_, **options):
    """Opens datastore of `type`."""

    backend = get_backend(type_)

    return backend.open_store(**options)

data_object_types = [
            "rows", # iterator of rows
            "sql_statement"   # SQLAlchemy statement object
            "sql_table" # SQLAlchemy table object
        ]

class DataObject(object):
    def representations(self):
        """Returns list of representation names of this data object. Default
        implementation returns only `rows` as this representation should be
        implemented by all data objects. """
        return ["rows"]

    def is_compatible(self, obj, required=None, ignored=None):
        """Reeturns `True` when the receiver and the `object` share
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
        """Flushes oustanding data"""
        pass

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

def shared_representations(objects):
    """Returns representations that are shared by all `objects`"""
    objects = objects.values()
    reps = set(objects[0].representations())
    for obj in objects[1:]:
        reps &= set(obj.representations())

    return reps

class IterableDataSource(DataObject):
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

class IterableRecordsDataSource(IterableDataSource):
    def rows(self):
        names = [str(field) for field in self.fields]
        for record in self.iterable:
            yield [record[f] for f in names]

    def records(self):
        return iter(self.iterable)

class RowListDataObject(DataObject):
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

class TargetDataObject(DataObject):
    def append(self, row):
        raise IsNotSourceError
    def append_from(self, obj):
        raise IsNotSourceError
    def truncate(self):
        raise IsNotSourceError

class SourceDataObject(DataObject):
    def rows(self):
        raise IsNotTargetError

class DataStore(object):
    def __init__(self, **options):
        pass

    def close(self):
        pass

    def objects(self, names=None):
        """Return list of objects, if available

        * `names`: only objects with given names are returned

        """
        raise NotImplementedError

    def get_object(self, name, **args):
        """Subclasses should implement this"""
        raise NotImplementedError

    def __getitem__(self, name):
        return get_object(self, name)

    def create(name, fields, replace=False, from_obj=None, temporary=False,
               **options):
        """Args:
            * replace
            * form_obj: object from which the target is created
            * temporary: table is destroyed after store is closed or
              disconnected
        """
        pass

    def create_temporary(fields, from_obj=None, **options):
        """Creates a temporary data object"""
        raise NotImplementedError

    def truncate(name, **options):
        obj = self.data_object(name, **args)
        obj.truncate()
        pass

    def rename(name, new_name, force=False):
        """Renames object from `name` to `new_name`. If `force` is ``True``
        then target is lost"""
        raise NotImplementedError


class SimpleMemoryStore(DataStore):
    def __init__(self):
        """Creates simple in-memory data object store. Useful for temporarily
        store objects. Creates list based objects with `rows` and `records`
        representations."""

        super(SimpleMemoryStore, self).__init__()
        catalogue = {}

    def objects(self):
        return catalogue.keys()

    def get_object(self, name):
        try:
            return catalogue[name]
        except KeyError:
            raise NoSuchObjectError(name)

    def create(name, fields, replace=False, from_obj=None, temporary=False,
               **options):
        """Creates and returns a data object that wraps a Python list as a
        data container."""

        if not replace and self.exists(name):
            raise ObjectExistsError(name)

        obj = RowListDataObject(fields)
        catalogue[name] = obj
        return obj

    def exists(name):
        return name in catalogue

def copy_object(source_store, source_name, target_store,
                target_name=None, create=False, replace=False):
    """Convenience method that copies object data from source store to target
    store. `source_object` and `target_object` should be object names within
    the respective stores. If `target_name` is not specified, then
    `source_name` is used."""

    target_name = target_name or source_name

    source = source_store.get_object(source_name)
    if create:
        if not replace and target_store.exists(target_name):
            raise Exception("Target object already exists. Use reaplce=True to "
                            "delete the object object and create replacement")
        target = target_store.create(target_name, source.fields, replace=True,
                                     from_obj=source)
    else:
        target = target_store(target_name)
        target.append_from(source)
        target.flush()

    return target

