"""Data Stores"""

__all__ = [
        "DataStore",
        "DataObject",
        "IterableDataSource"
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

    def __len__(self):
        raise NotImplementedError
    def best_representation(self, reps, required_store=None):
        """Returns best representation from list of representations `reps`. If
        store is provided, then target must be from the same store, otherwise
        an exception is raised.
        """
        pass


class IterableDataSource(DataObject):
    def __init__(self, iterable, fields):
        """Create a data object that wraps an iterable."""
        self.fields = fields
        self.iterable = iterable

    def representations(self):
        """Returns the only representation of iterable object, which is
        `rows`"""
        return ["rows"]

    def dup(self, copies=1):
        iterables = itertools.tee(self.iterable, copies + 1)
        self.iterable = iterables[0]
        return iterables [1:]

    def rows(self):
        return iter(self.iterable)

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


    # Should define:
    # __len__



