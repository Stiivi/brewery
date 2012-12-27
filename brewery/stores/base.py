"""Data Stores"""

__all__ = [
        "DataStore",
        "DataObject"
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

class DataObject(object):
    def best_representation(self, reps, required_store=None):
        """Returns best representation from list of representations `reps`. If
        store is provided, then target must be from the same store, otherwise
        an exception is raised.
        """
        pass

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

    def __iter__(self):
        return self.rows()
        # return DataObjectRowsIterator(self)

    def __len__(self):
        raise NotImplementedError

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

    # Should define:
    # __len__

class DataObjectRowsIterator(object):
    """Default iterator for data objects with "rows" representation that do
    not provide their own iterator"""

    def __init__(self, data_object):
        self.iterator = data_object.rows()

    def __iter__(self):
        return self

    def next(self):
        return next(self.iterator)

