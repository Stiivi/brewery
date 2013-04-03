# -*- Encoding: utf8 -*-

from .errors import *
from .metadata import *
from .extensions import *

__all__ = [
        "DataStore",
        "SimpleMemoryStore",
        "open_store",
        "copy_object"
        ]

_data_stores = {
            "sql":"brewery.objects.sql.SQLDataStore",
            "csv_directory": "brewery.objects.text.CSVDirectoryDataStore",
            "mdb":"brewery.objects.mdb.MDBDataStore"
        }

def open_store(type_, *args, **kwargs):
    """Opens datastore of `type`."""

    ns = get_namespace("store_types")
    if not ns:
        ns = initialize_namespace("store_types", root_class=DataStore,
                                suffix="_store")

    try:
        factory = ns[type_]
    except KeyError:
        raise BreweryError("Unable to find factory for store of type '%s'" %
                                type_)
    return factory(*args, **kwargs)


class DataStore(object):
    def __init__(self, **options):
        pass

    def close(self):
        pass

    def object_names(self):
        """Returns list of all object names contained in the store"""
        raise NotImplementedError

    def objects(self, names=None, autoload=False):
        """Return list of objects, if available

        * `names`: only objects with given names are returned
        * `autoload`: load object list if necessary, otherwise cached version
          is used if store cachces object metadata.

        Note that loading list of objects might be costly operation in some
        cases.
        """
        raise NotImplementedError

    def get_object(self, name, **args):
        """Subclasses should implement this"""
        raise NotImplementedError

    def __getitem__(self, name):
        return self.get_object(name)

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
        target = target_store.get_object(target_name)
        target.append_from(source)
        target.flush()

    return target

