# -*- coding: utf-8 -*-
# Common brewery functions and classes - related to data processing or process
# management
#
# For language utility functions see module util

import re
import sys
import urllib2
import urlparse
import logging

__all__ = [
    "logger_name",
    "get_logger",
    "create_logger",
    "IgnoringDictionary",
    "MissingPackage",
    "decamelize",
    "to_identifier",
    "get_backend",
    # FIXME: move these
    "coalesce_value",
    "collapse_record",
    "expand_record"
]

# FIXME: this is for array orientation
backend_aliases = {
            "default":"python_array",
            "python":"python_array",
            "carray":"carray_backend"
        }

_backends = { }

logger_name = "brewery"
logger = None

def get_logger():
    """Get brewery default logger"""
    global logger

    if logger:
        return logger
    else:
        return create_logger()

def create_logger():
    """Create a default logger"""
    global logger
    logger = logging.getLogger(logger_name)

    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)s %(message)s')

    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if __debug__:
        logger.setLevel(logging.DEBUG)


    return logger

class IgnoringDictionary(dict):
    """Simple dictionary extension that will ignore any keys of which values
    are empty (None/False)"""
    def setnoempty(self, key, value):
        """Set value in a dictionary if value is not null"""
        if value:
            self[key] = value

class MissingPackageError(Exception):
    """Exception raised when encountered a missing package."""
    pass

class MissingPackage(object):
    """Bogus class to handle missing optional packages - packages that are not
    necessarily required for Cubes, but are needed for certain features."""

    def __init__(self, package, feature = None, source = None, comment = None):
        self.package = package
        self.feature = feature
        self.source = source
        self.comment = comment

    def __call__(self, *args, **kwargs):
        self._fail()

    def __getattr__(self, name):
        self._fail()

    def _fail(self):
        if self.feature:
            use = " to be able to use: %s" % self.feature
        else:
            use = ""

        if self.source:
            source = " from %s" % self.source
        else:
            source = ""

        if self.comment:
            comment = ". %s" % self.comment
        else:
            comment = ""

        raise MissingPackageError("Optional package '%s' is not installed. "
                                  "Please install the package%s%s%s" %
                                      (self.package, source, use, comment))

def decamelize(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1)

def to_identifier(name):
    return re.sub(r' ', r'_', name).lower()

# FIXME: depreciated
def register_backend(backend_name, backend):
    """Registers `backend` under `backend_name`.

    In the future this version will do backend validation and will register
    other objects provided by the backend.
    """

    _backends[backend_name] = backend

# FIXME: depreciated
def get_backend(backend_name):
    """Finds the backend with name `backend_name`. First try to find backend
    relative to the brewery.backends.* then search full path. """

    if backend_name in _backends:
        return _backends[backend_name]

    backend_name = backend_aliases.get(backend_name, backend_name)
    backend = sys.modules.get("brewery.backends."+backend_name)

    if not backend:
        # Then try to find a module with full module path name
        try:
            backend = sys.modules[backend_name]
        except KeyError as e:
            raise Exception("Unable to find backend module %s (%s)" % (backend_name, e))

    if not hasattr(backend, "create_array"):
        raise NotImplementedError("Backend %s does not implement create_array" % backend_name)

    return backend

def get_backend_object(reference):
    """Get object from a backend. `reference` is a string which valid Python
    reference to a module object. First relative to `brewery.backends` module
    is checked, then `reference` is considered as absolute reference."""

    raise NotImplemented

def open_resource(resource, mode = None):
    """Get file-like handle for a resource. Conversion:

    * if resource is a string and it is not URL or it is file:// URL, then opens a file
    * if resource is URL then opens urllib2 handle
    * otherwise assume that resource is a file-like handle

    Returns tuple: (handle, should_close) where `handle` is file-like object and `should_close` is
        a flag whether returned handle should be closed or not. Closed should be resources which
        where opened by this method, that is resources referenced by a string or URL.
    """

    if type(resource) == str or type(resource) == unicode:
        should_close = True
        parts = urlparse.urlparse(resource)
        if parts.scheme == '' or parts.scheme == 'file':
            if mode:
                handle = open(resource, mode=mode)
            else:
                handle = open(resource)
        else:
            handle = urllib2.urlopen(resource)
    else:
        should_close = False
        handle = resource

    return handle, should_close

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



def coalesce_value(value, storage_type, empty_values=None, strip=False):
    """Coalesces `value` to given storage `type`. `empty_values` is a dictionary
    where keys are storage type names and values are values to be used
    as empty value replacements."""
    if empty_values is None:
        empty_values={}
    if storage_type in ["string", "text"]:
        if strip:
            value = value.strip()
        elif value:
            value = unicode(value)

        if value == "" or value is None:
            value = empty_values.get("string")
    elif storage_type == "integer":
        # FIXME: use configurable thousands separator (now uses space)
        if strip:
            value = re.sub(r"\s", "", value.strip())

        try:
            value = int(value)
        except ValueError:
            value = empty_values.get("integer")
    elif storage_type == "float":
        # FIXME: use configurable thousands separator (now uses space)
        if strip:
            value = re.sub(r"\s", "", value.strip())

        try:
            value = float(value)
        except ValueError:
            value = empty_values.get("float")
    elif storage_type == "list":
        # FIXME: undocumented type
        value = value.split(",")

    return value
