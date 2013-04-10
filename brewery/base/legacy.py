# -*- Encoding: utf8 -*-
"""Homeless legacy brewery code lives here. It will be relocated, changed or pruned."""

class FieldTypeProbe(object):
    """Probe for guessing field data type

    Attributes:
        * field: name of a field which statistics are being presented
        * storage_types: found storage types
        * unique_storage_type: if there is only one storage type, then this is set to that type
    """
    def __init__(self, field):
        self.field = field

        self.storage_types = set()

        self.null_count = 0
        self.empty_string_count = 0

    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        if value is None:
            self.null_count += 1
        if value == '':
            self.empty_string_count += 1

    @property
    def unique_storage_type(self):
        """Return storage type if there is only one. This should always return a type in relational
        databases, but does not have to in databases such as MongoDB."""

        if len(self.storage_types) == 1:
            return list(self.storage_types)[0]
        return None

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
