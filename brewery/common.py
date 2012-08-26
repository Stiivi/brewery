import re

__all__ = [
    "coalesce_value",
    "collapse_record",
    "expand_record"
]


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
