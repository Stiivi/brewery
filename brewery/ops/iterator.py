"""Iterator based operations."""
from ..metadata import *
import itertools
import functools
from collections import OrderedDict

# FIXME: add cheaper version for already sorted data

def as_records(iterator, fields):
    """Returns iterator of dictionaries where keys are defined in fields."""

    names = [str(field) for field in fields]
    for row in iterator:
        yield dict(zip(names, row))

def distinct(iterator, fields, keys, is_sorted=False):
    """Return distinct `keys` from `iterator`. `iterator` does
    not have to be sorted. If iterator is sorted by the keys and `is_sorted`
    is ``True`` then more efficient version is used."""

    row_filter = FieldFilter(keep=keys).row_filter(fields)
    if is_sorted:
        last_key = object()

        # FIXME: use itertools equivalent
        for value in iterator:
            key_tuple = (row_filter(row))
            if key_tuple != last_key:
                yield row

    else:
        distinct_values = set()

        for row in iterator:
            # Construct key tuple from distinct fields
            key_tuple = tuple(row_filter(row))

            if key_tuple not in distinct_values:
                distinct_values.add(key_tuple)
                yield row

def unique(iterator, fields, keys, discard=False):
    """Return rows that are unique by `keys`. If `discard` is `True` then the
    action is reversed and duplicate rows are returned."""

    # FIXME: add is_sorted version

    row_filter = FieldFilter(keep=keys).row_filter(fields)

    distinct_values = set()

    for row in iterator:
        # Construct key tuple from distinct fields
        key_tuple = tuple(row_filter(row))

        if key_tuple not in distinct_values:
            distinct_values.add(key_tuple)
            if not discard:
                yield row
        else:
            if discard:
                # We already have one found record, which was discarded
                # (because discard is true), now we pass duplicates
                yield row

def append(iterators):
    """Appends iterators"""
    return itertools.chain(*iterators)

def sample(iterator, value, discard=False, mode="first"):
    """Returns sample from the iterator. If `mode` is ``first`` (default),
    then `value` is number of first records to be returned. If `mode` is
    ``nth`` then one in `value` records is returned."""

    if mode == "first":
        if discard:
            return itertools.islice(iterator, value, None)
        else:
            return itertools.islice(iterator, value)
    elif mode == "nth":
        if discard:
            return discard_nth(iterator, value)
        else:
            return itertools.islice(iterator, None, None, value)
    elif mode == "random":
        raise NotImplementedError("random sampling is not yet implemented")
    else:
        raise Exception("Unknown sample mode '%s'" % mode)

def discard_nth(iterator, step):
    """Discards every step-th item from `iterator`"""
    for i, value in enumerate(iterator):
        if i % value != 0:
            yield value

def field_filter(iterator, fields, field_filter):
    """Filters fields in `iterator` according to the `field_filter`.
    `iterator` should be a rows iterator and `fields` is list of iterator's
    fields."""
    row_filter = field_filter.row_filter(fields)
    return itertools.imap(row_filter, iterator)

def text_substitute(iterator, fields, field, substitutions):
    """Substitute field using text substitutions"""
    # Compile patterns
    substitutions = [(re.compile(patt), r) for (patt, r) in subsitutions]
    index = fields.index(field)
    for row in iterator:
        row = list(row)

        value = row[index]
        for (pattern, repl) in substitutions:
            value = re.sub(pattern, repl, value)
        row[index] = value

        yield row

def string_strip(iterator, fields, strip_fields=None, chars=None):
    """Strip characters from `strip_fields` in the iterator. If no
    `strip_fields` is provided, then it strips all `string` or `text` storage
    type objects."""

    if not strip_fields:
        strip_fields = []
        for field in fields:
            if field.storage_type =="string" or field.storage_type == "text":
                strip_fields.append(field)

    indexes = fields.indexes(strip_fields)

    for row in iterator:
        row = list(row)
        for index in indexes:
            value = row[index]
            if value:
                row[index] = value.strip(chars)
        yield row

# def threshold(value, low, high, bins=None):
#     """Returns threshold value for `value`. `bins` should be names of bins. By
#     default it is ``['low', 'medium', 'high']``
#     """
#
#     if not bins:
#         bins = ['low', 'medium', 'high']
#     elif len(bins) != 3:
#         raise Exception("bins should be a list of three elements")
#
#     if low is None and high is None:
#         raise Exception("low and hight threshold values should not be "
#                         "both none at the same time.")
#


#####################
# Transformations

class CopyValueTransformation(object):
    def __init__(self, fields, source, missing_value=None):
        self.source_index = fields.index(source)
        self.missing_value = missing_value
    def __call__(self, row):
        return row[self.source_index] or missing_value

class SetValueTransformation(object):
    def __init__(self, value):
        self.value = value
    def __call__(self, row):
        return self.value

class MapTransformation(object):
    def __init__(self, fields, mapping, source, missing_value=None):
        self.source_index = fields.index(source)
        self.missing_value = missing_value
        self.mapping = mapping
    def __call__(self, row):
        return self.mapping.get(row[self.source_index], self.missing_value)

class FunctionTransformation(object):
    def __init__(self, fields, function, source, args, missing_value=None):
        self.function = function
        if isinstance(source, basestring):
            self.source_index = fields.index(source)
            self.mask = None
        else:
            self.source_index = None
            self.mask = fields.mask(source)

        self.args = args or {}
        self.missing_value = missing_value

    def __call__(self, row):
        if self.source_index:
            result = self.function(row[self.source_index], **self.args)
        else:
            args = list(itertools.compress(row, self.mask))
            result = self.function(*args, **self.args)

        return result or self.missing_value

def prepare_transformation(transformation, fields):
    """Returns an ordered dictionary of transformations where keys are target
    fields and values are transformation callables which accept a row of
    structure `fields` as an input argument."""
    out = OrderedDict()

    for t in transformation:
        target = t[0]
        if len(t) == 1:
            out[target] = CopyValueTransformation(fields, target)
            continue
        desc = t[1]
        # ("target", None) - no trasformation, just copy the field
        if desc is None:
            out[target] = CopyValueTransformation(fields, target)
            continue
        elif isinstance(desc, basestring):
            out[target] = CopyValueTransformation(fields, desc)
            continue

        action = desc.get("action")
        source = desc.get("source", target)
        if not action or action == "copy":
            out[target] = CopyValueTransformation(fields, source,
                                                    desc.get("missing_value"))
        elif action == "function":
            out[target] = FunctionTransformation(fields, desc.get("function"),
                                                 source,
                                                 desc.get("args"),
                                                 desc.get("missing_value"))
        elif action == "map":
            out[target] = MapTransformation(fields, desc.get("map"),
                                                 source,
                                                 desc.get("missing_value"))
        elif action == "set":
            out[target] = SetValueTransformation(desc.get("value"))

    return out

def create_transformer(transformation):
    return functools.partial(transform, transformation.values())

def transform(transformations, row):
    """Transforms `row` with structure `input_fields` according to
    transformations which is a list of Transformation objects."""

    out = [trans(row) for trans in transformations]
    return out

