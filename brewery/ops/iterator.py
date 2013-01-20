# -*- coding: utf-8 -*-
"""Iterator based operations."""
from ..metadata import *
from ..common import get_logger
import itertools
import functools
from collections import OrderedDict, namedtuple

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

def select(iterator, fields, predicate, arg_fields, discard=False, kwargs=None):
    """Returns an interator selecting fields where `predicate` is true.
    `predicate` should be a python callable. `arg_fields` are names of fields
    to be passed to the function (in that order). `kwargs` are additional key
    arguments to the predicate function."""
    indexes = fields.indexes(arg_fields)
    row_filter = FieldFilter(keep=arg_fields).row_filter(fields)

    for row in iterator:
        values = [row[index] for index in indexes]
        flag = predicate(*values, **kwargs)
        if (flag and not discard) or (not flag and discard):
            yield row

def select_records(iterator, fields, predicate, discard=False, kwargs=None):
    """Returns an interator selecting fields where `predicate` is true.
    `predicate` should be a python callable. `arg_fields` are names of fields
    to be passed to the function (in that order). `kwargs` are additional key
    arguments to the predicate function."""

    for record in iterator:
        if kwargs:
            record = dict(kwargs).update(record)
        flag = predicate(**record)
        if (flag and not discard) or (not flag and discard):
            yield record

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

###
# Simple and naive aggregation in Python

def agg_sum(a, value):
    return a+value

def agg_average(a, value):
    return (a[0]+1, a[1]+value)

def agg_average_finalize(a):
    return a[1]/a[0]

AggregationFunction = namedtuple("AggregationFunction",
                            ["func", "start", "finalize"])
aggregation_functions = {
            "sum": AggregationFunction(agg_sum, 0, None),
            "min": AggregationFunction(min, 0, None),
            "max": AggregationFunction(max, 0, None),
            "average": AggregationFunction(agg_average, (0,0), agg_average_finalize)
        }

def aggregate(iterator, fields, key_fields, measures, include_count=True):
    """Aggregates measure fields in `iterator` by `keys`. `fields` is a field
    list of the iterator, `keys` is a list of fields that will be used as
    keys. `aggregations` is a list of measures to be aggregated.

    `measures` should be a list of tuples in form (`measure`, `aggregate`).
    See `distill_measure_aggregates()` for how to convert from arbitrary list
    of measures into this form.

    Output of this iterator is an iterator that yields rows with fields that
    contain: key fields, measures (as specified in the measures list) and
    optional record count if `include_count` is ``True`` (default).

    Result is not ordered even the input was ordered.

    .. note:

        This is naïve, pure Python implementation of aggregation. Might not
        fit your expectations in regards of speed and memory consumption for
        large datasets.
    """

    # TODO: create sorted version
    # TODO: include SQL style COUNT(field) to count non-NULL values

    measure_fields = set()
    measure_aggregates = []
    for measure in measures:
        if isinstance(measure, basestring) or isinstance(measure, Field):
            field = str(measure)
            index = fields.index(field)
            measure_aggregates.append( (field, index, "sum") )
        else:
            field = measure[0]
            index = fields.index(field)
            measure_aggregates.append( (field, index, measure[1]) )

        measure_fields.add(field)


    if key_fields:
        key_selectors = fields.mask(key_fields)
    else:
        key_selectors = []

    keys = set()

    # key -> list of aggregates
    aggregates = {}

    for row in iterator:
        # Create aggregation key
        key = tuple(itertools.compress(row, key_selectors))

        # Create new aggregate record for key if it does not exist
        #
        try:
            key_aggregate = aggregates[key]
        except KeyError:
            keys.add(key)
            key_aggregate = []
            for measure, index, function in measure_aggregates:
                start = aggregation_functions[function].start
                key_aggregate.append(start)
            if include_count:
                key_aggregate.append(0)

            aggregates[key] = key_aggregate

        for i, (measure, index, function) in enumerate(measure_aggregates):
            func = aggregation_functions[function].func
            key_aggregate[i] = func(key_aggregate[i], row[index])

        if include_count:
            key_aggregate[-1] += 1

    # Pass results to output
    for key in keys:
        row = list(key[:])

        key_aggregate = aggregates[key]
        for i, (measure, index, function) in enumerate(measure_aggregates):
            aggregate = key_aggregate[i]
            finalize = aggregation_functions[function].finalize
            if finalize:
                row.append(finalize(aggregate))
            else:
                row.append(aggregate)

        if include_count:
            row.append(key_aggregate[-1])

        yield row

