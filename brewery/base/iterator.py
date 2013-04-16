# -*- coding: utf-8 -*-
"""Iterator composing operations."""
from ..metadata import *
from ..common import get_logger
from ..errors import *
from ..operations import operation
from ..objects import *
import itertools
import functools
from collections import OrderedDict, namedtuple

# FIXME: add cheaper version for already sorted data
# FIXME: BasicAuditProbe was removed

def iterator(func):
    """Wraps a function that provides an operation returning an iterator.
    Assumes same fields as first argument object"""
    @functools.wraps(func)
    def decorator(*args, **kwargs):
        fields = args[0].fields
        result = func(*args, **kwargs)
        return IterableDataSource(result, fields)

    return decorator

@operation("rows")
@iterator
def as_records(obj):
    """Returns iterator of dictionaries where keys are defined in fields."""

    names = [str(field) for field in obj.fields]
    for row in obj:
        yield dict(zip(names, row))

@operation("rows")
@iterator
def distinct(iterator, keys=None, is_sorted=False):
    """Return distinct `keys` from `iterator`. `iterator` does
    not have to be sorted. If iterator is sorted by the keys and `is_sorted`
    is ``True`` then more efficient version is used."""

    fields = iterator.fields
    if keys:
        row_filter = FieldFilter(keep=keys).row_filter(fields)
    else:
        row_filter = FieldFilter().row_filter(fields)

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

@operation("rows")
@iterator
def unique(iterator, keys, discard=False):
    """Return rows that are unique by `keys`. If `discard` is `True` then the
    action is reversed and duplicate rows are returned."""

    # FIXME: add is_sorted version

    row_filter = FieldFilter(keep=keys).row_filter(iterator.fields)

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

@operation("rows")
@iterator
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

@operation("rows")
@iterator
def select(iterator, predicate, arg_fields, discard=False, kwargs=None):
    """Returns an interator selecting fields where `predicate` is true.
    `predicate` should be a python callable. `arg_fields` are names of fields
    to be passed to the function (in that order). `kwargs` are additional key
    arguments to the predicate function."""
    fields = iterator.fields
    indexes = fields.indexes(arg_fields)
    row_filter = FieldFilter(keep=arg_fields).row_filter(fields)

    for row in iterator:
        values = [row[index] for index in indexes]
        flag = predicate(*values, **kwargs)
        if (flag and not discard) or (not flag and discard):
            yield row

@operation("rows")
@iterator
def select_from_set(iterator, field, values, discard=False):
    """Select rows where value of `field` belongs to the set of `values`. If
    `discard` is ``True`` then the matching rows are discarded instead
    (operation is inverted)."""

    fields = iterator.fields
    index = fields.index(field)

    # Convert the values to more efficient set
    values = set(values)

    if discard:
        predicate = lambda row: row[index] not in values
    else:
        predicate = lambda row: row[index] in values

    return itertools.ifilter(predicate, iterator)

@operation("records")
@iterator
def select_records(iterator, predicate, discard=False, kwargs=None):
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

@operation("rows")
@operation("records")
@iterator
def discard_nth(iterator, step):
    """Discards every step-th item from `iterator`"""
    print "ITERATOR: %s STEP %s" % (iterator, step)
    for i, value in enumerate(iterator):
        if i % step != 0:
            yield value

@operation("rows")
def field_filter(iterator, field_filter):
    """Filters fields in `iterator` according to the `field_filter`.
    `iterator` should be a rows iterator and `fields` is list of iterator's
    fields."""
    row_filter = field_filter.row_filter(iterator.fields)

    iterator = itertools.imap(row_filter, iterator)
    new_fields = field_filter.filter(iterator.fields)

    return IterableDataSource(iterator, new_fields)

def to_dict(iterator, fields, key=None):
    """Returns dictionary constructed from the iterator. `fields` are
    iterator's fields, `key` is name of a field or list of fields that will be
    used as a simple key or composite key.

    If no `key` is provided, then the first field is used as key.

    Keys are supposed to be unique.

    .. warning::

        This method consumes whole iterator. Might be very costly on large
        datasets.
    """

    if not key:
        index = 0
        indexes = None
    elif isinstance(key, basestring):
        index = fields.index(key)
        indexes = None
    else:
        indexes = fields.indexes(key)

    if indexes is None:
        d = dict( (row[index], row) for row in iterator)
    else:
        for row in iterator:
            print "ROW: %s" % (row, )
            key_value = (row[index] for index in indexes)
            d[key_value] = row

    return d

# FIXME: requires new fields
@operation("rows", "rows[]")
def left_inner_join(master, details, joins):
    """Creates left inner master-detail join (star schema) where `master` is an
    iterator if the "bigger" table `details` are details. `joins` is a list of
    tuples `(master, detail)` where the master is index of master key and
    detail is index of detail key to be matched.

    If `inner` is `True` then inner join is performed. That means that only
    rows from master that have corresponding details are returned.

    .. warning::

        all detail iterators are consumed and result is held in memory. Do not
        use for large datasets.
    """

    result = _left_inner_join_iterator(master, details, joins)

    out_fields = master.fields
    for detail in details:
        out_fields += detail.fields

    return IterableDataSource(result, out_fields)

def _left_inner_join_iterator(master, details, joins):
    """Simple iterator implementation of the left inner join"""

    maps = []

    if not details:
        raise ArgumentError("No details provided, nothing to join")

    if not joins:
        raise ArgumentError("No joins specified")

    if len(details) != len(joins):
        raise ArgumentError("For every detail there should be a join "
                            "(%d:%d)." % (len(details), len(joins)))

    djoins = []
    for detail, join in zip(details, joins):
        # FIXME: do not use list comprehension here for better error handling
        index = detail.fields.index(join[1])
        detail_dict = dict( (row[index], row) for row in detail )
        maps.append(detail_dict)

    for master_row in master:
        row = list(master_row)
        match = True

        for detail, join in zip(maps, joins):
            index = master.fields.index(join[0])
            key = master_row[index]
            try:
                detail_row = detail[key]
                row += detail_row
            except KeyError:
                match = False
                break

        if match:
            yield row

@operation("rows")
@iterator
def text_substitute(iterator, field, substitutions):
    """Substitute field using text substitutions"""
    # Compile patterns
    fields = iterator.fields
    substitutions = [(re.compile(patt), r) for (patt, r) in subsitutions]
    index = fields.index(field)
    for row in iterator:
        row = list(row)

        value = row[index]
        for (pattern, repl) in substitutions:
            value = re.sub(pattern, repl, value)
        row[index] = value

        yield row

@operation("rows")
@iterator
def string_strip(iterator, strip_fields=None, chars=None):
    """Strip characters from `strip_fields` in the iterator. If no
    `strip_fields` is provided, then it strips all `string` or `text` storage
    type objects."""

    fields = iterator.fields
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

@operation("rows")
@iterator
def basic_audit(iterable, distinct_threshold):
    """Performs basic audit of fields in `iterable`. Returns a list of
    dictionaries with keys:

    * `field_name` - name of a field
    * `record_count` - number of records
    * `null_count` - number of records with null value for the field
    * `null_record_ratio` - ratio of null count to number of records
    * `empty_string_count` - number of strings that are empty (for fields of type string)
    * `distinct_values` - number of distinct values (if less than distinct threshold). Set
      to None if there are more distinct values than `distinct_threshold`.
    """

    fields = iterable.fields

    stats = []
    for field in fields:
        stat = probes.BasicAuditProbe(field.name, distinct_threshold=distinct_threshold)
        stats.append(stat)

    for row in iterable:
        for i, value in enumerate(row):
            stats[i].probe(value)

    for stat in stats:
        stat.finalize()
        if stat.distinct_overflow:
            dist_count = None
        else:
            dist_count = len(stat.distinct_values)

        row = [ stat.field,
                stat.record_count,
                stat.null_count,
                stat.null_record_ratio,
                stat.empty_string_count,
                dist_count
              ]

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

