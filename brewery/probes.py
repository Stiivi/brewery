"""Data probes"""

import common
import re
import datetime

__all__ = [
    "Probe",
    "MissingValuesProbe",
    "StatisticsProbe",
    "DistinctProbe",
    "StorageTypeProbe",
    "MultiProbe",
    "CompletenessProbe",
    "probe_types"
]

def probe_type(string, date_format="%Y-%m-%dT%H:%M:%S.Z"):
    """Guess one of basic types that the `string` might contain. Returns a
    string with basic type name. If `date_format` is ``None`` then string is
    not tested for date type. Default is ISO date format."""

    if string is None:
        return None

    try:
        int(string)
        return "integer"
    except ValueError:
        pass

    try:
        float(string)
        return "float"
    except ValueError:
        pass

    if date_format:
        try:
            datetime.datetime.strptime(string, date_format)
            return "date"
        except ValueError:
            pass

    return "string"


class Probe(object):
    def probe(self, value):
        """Probes single value"""
        raise NotImplementedError

    def probe_many(self, iterable):
        """Probes all values from `iterable`. Probes might implement more
        efficiet versions of this method. Default implementation iterates
        through `iterable` items and calls `probe()` on every value."""

        for value in iterable:
            self.probe(value)

class ProbeSet(object):
    """Collection of probes."""
    def __init__(self, probes=None):
        """Creates a probe set with `probes`."""
        if probes:
            self.probes = probes
        else:
            self.probes = []

    def probe(self, value):
        for probe in self.probes:
            probe.probe(value)


    def to_dict(self):
        d = {}
        for probe in self.probes:
            name = common.to_identifier(common.decamelize(probe.__class__.__name__))
            re.sub('_probe$', name, '')
            d[name] = probe.to_dict()

        return d

class MissingValuesProbe(object):
    """Data quality statistics for a dataset field

    :Attributes:
        * `count`: total count of null records
    """
    def __init__(self):
        self.count = 0

    def probe(self, value):
        """Probe the value.
        """

        if value is None:
            self.count += 1

    def to_dict(self):
        return {"count": self.count}

class CompletenessProbe(object):
    """Data quality statistics for a dataset field

    :Attributes:
        * `count`: total count of records
        * `unknown`: number of unknown records (NULL, None, nil, ...)
    """
    def __init__(self):
        self.count = 0
        self.unknown = 0

    def probe(self, value):
        """Probe the value.
        """
        self.count += 1
        if value is None:
            self.unknown += 1

    def to_dict(self):
        return {"count": self.count, "unknown": self.unknown}

class StatisticsProbe(object):
    """Data quality statistics for a dataset field

    :Attributes:
        * `min` - minimum value found
        * `max` - maxumum value found
        * `sum` - sum of values
        * `count` - count of values
        * `average` - average value
    """
    def __init__(self):
        self.min = None
        self.max = None
        self.sum = None
        self.count = 0
        self.fields = ["min", "max", "sum", "count"]

    @property
    def average(self):
        return self.sum / self.count

    def probe(self, value):
        self.count += 1
        if value is not None:
            if self.sum is None:
                self.sum = value
                self.min = value
                self.max = value
            else:
                self.sum += value
                self.min = min(self.min, value)
                self.max = max(self.max, value)

    def to_dict(self):
        return {"count": self.count, "min": self.min, "max": self.max,
                "sum": self.sum, "average": self.average }

class DistinctProbe(object):
    """Probe for distinct values."""
    def __init__(self, threshold = None):
        self.values = set([])
        self.overflow = False
        self.threshold = threshold
        self.fields = ["values", ("overflow", "integer")]

    def probe(self, value):
        self.overflow = self.threshold and len(self.values) >= self.threshold

        if not self.overflow:
            self.values.add(value)

class StorageTypeProbe(object):
    """Probe for guessing field data type

    Attributes:
        * field: name of a field which statistics are being presented
        * storage_types: found storage types
        * unique_storage_type: if there is only one storage type, then this is set to that type
    """
    def __init__(self):
        self.storage_types = set()

    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

    @property
    def unique_storage_type(self):
        """Return storage type if there is only one. This should always return a type in relational
        databases, but does not have to in databases such as MongoDB."""

        if len(self.storage_types) == 1:
            return list(self.storage_types)[0]
        else:
            return None

    def to_dict(self):
        d = {
            "storage_types": [str(st) for st in self.storage_types],
            "unique_storage_type": self.unique_storage_type
        }
        return d

class ValueTypeProbe(object):
    """Probe for guessing field value data type. It should be one of:
       `int`, `float`, ...

    Attributes:
        * field: name of a field which statistics are being presented
        * storage_types: found storage types
        * unique_storage_type: if there is only one storage type, then this is set to that type
    """
    def __init__(self):
        self.int_count = 0
        self.float_count = 0
        self.date_count = 0
        # ISO datetime "%Y-%m-%dT%H:%M:%S" )

    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

    @property
    def unique_storage_type(self):
        """Return storage type if there is only one. This should always return a type in relational
        databases, but does not have to in databases such as MongoDB."""

        if len(self.storage_types) == 1:
            return list(self.storage_types)[0]
        else:
            return None

    def to_dict(self):
        d = {
            "storage_types": [str(st) for st in self.storage_types],
            "unique_storage_type": self.unique_storage_type
        }
        return d

class BasicAuditProbe(Probe):
    """Basic Data quality statistics for a dataset field

    :Attributes:
        * `field`: name of a field for which statistics are being collected

        * `value_count`: number of records in which the field exist. In relationad database table this
          is equal to number of rows, in document based databse, such as MongoDB, it is number of
          documents that have a key present (being null or not)

        * `record_count`: total count of records in dataset. This should be set explicitly on
          finalisation. Seet :meth:`FieldStatistics.finalize`. In relational database this should be the
          same as `value_count`.

        * `value_ratio`: ratio of value count to record count, 1 for relational databases

        * `null_count`: number of records where field is null

        * `null_value_ratio`: ratio of records with nulls to total number of probed values =
          `null_value_ratio` / `value_count`

        * `null_record_ratio`: ratio of records with nulls to total number of records =
          `null_value_ratio` / `record_count`

        * `empty_string_count`: number of empty strings

        * `storage_types`: list of all encountered storage types (CSV, MongoDB, XLS might have different
          types within a field)

        * `unique_storage_type`: if there is only one storage type, then this is set to that type

        * `distict_values`: list of collected distinct values

        * `distinct_threshold`: number of distict values to collect, if count of distinct values is
          greather than threshold, collection is stopped and `distinct_overflow` will be set. Set to 0
          to get all values. Default is 10.
    """
    def __init__(self, key=None, distinct_threshold=10):
        self.field = key
        self.value_count = 0
        self.record_count = 0
        self.value_ratio = 0

        self.distinct_values = set()
        self.distinct_overflow = False
        self.storage_types = set()

        self.null_count = 0
        self.null_value_ratio = 0
        self.null_record_ratio = 0
        self.empty_string_count = 0

        self.distinct_threshold = distinct_threshold

        self.unique_storage_type = None

        self.probes = []

    def probe(self, value):
        """Probe the value:

        * increase found value count
        * identify storage type
        * probe for null and for empty string

        * probe distinct values: if their count is less than ``distinct_threshold``. If there are more
          distinct values than the ``distinct_threshold``, then distinct_overflow flag is set and list
          of distinct values will be empty

        """

        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        self.value_count += 1

        # FIXME: check for existence in field.empty_values
        if value is None:
            self.null_count += 1

        if value == '':
            self.empty_string_count += 1

        self._probe_distinct(value)

        for probe in self.probes:
            probe.probe(value)

    def _probe_distinct(self, value):
        """"""
        if self.distinct_overflow:
            return

        # We are not testing lists, dictionaries and object IDs
        storage_type = value.__class__

        if not self.distinct_threshold or self.distinct_threshold == 0 or len(self.distinct_values) < self.distinct_threshold:
            try:
                self.distinct_values.add(value)
            except:
                # FIXME: Should somehow handle invalid values that can not be added
                pass
        else:
            self.distinct_overflow = True

    def finalize(self, record_count = None):
        """Compute final statistics.

        :Parameters:
            * `record_count`: final number of records in probed dataset.
                See :meth:`FieldStatistics` for more information.
        """
        if record_count:
            self.record_count = record_count
        else:
            self.record_count = self.value_count

        if self.record_count:
            self.value_ratio = float(self.value_count) / float(self.record_count)
            self.null_record_ratio = float(self.null_count) / float(self.record_count)

        if self.value_count:
            self.null_value_ratio = float(self.null_count) / float(self.value_count)

        if len(self.storage_types) == 1:
            self.unique_storage_type = list(self.storage_types)[0]

    def dict(self):
        raise Exception("dict() is depreciated, use to_dict() instead")

    def to_dict(self):
        """Return dictionary representation of the receiver."""
        d = {
            "key": self.field,
            "value_count": self.value_count,
            "record_count": self.record_count,
            "value_ratio": self.value_ratio,
            "storage_types": list(self.storage_types),
            "null_count": self.null_count,
            "null_value_ratio": self.null_value_ratio,
            "null_record_ratio": self.null_record_ratio,
            "empty_string_count": self.empty_string_count,
            "unique_storage_type": self.unique_storage_type
        }

        if self.distinct_overflow:
            d["distinct_overflow"] = self.distinct_overflow,
            d["distinct_values"] = []
        else:
            d["distinct_values"] = list(self.distinct_values)

        return d

    def __repr__(self):
        return "FieldStatistics:%s" % (self.dict())

