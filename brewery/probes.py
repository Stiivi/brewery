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

class MultiProbe(object):
    """Probe with multiple probes"""
    def __init__(self, probes = None):
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
