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

class StatisticsProbe(object):
    """Data quality statistics for a dataset field

    :Attributes:
        * `record_count`: total count of records in dataset. This should be set explicitly on
          finalisation. Seet :meth:`FieldStatistics.finalize`. In relational database this should be the
          same as `value_count`.

        * `fields`: 
        * `count`: number of non-null objects
    """    
    def __init__(self):
        self.min = None
        self.max = None
        self.sum = None
        self.count = 0
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

class DistinctProbe(object):
    def __init__(self, threshold = None):
        self.distinct_values = set([])
        self.overflow = False
        self.threshold = threshold

    def probe(self, value):
        if self.threshold and len(self.distinct_values) >= threshold:
            self.overflow = True
            return

        self.distinct_values.add(value)

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
    