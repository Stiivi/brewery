"""Field statistics"""

import sets

try:
    import bson
except:
    pass

class FieldStatistics(object):
    """Data quality statistics for a dataset field
    
    Attributes:
        * field: name of a field which statistics are being presented
        * record_count: total count of records in dataset
        * value_count: number of records in which the field exist. In RDB table this is equal to record_count, in
            document based databse, such as MongoDB it is number of documents that have a key present (being null
            or not)
        * value_ratio: ratio of value count to record count, 1 for relational databases
        * null_count: number of records where field is null
        * null_value_ratio: ratio of records
        * unique_storage_type: if there is only one storage type, then this is set to that type
        * distinct_threshold: number of distict values to collect, if distinct values is greather than threshold,
            no more values are being collected and distinct_overflow will be set. Set to 0 to get all values.
            Default is 10.
    """    
    def __init__(self, key, distinct_threshold = 10):
        self.field = key
        self.value_count = 0
        self.record_count = 0
        self.value_ratio = 0
        
        self.distinct_values = sets.Set()
        self.distinct_overflow = False
        self.storage_types = sets.Set()

        self.null_count = 0
        self.null_value_ratio = 0
        self.null_record_ratio = 0
        self.empty_string_count = 0
        
        self.distinct_threshold = distinct_threshold
        
        self.unique_storage_type = None
        
    def probe(self, value):
        storage_type = value.__class__
        self.storage_types.add(storage_type.__name__)

        self.value_count += 1
        
        if value == None:
            self.null_count += 1
        if value == '':
            self.empty_string_count += 1

        self.probe_distinct(value)

    def probe_distinct(self, value):
        """Find distinct values, if theyr count is less than ``distinct_threshold``. If there are
        more distinct values than the ``distinct_threshold``, then distinct_overflow flag is set and
        list of distinct values will be empty"""
        if self.distinct_overflow:
            return

        # We are not testing lists, dictionaries and object IDs
        storage_type = value.__class__

        if self.distinct_threshold == 0 or len(self.distinct_values) < self.distinct_threshold:
            try:
                self.distinct_values.add(value)
            except:
                # FIXME: Should somehow handle invalid values that can not be added
                pass
        else:
            self.distinct_overflow = True
            
    def finalize(self):
        if self.record_count:
            self.value_ratio = float(self.value_count) / float(self.record_count)
            self.null_record_ratio = float(self.null_count) / float(self.record_count)

        if self.value_count:
            self.null_value_ratio = float(self.null_count) / float(self.value_count)

        if len(self.storage_types) == 1:
            self.unique_storage_type = list(self.storage_types)[0]

    def dict(self):
        d = {}
        d["key"]= self.field
        d["value_count"]= self.value_count
        d["record_count"]= self.record_count
        d["value_ratio"]= self.value_ratio
        if self.distinct_overflow:
            d["distinct_overflow"] = self.distinct_overflow,
            d["distinct_values"] = []
        else:
            d["distinct_values"] = list(self.distinct_values)

        d["storage_types"]= list(self.storage_types)

        d["null_count"] = self.null_count
        d["null_value_ratio"] = self.null_value_ratio
        d["null_record_ratio"] = self.null_record_ratio
        d["empty_string_count"] = self.empty_string_count

        d["unique_storage_type"] = self.unique_storage_type
        
        return d
    
