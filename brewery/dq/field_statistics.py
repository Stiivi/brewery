#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Field statistics"""

import sets

class FieldStatistics(object):
    """Data quality statistics for a dataset field
    
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
    def __init__(self, key = None, distinct_threshold = 10):
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
        """Return dictionary representation of receiver."""
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
    
    def __repr__(self):
        return "FieldStatistics:%s" % (self.dict())

