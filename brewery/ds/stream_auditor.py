import base
import brewery.dq as dq

class StreamAuditor(base.DataTarget):
    """Target stream for auditing data values from stream. For more information about probed value
    properties, please refer to :class:`brewery.dq.FieldStatistics`"""
    def __init__(self, distinct_threshold = 10):
        super(StreamAuditor, self).__init__()

        self.record_count = 0
        self.stats = {}
        self.distinct_threshold = distinct_threshold
        self._field_names = None
        
    def initialize(self):
        self.record_count = 0

        # Cache field names into a variable (self.field_names is a getter function)
        self._field_names = self.field_names
        
        for field in self._field_names:
            stat = dq.FieldStatistics(field, distinct_threshold = self.distinct_threshold)
            self.stats[field] = stat

    def append(self, obj):
        """Probe row or record and update statistics."""
        self.record_count += 1
        
        if type(obj) == dict:
            self._probe_record(obj)
        else:
            self._probe_row(obj)
    
    def _probe_record(self, record):
        for field, value in record.items():
            if not field in self.stats:
                stat = brewery.dq.FieldStatistics(field, distinct_threshold = self.distinct_threshold)
                self.stats[field] = stat
            else:
                stat = self.stats[field]
            stat.probe(value)

    def _probe_row(self, row):
        if not self._field_names:
            raise ValueError("Fields are not initialized")
        for i, field in enumerate(self._field_names):
            value = row[i]
            stat = self.stats[field]
            stat.probe(value)

    def finalize(self):
        for key, stat in self.stats.items():
            stat.finalize(self.record_count)
            
    @property        
    def field_statistics(self):
        """Return field statistics as dictionary: keys are field names, values are 
        :class:`brewery.dq.FieldStatistics` objects"""
        return self.stats

