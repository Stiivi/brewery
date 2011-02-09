import base
import brewery.ds as ds

class SampleNode(base.Node):
    def __init__(self, sample_size = 1000, discard_sample = False, mode = None):
        """Creates and initializes sample node
        
        :Parameters:
            * `sample_size` - number of records to be sampled
            * `discard_sample` - flag whether the sample is discarded or included. By default `False` -
              sample is included.
            * `mode` - sampling mode - ``first`` - get first N items, ``nth`` - get one in n, ``random``
              - get random %. Note: mode is not yet implemented.
            """
        super(SampleNode, self).__init__()
        self.sample_size = sample_size
        self.discard_sample = discard_sample

    def run(self):
        pipe = self.input
        count = 0
        for row in pipe.rows():
            self.put(row)
            count += 1
            if count >= self.sample_size:
                pipe.stop()
                break

class AppendNode(base.Node):
    def __init__(self):
        """Creates a node that concatenates records from inputs. Order of input pipes matter."""
        super(AppendNode, self).__init__()

    @property
    def output_fields(self):
        if not self.inputs:
            raise ValueError("Can not get list of output fields: node has no input")

        return self.inputs[0].fields

    def run(self):
        """Append data objects from inputs sequentially."""
        for pipe in self.inputs:
            for row in pipe.rows():
                self.put(row)

class DistinctNode(base.Node):
    def __init__(self, distinct_fields = None, discard = False):
        """Creates a node that will pass distinct records with given distinct fields.
        
        :Parameters:
            * `distinct_fields` - list of names of key fields
            * `discard` - whether the distinct fields are discarded or kept. By default False.
            
        If `discard` is ``False`` then first record with distinct keys is passed to the output. This is
        used to find all distinct key values.
        
        If `discard` is ``True`` then first record with distinct keys is discarded and all duplicate
        records with same key values are passed to the output. This mode is used to find duplicate
        records. For example: there should be only one invoice per organisation per month. Set
        `distinct_fields` to `organisaion` and `month`, sed `discard` to ``True``. Running this node
        should give no records on output if there are no duplicates.
        
        """
        
        super(DistinctNode, self).__init__()
        if distinct_fields:
            self.distinct_fields = distinct_fields
        else:
            self.distinct_fields = []
            
        self.discard = discard
            
    def run(self):
        pipe = self.input
        self.distinct_values = set()

        # Just copy input to output if there are no distinct keys
        # FIXME: should issue a warning?
        if not self.distinct_fields:
            for row in pipe.rows():
                self.put(row)
            return

        for record in pipe.records():
            pass_flag = True
            # Construct key tuple from distinct fields
            key_tuple = []
            for field in self.distinct_fields:
                key_tuple.append(record.get(field))

            key_tuple = tuple(key_tuple)
            if key_tuple not in self.distinct_values:
                self.distinct_values.add(key_tuple)
                if not self.discard:
                    self.put(record)
            else:
                if self.discard:
                    # We already have one found record, which was discarded (because discard is true),
                    # now we pass duplicates
                    self.put(record)

class Aggregate(object):
    """Structure holding aggregate information (should be replaced by named tuples in Python 3)"""
    def __init__(self):
        self.count = 0
        self.sum = 0
        self.min = 0
        self.max = 0
        self.average = None
    
    def aggregate_value(self, value):
        self.count += 1
        self.sum += value
        self.min = min(self.min, value)
        self.max = max(self.max, value)
        
    def finalize(self):
        if self.count:
            self.average = self.sum / self.count
        else:
            self.average = None
class KeyAggregate(object):
    def __init__(self):
        self.count = 0
        self.field_aggregates = {}
        
class AggregateNode(base.Node):
    """docstring for AggregateNode"""
    def __init__(self, key_fields = None, default_aggregations = ["sum"], 
                 record_count_field = "record_count"):
        """Creates a new node for aggregations. Supported aggregations: sum, avg, min, max""" 
                
        super(AggregateNode, self).__init__()
        if key_fields:
            self.key_fields = key_fields
        else:
            self.key_fields = []
            
        self.aggregations = {}
        self.aggregated_fields = []
        self.record_count_field = record_count_field
            
    def add_aggregation(self, field, aggregations = None):
        self.aggregations[field] = aggregations
        self.aggregated_fields.append(field)
    

    @property
    def output_fields(self):
        # FIXME: use storage types based on aggregated field type
        fields = ds.FieldList()

        if self.key_fields:
            for field in  self.input_fields.fields(self.key_fields):
                fields.append(field)

        for field in self.aggregated_fields:
            fields.append(ds.Field(field + "_sum", storage_type = "float", analytical_type = "range"))
            fields.append(ds.Field(field + "_min", storage_type = "float", analytical_type = "range"))
            fields.append(ds.Field(field + "_max", storage_type = "float", analytical_type = "range"))
            fields.append(ds.Field(field + "_average", storage_type = "float", analytical_type = "range"))
        fields.append(ds.Field(self.record_count_field, storage_type = "integer", analytical_type = "range"))

        return fields
        
    def run(self):
        pipe = self.input
        self.aggregates = {}
        self.keys = []
        self.counts = {}
        
        key_indexes = self.input_fields.indexes(self.key_fields)
        value_indexes = self.input_fields.indexes(self.aggregated_fields)
        
        for row in pipe.rows():
            # Create aggregation key
            key = []
            for i in key_indexes:
                key.append(row[i])

            key = tuple(key)

            # Create new aggregate record for key if it does not exist
            #
            if key not in self.aggregates:
                self.keys.append(key)
                key_aggregate = KeyAggregate()
                self.aggregates[key] = key_aggregate
            else:
                key_aggregate = self.aggregates[key]

            # Create aggregations for each field to be aggregated
            #
            key_aggregate.count += 1
            for i in value_indexes:
                if i not in key_aggregate.field_aggregates:
                    aggregate = Aggregate()
                    key_aggregate.field_aggregates[i] = aggregate
                else:
                    aggregate = key_aggregate.field_aggregates[i]
                value = row[i]

                aggregate.aggregate_value(value)
            
        # Pass results to output
        for key in self.keys:
            row = []
            for key_value in key:
                row.append(key_value)

            key_aggregate = self.aggregates[key]
            for i in value_indexes:
                aggregate = key_aggregate.field_aggregates[i]
                aggregate.finalize()
                row.append(aggregate.sum)
                row.append(aggregate.min)
                row.append(aggregate.max)
                row.append(aggregate.average)

            row.append(key_aggregate.count)

            self.put(row)
