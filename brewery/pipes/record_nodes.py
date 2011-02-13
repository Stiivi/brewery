import base
import brewery.ds as ds
import brewery.dq as dq
import logging

class SampleNode(base.Node):
    """Create a data sample from input stream. There are more sampling possibilities:
    
    * fixed number of records
    * % of records, random *(not yet implemented)*
    * get each n-th record *(not yet implemented)*
    
    Node can work in two modes: pass sample to the output or discard sample and pass the rest.
    The mode is controlled through the `discard` flag. When it is false, then sample is passed
    and rest is discarded. When it is true, then sample is discarded and rest is passed.
    
    """
    
    __node_info__ = {
        "label" : "Sample Node",
        "description" : "Pass data sample from input to output.",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "sample_size",
                 "description": "Size of the sample to be passed to the output"
            },
            {
                "name": "discard",
                 "description": "flag whether the sample is discarded or included",
                 "default": "True"
            }
        ]
    }
    

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
            logging.debug("sampling row %d" % count)
            self.put(row)
            count += 1
            if count >= self.sample_size:
                pipe.stop()
                break

class AppendNode(base.Node):
    """Sequentialy append input streams. Concatenation order reflects input stream order. The
    input streams should have same set of fields."""
    __node_info__ = {
        "label" : "Append",
        "description" : "Concatenate input streams."
    }

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

class MergeNode(base.Node):
    """Merge two or more streams (join)"""
    
    def __init__(self):
        super(MergeNode, self).__init__()
        

class DistinctNode(base.Node):
    """Node will pass distinct records with given distinct fields.
    
    If `discard` is ``False`` then first record with distinct keys is passed to the output. This is
    used to find all distinct key values.
    
    If `discard` is ``True`` then first record with distinct keys is discarded and all duplicate
    records with same key values are passed to the output. This mode is used to find duplicate
    records. For example: there should be only one invoice per organisation per month. Set
    `distinct_fields` to `organisaion` and `month`, sed `discard` to ``True``. Running this node
    should give no records on output if there are no duplicates.
    
    """
    __node_info__ = {
        "label" : "Distinct Node",
        "description" : "Pass only distinct records (discard duplicates) or pass only duplicates",
        "attributes" : [
            {
                "name": "distinct_fields",
                "label": "distinct fields",
                "description": "List of key fields that will be considered when comparing records"
            },
            {
                "name": "discard",
                "label": "derived field",
                "description": "Field where substition result will be stored. If not set, then "
                               "original field will be replaced with new value."
            }
        ]
    }

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
    """Aggregate"""
    
    __node_info__ = {
        "label" : "Aggregate Node",
        "description" : "Aggregate values grouping by key fields.",
        "output" : "Key fields followed by aggregations for each aggregated field. Last field is "
                   "record count.",
        "attributes" : [
            {
                 "name": "keys",
                 "description": "List of fields according to which records are grouped"
            },
            {
                "name": "record_count_field",
                 "description": "Name of a field where record count will be stored. "
                                "Default is `record_count`"
            }
        ]
    }
    
    def __init__(self, keys = None, default_aggregations = ["sum"], 
                 record_count_field = "record_count"):
        """Creates a new node for aggregations. Supported aggregations: sum, avg, min, max""" 
                
        super(AggregateNode, self).__init__()
        if keys:
            self.key_fields = keys
        else:
            self.key_fields = []
            
        self.aggregations = {}
        self.aggregated_fields = []
        self.record_count_field = record_count_field
            
    def add_aggregation(self, field, aggregations = None):
        """Add aggregation for `field` """
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

class SelectNode(base.Node):
    """Select records that will be selected by a predicate function.


    Example: configure a node that will select records where `amount` field is greater than 100
    
    .. code-block:: python

        def select_greater_than(value, threshold):
            return value > threshold

        node.function = select_greater_than
        node.fields = ["amount"]
        node.kwargs = {"threshold": 100}

    The `discard` flag controls behaviour of the node: if set to ``True``, then selection is
    inversed and fields that function evaluates as ``True`` are discarded. Default is False -
    selected records are passed to the output.
    """
    
    __node_info__ = {
        "label" : "Select",
        "description" : "Select records by a predicate function.",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "function",
                 "description": "Predicate function. Should be a callable object."
            },
            {
                 "name": "fields",
                 "description": "List of field names to be passed to the function."
            },
            {
                "name": "discard",
                 "description": "flag whether the selection is discarded or included",
                 "default": "True"
            },
            {
                 "name": "kwargs",
                 "description": "Keyword arguments passed to the predicate function"
            },
        ]
    }

    def __init__(self, function = None, fields = None, discard = False, **kwargs):
        """Creates a node that will select records based on condition `function`. 
        
        :Parameters:
            * `function`: callable object that returns either True or False
            * `fields`: list of fields passed to the function
            * `discard`: if ``True``, then selection is inversed and fields that function
              evaluates as ``True`` are discarded. Default is False - selected records are passed
              to the output.
            * `kwargs`: additional arguments passed to the function
        
        """
        super(SelectNode, self).__init__()
        self.function = function
        self.fields = fields
        self.discard = discard
        self.kwargs = kwargs
    
    def initialize(self):
        self.indexes = self.input_fields.indexes(self.fields)
    
    def run(self):
        for row in self.input.rows():
            values = [row[index] for index in self.indexes]
            flag = self.function(*values, **self.kwargs)
            if (flag and not self.discard) or (not flag and self.discard):
                self.put(row)

class SetSelectNode(base.Node):
    """Select records where field value is from predefined set of values.
    
    Use case examples:
    
    * records from certain regions in `region` field
    * recprds where `quality` status field is `low` or `medium`
    
    """
    
    
    __node_info__ = {
        "label" : "Set Select",
        "description" : "Select records by a predicate function.",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "field",
                 "description": "Field to be tested."
            },
            {
                 "name": "value_set",
                 "description": "set of values that will be used for record selection"
            },
            {
                "name": "discard",
                 "description": "flag whether the selection is discarded or included",
                 "default": "True"
            }
        ]
    }

    def __init__(self, field = None, value_set = None, discard = False):
        """Creates a node that will select records where `field` contains value from `value_set`.

        :Parameters:
            * `field`: field to be tested
            * `value_set`: set of values that will be used for record selection
            * `discard`: if ``True``, then selection is inversed and records that function
              evaluates as ``True`` are discarded. Default is False - selected records are passed
              to the output.

        """
        super(SetSelectNode, self).__init__()
        self.field = field
        self.value_set = value_set
        self.discard = discard

    def initialize(self):
        self.field_index = self.input_fields.index(self.field)

    def run(self):
        for row in self.input.rows():
            flag = row[self.field_index] in self.value_set
            if (flag and not self.discard) or (not flag and self.discard):
                self.put(row)

class AuditNode(base.Node):
    """Node chcecks stream for empty strings, not filled values, number distinct values.
    
    Audit note passes following fields to the output:
    
        * `field_name` - name of a field from input
        * `record_count` - number of records
        * `null_count` - number of records with null value for the field
        * `null_record_ratio` - ratio of null count to number of records
        * `empty_string_count` - number of strings that are empty (for fields of type string)
        * `distinct_values` - number of distinct values (if less than distinct threshold). Set
          to None if there are more distinct values than `distinct_threshold`.
    """
    
    __node_info__ = {
        "icon" : "data_audit_node",
        "label" : "Data Audit",
        "description" : "Perform basic data audit.",
        "attributes" : [
            {
                "name": "distinct_threshold",
                "label": "distinct threshold",
                "description": "number of distinct values to be tested. If there are more "
                               "than the threshold, then values are not included any more "
                               "and result `distinct_values` is set to None "
            }
        ]
    }

    def __init__(self, distinct_threshold = 10):
        """Creates a field audit node.
        
        :Attributes:
            * `distinct_threshold` - number of distinct values to be tested. If there are more
            than the threshold, then values are not included any more and result `distinct_values`
            is set to None
        
        Audit note passes following fields to the output:
        
            * field_name - name of a field from input
            * record_count - number of records
            * null_count - number of records with null value for the field
            * null_record_ratio - ratio of null count to number of records
            * empty_string_count - number of strings that are empty (for fields of type string)
            * distinct_values - number of distinct values (if less than distinct threshold). Set
              to None if there are more distinct values than `distinct_threshold`.
        
        """
        super(AuditNode, self).__init__()
        self.distinct_threshold = distinct_threshold
        
    def output_fields(self):

        audit_record_fields = [
                               ("field_name", "string", "typeless"),
                               ("record_count", "integer", "range"),
                               ("null_count", "float", "range"),
                               ("null_record_ratio", "float", "range"),
                               ("empty_string_count", "integer", "range"),
                               ("distinct_values", "integer", "range")
                               ]
                               
        fields = ds.FieldList(audit_record_fields)
        return fields

    def initialize(self):
        self.stats = []
        for field in self.input_fields:
            stat = dq.FieldStatistics(field.name, distinct_threshold = self.distinct_threshold)
            self.stats.append(stat)
        
    def run(self):
        for row in self.input.rows():
            for i, value in enumerate(row):
                self.stats[i].probe(value)
        
        for stat in self.stats:
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
            self.put(row)

