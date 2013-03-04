# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .base import Node
from ..objects import *
from ..dq.field_statistics import FieldStatistics
from ..metadata import *
import brewery.ops as ops
import logging
import itertools
import functools

__all__ = [
            "SampleNode",
            "AppendNode",
            "DistinctNode",
            "AggregateNode",
            "AuditNode",
            "SelectNode",
            "SetSelectNode",
            "SelectNode",
            "SelectRecordsNode",
            "AuditNode",
            # FIXME: depreciate this name
            "FunctionSelectNode",
        ]

class SampleNode(Node):
    """Create a data sample from input stream. There are more sampling possibilities:

    * fixed number of records
    * % of records, random *(not yet implemented)*
    * get each n-th record *(not yet implemented)*

    Node can work in two modes: pass sample to the output or discard sample and pass the rest.
    The mode is controlled through the `discard` flag. When it is false, then sample is passed
    and rest is discarded. When it is true, then sample is discarded and rest is passed.

    """

    node_info = {
        "label" : "Sample Node",
        "description" : "Pass data sample from input to output.",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "size",
                 "description": "Size of the sample to be passed to the output",
                 "type": "integer"
            },
            {
                "name": "discard",
                 "description": "flag whether the sample is discarded or included",
                 "default": "True"
            }
        ]
    }


    def __init__(self, size=1000, discard_sample=False, mode="first"):
        """Creates and initializes sample node

        Parameters:

        * `size` - number of records to be sampled
        * `discard_sample` - flag whether the sample is discarded or included.
          By default `False` - sample is included.
        * `mode` - sampling mode - ``first`` - get first N items, ``nth``
          - get one in n, ``random`` - get random %.

        Note: mode is not yet implemented.
        """
        super(SampleNode, self).__init__()
        self.size = size
        self.discard_sample = discard_sample
        self.mode = mode

    def initialize_fields(self,sources):
        fields = sources[0].fields.clone(origin=self, freeze=True)
        self.output_fields = fields

    def evaluate(self, context, sources):
        # FIXME: add other implementations, such as SQL
        rows = sources[0].rows()
        context.debug("sample %s, mode %s" % (self.size, self.mode))
        iterator = ops.iterator.sample(rows, self.size,
                                discard=self.discard_sample,mode=self.mode)
        context.debug("got iterator %s" % iterator)
        return IterableDataSource(iterator, sources[0].fields)

class AppendNode(Node):
    """Sequentialy append input streams. Concatenation order reflects input stream order. The
    input streams should have same set of fields."""
    node_info = {
        "label" : "Append",
        "description" : "Concatenate input streams.",
        "_array_status": "ported"
    }

    def __init__(self):
        """Creates a node that concatenates records from inputs. Order of input pipes matter."""
        super(AppendNode, self).__init__()
        self.output_fields = None

    def initialize_fields(self, sources):
        self.output_fields = sources[0].fields.clone(origin=self, freeze=true)

    def evaluate(self, context, sources):
        # 1. order keys in sources
        # 2. check for matching fields
        # TODO: add field
        result = None
        if "sql_statement" in shared_representations(sources):
            context.debug("trying to use SQL statement")
            first_source = sources[0]
            can_compose = True
            for source in sources[1:]:
                if not first_source.can_compose(first_source):
                    can_compose = False
                    break

            if can_compose:
                statements = [source.sql_statement() for source in sources]
                statement = ops.sql.append(statements)
                result = first_source.copy()
                result.statement = statement

        if not result:
            context.debug("appending using iterator")
            fields = sources[0].fields
            iterators = [source.rows() for source in sources.values()]
            iterator = ops.iterator.append(iterators)
            result = IterableDataSource(iterator, fields)

        return result

class MergeNode(Node):
    """Merge two or more streams (join).

    Inputs are joined in a star-like fashion: one input is considered master
    and others are details adding information to the master. By default master
    is the first input.  Joins are specified as list of tuples: (`input_tag`,
    `master_input_key`, `other_input_key`).

    Following configuration code shows how to add region and category details:

    .. code-block:: python

        node.keys = [ [1, "region_code", "code"],
                      [2, "category_code", "code"] ]

    Master input should have fields `region_code` and `category_code`, other inputs should have
    `code` field with respective values equal to master keys.

    .. code-block:: python

        node.keys = [ [1, "region_code", "code"],
                      [2, ("category_code", "year"), ("code", "year")] ]

    As a key you might use either name of a sigle field or list of fields for compound keys. If
    you use compound key, both keys should have same number of fields. For example, if there is
    categorisation based on year:

    The detail key might be omitted if it the same as in master input:

    .. code-block:: python

        node.keys = [ [1, "region_code"],
                      [2, "category_code"] ]

    Master input should have fields `region_code` and `category_code`, input #1 should have
    `region_code` field and input #2 should have `category_code` field.

    To filter-out fields you do not want in your output or to rename fields you can use `maps`. It
    should be a dictionary where keys are input tags and values are either
    :class:`FieldFilter` objects or dictionaries with keys ``rename`` and ``drop``.

    Following example renames ``source_region_name`` field in input 0 and drops field `id` in
    input 1:

    .. code-block:: python

        node.maps = {
                        0: FieldFilter(rename = {"source_region_name":"region_name"}),
                        1: FieldFilter(drop = ["id"])
                    }

    It is the same as:

    .. code-block:: python

        node.maps = {
                        0: { "rename" = {"source_region_name":"region_name"} },
                        1: { "drop" = ["id"] }
                    }

    The first option is preferred, the dicitonary based option is provided for convenience
    in cases nodes are being constructed from external description (such as JSON dictionary).

    .. note::

        Limitations of current implementation (might be improved in the future):

        * only inner join between datasets: that means that only those input records are joined
          that will have matching keys
        * "detail" datasets should have unique keys, otherwise the behaviour is undefined
        * master is considered as the largest dataset

    How does it work: all records from detail inputs are read first. Then records from master
    input are read and joined with cached input records. It is recommended that the master dataset
    set is the largest from all inputs.

    """

    node_info = {
        "label" : "Merge Node",
        "description" : "Merge two or more streams",
        "attributes" : [
            {
                "name": "joins",
                "description": "Join specification (see node documentation)"
            },
            {
                "name": "master",
                "description": "Tag (index) of input dataset which will be considered as master"
            },
            {
                "name": "maps",
                "description": "Specification of which fields are passed from input and how they are going to be (re)named"
            },
            {
                "name": "join_types",
                "description": "Dictionary where keys are stream tags (indexes) and values are "
                               "types of join for the stream. Default is 'inner'. "
                               "-- **Not implemented**"
            }
        ],
        "_array_status": "unported"
    }

    def __init__(self, joins = None, master = None, maps = None):
        super(MergeNode, self).__init__()
        if joins:
            self.joins = joins
        else:
            self.joins = []

        if master:
            self.master = master
        else:
            self.master = 0

        self.maps = maps

        self.output_fields = []

    def initialize(self):
        pass
        # Check joins and normalize them first
        self._keys = {}
        self._kindexes = {}

        self.master_input = self.inputs[self.master]
        self.detail_inputs = []
        for (tag, pipe) in enumerate(self.inputs):
            if pipe is not self.master_input:
                self.detail_inputs.append( (tag, pipe) )

        for join in self.joins:
            joinlen = len(join)
            if joinlen == 3:
                (detail_tag, master_key, detail_key) = join
            elif joinlen == 2:
                # We use same key names for detail as master if no detail key is specified
                (detail_tag, master_key) = join
                detail_key = master_key
            else:
                raise Exception("Join specification should be a tuple/list of two or three elements.")

            # Convert to tuple if it is just a string (as expected later)
            if not (type(detail_key) == list or type(detail_key) == tuple):
                detail_key = (detail_key, )
            if not (type(master_key) == list or type(master_key) == tuple):
                master_key = (master_key, )

            if detail_tag == self.master:
                raise Exception("Can not join master to itself.")

            self._keys[detail_tag] = (detail_key, master_key)

            detail_input = self.inputs[detail_tag]

            # Get field indexes
            detail_indexes = detail_input.fields.indexes(detail_key)
            master_indexes = self.master_input.fields.indexes(master_key)
            self._kindexes[detail_tag] = (detail_indexes, master_indexes)

        # Prepare storage for input data
        self._input_rows = {}
        for (tag, pipe) in enumerate(self.inputs):
            self._input_rows[tag] = {}

        # Create map filters

        self._filters = {}
        self._maps = {}
        if self.maps:
            for (tag, fmap) in self.maps.items():
                if type(fmap) == dict:
                    fmap = FieldFilter(rename = fmap.get("rename"), drop = fmap.get("drop"), keep=fmap.get("keep"))
                elif type(fmap) != FieldFilter:
                    raise Exception("Unknown field map type: %s" % type(fmap) )
                f = fmap.row_filter(self.inputs[tag].fields)
                self._maps[tag] = fmap
                self._filters[tag] = f

        # Construct output fields
        fields = FieldList()
        for (tag, pipe) in enumerate(self.inputs):
            fmap = self._maps.get(tag, None)
            if fmap:
                fields += fmap.map(pipe.fields)
            else:
                fields += pipe.fields

        self.output_fields = fields

    def evaluate():
        pass

    def join(self, details):
        """Only inner join is implemented"""
        # First, read details, then master. )
        for (tag, pipe) in self.detail_inputs:
            detail = self._input_rows[tag]

            key_indexes = self._kindexes[tag][0]
            self._read_input(tag, pipe, key_indexes, detail)

        rfilter = self._filters.get(self.master)

        for row in self.master_input.rows():
            if rfilter:
                joined_row = rfilter.filter(row[:])
            else:
                joined_row = row[:]

            joined = False
            for (tag, pipe) in self.detail_inputs:
                detail_data = self._input_rows[tag]

                # Create key from master
                key = []
                for i in self._kindexes[tag][1]:
                    key.append(row[i])
                key = tuple(key)

                detail = detail_data.get(tuple(key))

                if not detail:
                    joined = False
                    break
                else:
                    joined = True
                    joined_row += detail

            if joined:
                self.put(joined_row)

    def _read_input(self, tag, pipe, key_indexes, detail):
        rfilter = self._filters.get(tag)
        for row in pipe.rows():
            key = []
            for i in key_indexes:
                key.append(row[i])

            if rfilter:
                detail[tuple(key)] = rfilter.filter(row)
            else:
                detail[tuple(key)] = row

class DistinctNode(Node):
    """Node will pass distinct records with given distinct fields.

    If `discard` is ``False`` then first record with distinct keys is passed to the output. This is
    used to find all distinct key values.

    If `discard` is ``True`` then first record with distinct keys is discarded and all duplicate
    records with same key values are passed to the output. This mode is used to find duplicate
    records. For example: there should be only one invoice per organisation per month. Set
    `distinct_fields` to `organisaion` and `month`, sed `discard` to ``True``. Running this node
    should give no records on output if there are no duplicates.

    """
    node_info = {
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
        ],
        "_array_status": "unported"
    }

    def __init__(self, keys = None, discard = False):
        """Creates a node that will pass distinct records with given distinct fields.

        :Parameters:
            * `keys` - list of names of key fields
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
        if keys:
            self.keys = keys
        else:
            self.keys = []

        self.discard = discard

    def preview(self, context, sources):
        source_fields = sources[0].fields
        fields = source_fields.clone(origin=self, freeze=True)
        return fields

    def evaluate(self, context, sources):
        source = sources[0]
        if False and "sql_statement" in source.representations():
            # FIXME: enable this branch once SQL row distinct is implemented
            if self.discard:
                raise NotImplementedError("discard in distinct SQL is not implemented")
            statement = source.sql_statement
            statement = ops.sql.distinct_rows(statement, keys)
            context.debug("using SQL: %s" % str(statement))
            result = source.copy()
            result.statement = statement
        else:
            context.debug("using distinct iterator on keys %s" % (self.keys, ))
            iterator = ops.iterator.unique(source.rows(),
                                             fields=source.fields,
                                             keys=self.keys,
                                             discard=self.discard)
            result = IterableDataSource(iterator, source.fields)

        return result

class AggregateNode(Node):
    """Aggregate"""

    node_info = {
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
            },
            {
                "name": "measures",
                "description": "List of fields to be aggregated."
            }

        ]
    }

    def __init__(self, keys=None, measures=None, default_aggregations=None,
                 record_count_field="record_count"):
        """Creates a new node for aggregations. Supported aggregations: sum, avg, min, max"""

        super(AggregateNode, self).__init__()
        if default_aggregations is None:
            default_aggregations= ["sum"]
        if keys:
            self.keys = keys
        else:
            self.keys = []

        self.record_count_field = record_count_field
        self.measures = measures or []

    def add_measure(self, field, aggregation=None):
        """Add aggregation for `field` """
        # FIXME: depreciate this
        self.measures.append( (field, aggregation) )

    def output_fields(self, source):
        # FIXME: use storage types based on aggregated field type
        fields = FieldList()

        if self.keys:
            for field in source.fields.fields(self.keys):
                fields.append(field)

        distilled_measures = distill_aggregate_measures(self.measures)

        for measure, aggregate in distilled_measures:
            if isinstance(measure, basestring):
                name = measure
                storage_type = "float"
                concrete_storage_type = None
            else:
                name = measure.name
                storage_type = measure.storage_type
                concrete_storage_type = measure.concrete_storage_type

            aggregate_name = "%s_%s" % (name, aggregate)

            field = Field(aggregate_name, storage_type=storage_type,
                                concrete_storage_type=concrete_storage_type,
                                analytical_type="measure")

            fields.append(field)

        fields.append(Field(self.record_count_field, storage_type = "integer",
                                                analytical_type = "measure"))
        return fields

    def evaluate(self, context, sources):
        # FIXME: add SQL version
        source = sources[0]

        context.debug("aggregation keys: %s" % (self.keys, ))
        context.debug("source fields: %s" % (source.fields, ))
        distilled_measures = distill_aggregate_measures(self.measures)
        output_fields = self.output_fields(source)
        context.debug("aggregation result fields: %s" % (output_fields.names(), ) )
        iterator = ops.iterator.aggregate(source.rows(),
                                            fields=source.fields,
                                            key_fields=self.keys,
                                            measures=distilled_measures,
                                            include_count=True)
        obj = IterableDataSource(iterator, self.output_fields(source))

        return obj

class SelectRecordsNode(Node):
    """Select or discard records from the stream according to a predicate.

    The parameter names of the callable function should reflect names of the fields:

    .. code-block:: python

        def is_big_enough(i, **args):
            return i > 1000000

        node.condition = is_big_enough

    You can use ``**record`` to catch all or rest of the fields as dictionary:

    .. code-block:: python

        def is_big_enough(**record):
            return record["i"] > 1000000

        node.condition = is_big_enough


    The condition can be also a string with python expression where local variables are record field
    values:

    .. code-block:: python

        node.condition = "i > 1000000"

    """

    node_info = {
        "label" : "Select",
        "description" : "Select or discard records from the stream according to a predicate.",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "condition",
                 "description": "Callable that evaluates to a boolean value",
                 "type": "function"
            },
            {
                "name": "discard",
                 "description": "flag whether the records matching condition are discarded or included",
                 "default": "False",
                 "type":"flag"
            },
            {
                "name":"kwargs",
                "description": "Additional keywork arguments passed to the "
                                "predicate function. They are replaced by "
                                "record values if the keys and field names "
                                "are the same.",
                "type": "dict"
            },
        ],
        "source": {
                "representations": ["records", "rows"],
                "preferred_rep": "records"
            },
        "output": {
                "representation": "records"
            }
    }


    def __init__(self, condition=None, discard=False):
        """Creates and initializes selection node
        """
        super(SelectRecordsNode, self).__init__()
        self.condition = condition
        self.discard = discard
        self.kwargs = None

    def _eval_predicate(self, expression, **record):
        return eval(expression, None, record)

    def evaluate(self, context, sources):
        source = sources[0]
        if isinstance(self.condition, basestring):
            expression = compile(self.condition, "SelectNode condition", "eval")
            predicate = functools.partial(self._eval_predicate, expression)
        else:
            predicate = self.condition

        if "records" in source.representations():
            source_iterator = source.records()
        else:
            source_iterator = as_records(source.rows())
        iterator = ops.iterator.select_records(source_iterator, source.fields,
                                       predicate=predicate,
                                       discard=self.discard,
                                       kwargs=self.kwargs)
        obj = IterableRecordsDataSource(iterator, source.fields)
        return obj

class SelectNode(Node):
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

    node_info = {
        "label" : "Select",
        "description" : "Select records by a predicate function (python callable).",
        "output" : "same fields as input",
        "attributes" : [
            {
                 "name": "function",
                 "description": "Predicate function. Should be a callable object."
            },
            {
                 "name": "fields",
                 "description": "List of field names to be passed to the "
                                 "function (in that order)."
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

    def __init__(self, function=None, fields=None, discard=False, **kwargs):
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

    def evaluate(self, context, sources):
        source = sources[0]
        iterator = ops.iterator.select(source.rows(), source.fields,
                                       predicate=self.function,
                                       arg_fields=self.fields,
                                       discard=self.discard,
                                       kwargs=self.kwargs)
        obj = IterableDataSource(iterator, source.fields)
        return obj

FunctionSelectNode = SelectNode

class SetSelectNode(Node):
    """Select records where field value is from predefined set of values.

    Use case examples:

    * records from certain regions in `region` field
    * recprds where `quality` status field is `low` or `medium`

    """


    node_info = {
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

    def __init__(self, field=None, value_set=None, discard=False):
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

    def evaluate(self, context, sources):
        source = sources[0]
        if "sql_statement" in source.representations():
            # FIXME: implement this
            raise NotImplementedError
            statement = source.sql_statement()
            statement = ops.sql.select_from_set(statement,
                                                source.fields,
                                                self.field,
                                                self.value_set,
                                                self.discard)
            result = source.copy()
            result.statement = statement
        else:
            iterator = ops.iterator.select_from_set(source.rows(),
                                                source.fields,
                                                self.field,
                                                self.value_set,
                                                self.discard)
            result = IterableDataSource(iterator, source.fields)

        return result

class AuditNode(Node):
    """Node chcecks stream for empty strings, not filled values, number distinct values.

    Audit note passes following fields to the output:

        * `field_name` - name of a field from input
        * `record_count` - number of records
        * `null_count` - number of records with null value for the field
        * `null_record_ratio` - ratio of null count to number of records
        * `empty_string_count` - number of strings that are empty (for fields of type string)
        * `distinct_count` - number of distinct values (if less than distinct threshold). Set
          to None if there are more distinct values than `distinct_threshold`.
    """

    node_info = {
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
                               ("field_name", "string", "nominal"),
                               ("record_count", "integer", "measure"),
                               ("null_count", "float", "measure"),
                               ("null_record_ratio", "float", "measure"),
                               ("empty_string_count", "integer", "measure"),
                               ("distinct_count", "integer", "measure")
                               ]

        return FieldList(audit_record_fields)

    def evaluate(self, context, sources):
        source = sources[0]
        fields = self.output_fields()

        if "sql_statement" in source.representations():
            raise NotImplementedError
        else:
            stats = ops.iterator.basic_audit(iterable, fields)
            output = IterableDataSource(stats, fields)

        return output

