#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base
import re
import brewery
import brewery.ds as ds
from brewery.common import FieldError
import itertools

class FieldMapNode(base.Node):
    """Node renames input fields or drops them from the stream.
    """
    node_info = {
        "type": "field",
        "label" : "Field Map",
        "description" : "Rename or drop fields from the stream.",
        "attributes" : [
            {
                "name": "map_fields",
                "label": "Map fields",
                "description": "Dictionary of input to output field name."
            },
            {
                "name": "drop_fields",
                "label": "drop fields",
                "description": "List of fields to be dropped from the stream - incompatible with keep_fields."
            },
            {
                "name": "keep_fields",
                "label": "keep fields",
                "description": "List of fields to keep from the stream - incompatible with drop_fields."
            }
        ]
    }

    def __init__(self, map_fields = None, drop_fields = None, keep_fields=None):
        super(FieldMapNode, self).__init__()

        if drop_fields and keep_fields:
            raise FieldError('Invalid configuration of FieldMapNode: you cant specify both keep_fields and drop_fields.')

        if map_fields:
            self.mapped_fields = map_fields
        else:
            self.mapped_fields = {}

        if drop_fields:
            self.dropped_fields = set(drop_fields)
        else:
            self.dropped_fields = set([])

        if keep_fields:
            self.kept_fields = set(keep_fields)
        else:
            self.kept_fields = set([])
            
        self._output_fields = []
        
    def rename_field(self, source, target):
        """Change field name"""
        self.mapped_fields[source] = target
    
    def drop_field(self, field):
        """Do not pass field from source to target"""
        self.dropped_fields.add(field)

    @property
    def output_fields(self):
        return self._output_fields

    def initialize(self):
        self.map = brewery.FieldMap(rename=self.mapped_fields, drop=self.dropped_fields, keep=self.kept_fields)
        self._output_fields = self.map.map(self.input.fields)
        self.filter = self.map.row_filter(self.input.fields)

    def run(self):
        self.mapped_field_names = self.mapped_fields.keys()

        for row in self.input.rows():
            row = self.filter.filter(row)
            self.put(row)

class TextSubstituteNode(base.Node):
    """Substitute text in a field using regular expression."""
    
    node_info = {
        "type": "field",
        "label" : "Text Substitute",
        "description" : "Substitute text in a field using regular expression.",
        "attributes" : [
            {
                "name": "field",
                "label": "substituted field",
                "description": "Field containing a string or text value where substition will "
                               "be applied"
            },
            {
                "name": "derived_field",
                "label": "derived field",
                "description": "Field where substition result will be stored. If not set, then "
                               "original field will be replaced with new value."
            },
            {
                "name": "substitutions",
                "label": "substitutions",
                "description": "List of substitutions: each substition is a two-element tuple "
                               "(`pattern`, `replacement`) where `pattern` is a regular expression "
                               "that will be replaced using `replacement`"
            }
        ]
    }

    def __init__(self, field, derived_field = None):
        """Creates a node for text replacement.
        
        :Attributes:
            * `field`: field to be used for substitution (should contain a string)
            * `derived_field`: new field to be created after substitutions. If set to ``None`` then the
              source field will be replaced with new substituted value. Default is ``None`` - same field
              replacement.
        
        """
        super(TextSubstituteNode, self).__init__()

        self.field = field
        self.derived_field = derived_field
        self.substitutions = []
        
    def add_substitution(self, pattern, repl):
        """Add replacement rule for field.
        
        :Parameters:
            * `pattern` - regular expression to be searched
            * `replacement` - string to be used as replacement, default is empty string
        """

        self.substitutions.append( (re.compile(pattern), repl) )
    
    # FIXME: implement this
    # @property
    # def output_fields(self):
    #     pass
        
    def run(self):
        pipe = self.input

        if self.derived_field:
            append = True
        else:
            append = False

        index = self.input_fields.index(self.field)
            
        for row in pipe.rows():
            value = row[index]
            for (pattern, repl) in self.substitutions:
                value = re.sub(pattern, repl, value)
            if append:
                row.append(value)
            else:
                row[index] = value

            self.put(row)


class StringStripNode(base.Node):
    """Strip spaces (orother specified characters) from string fields."""

    node_info = {
        "type": "field",
        "icon": "string_strip_node",
        "label" : "String Strip",
        "description" : "Strip characters.",
        "attributes" : [
            {
                "name": "fields",
                "description": "List of string fields to be stripped. If none specified, then all "
                               "fields of storage type `string` are stripped"
            },
            {
                "name": "chars",
                "description": "Characters to be stripped. "
                               "By default all white-space characters are stripped."
            }
        ]
    }

    def __init__(self, fields = None, chars = None):
        """Creates a node for string stripping.

        :Attributes:
            * `fields`: fields to be stripped
            * `chars`: characters to be stripped

        """
        super(StringStripNode, self).__init__()

        self.fields = fields
        self.chars = chars

    def run(self):

        if self.fields:
            fields = self.fields
        else:
            fields = []
            for field in self.input.fields:
                if field.storage_type == "string" or field.storage_type == "text":
                    fields.append(field)

        indexes = self.input_fields.indexes(fields)

        for row in self.input.rows():
            for index in indexes:
                value = row[index]
                if value:
                    row[index] = value.strip(self.chars)

            self.put(row)

class CoalesceValueToTypeNode(base.Node):
    """Coalesce values of selected fields, or fields of given type to match the type.
    
    * `string`, `text`
        * Strip strings
        * if non-string, then it is converted to a unicode string
        * Change empty strings to empty (null) values
    * `float`, `integer`
        * If value is of string type, perform string cleansing first and then convert them to
          respective numbers or to null on failure

    """

    node_info = {
        "type": "field",
        "icon": "coalesce_value_to_type_node",
        "description" : "Coalesce Value to Type",
        "attributes" : [
            {
                "name": "fields",
                "description": "List of fields to be cleansed. If none given then all fields "
                               "of known storage type are cleansed"
            },
            {
                "name": "types",
                "description": "List of field types to be coalesced (if no fields given)"
            },
            {
                "name": "empty_values",
                "description": "dictionary of type -> value pairs to be set when field is "
                               "considered empty (null)"
            }
        ]
    }

    def __init__(self, fields = None, types = None, empty_values = None):
        super(CoalesceValueToTypeNode, self).__init__()
        self.fields = fields
        self.types = types

        if empty_values:
            self.empty_values = empty_values
        else:
            self.empty_values = {}
        
    def initialize(self):
        if self.fields:
            fields = self.fields
        else:
            fields = self.input.fields

        self.string_fields = [f for f in fields if f.storage_type == "string"]
        self.integer_fields = [f for f in fields if f.storage_type == "integer"]
        self.float_fields = [f for f in fields if f.storage_type == "float"]
        
        self.string_indexes = self.input.fields.indexes(self.string_fields)
        self.integer_indexes = self.input.fields.indexes(self.integer_fields)
        self.float_indexes = self.input.fields.indexes(self.float_fields)
        
        self.string_none = self.empty_values.get("string")
        self.integer_none = self.empty_values.get("integer")
        self.float_none = self.empty_values.get("float")
        
    def run(self):
        
        for row in self.input.rows():
            for i in self.string_indexes:
                value = row[i]
                if type(value) == str or type(value) == unicode:
                    value = value.strip()
                elif value:
                    value = unicode(value)
                    
                if value == "" or value is None:
                    value = self.string_none

                row[i] = value

            for i in self.integer_indexes:
                value = row[i]
                if type(value) == str or type(value) == unicode:
                    value = re.sub(r"\s", "", value.strip())

                try:
                    value = int(value)
                except:
                    value = self.integer_none

                row[i] = value

            for i in self.float_indexes:
                value = row[i]
                if type(value) == str or type(value) == unicode:
                    value = re.sub(r"\s", "", value.strip())

                try:
                    value = float(value)
                except:
                    value = self.float_none

                row[i] = value
        
            self.put(row)

class ValueThresholdNode(base.Node):
    """Create a field that will refer to a value bin based on threshold(s). Values of `range` type
    can be compared against one or two thresholds to get low/high or low/medium/high value bins.

    *Note: this node is not yet implemented*
    
    The result is stored in a separate field that will be constructed from source field name and
    prefix/suffix.
    
    For example:
        * amount < 100 is low
        * 100 <= amount <= 1000 is medium
        * amount > 1000 is high

    Generated field will be `amount_threshold` and will contain one of three possible values:
    `low`, `medium`, `hight`
    
    Another possible use case might be for binning after data audit: we want to measure null 
    record count and we set thresholds:
        
        * ratio < 5% is ok
        * 5% <= ratio <= 15% is fair
        * ratio > 15% is bad
        
    We set thresholds as ``(0.05, 0.15)`` and values to ``("ok", "fair", "bad")``
        
    """
    
    node_info = {
        "type": "field",
        "label" : "Value Threshold",
        "description" : "Bin values based on a threshold.",
        "attributes" : [
            {
                "name": "thresholds",
                "description": "List of fields of `range` type and threshold tuples "
                               "(field, low, high) or (field, low)"
            },
            {
                "name": "bin_names",
                "description": "Names of bins based on threshold. Default is low, medium, high"
            },
            {
                "name": "prefix",
                "description": "field prefix to be used, default is none."
            },
            {
                "name": "suffix",
                "description": "field suffix to be used, default is '_bin'"
            }
        ]
    }

    def __init__(self, thresholds = None, bin_names = None, prefix = None, suffix = None):
        self.thresholds = thresholds
        self.bin_names = bin_names
        self.prefix = prefix
        self.suffix = suffix
        self._output_fields = None
    
    @property
    def output_fields(self):
        return self._output_fields
    
    def initialize(self):
        field_names = [t[0] for t in self.thresholds]

        self._output_fields = brewery.FieldList()

        for field in self.input.fields:
            self._output_fields.append(field)

        if self.prefix:
            prefix = self.prefix
        else:
            prefix = ""
            
        if self.suffix:
            suffix = self.suffix
        else:
            suffix = "_bin"

        for name in field_names:
            field = brewery.Field(prefix + name + suffix)
            field.storage_type = "string"
            field.analytical_type = "set"
            self._output_fields.append(field)

        input_fields = self.input.fields

        # Check input fields
        for name in field_names:
            if not name in self.input.fields:
                raise FieldError("No input field with name %s" % name)
                
        self.threshold_field_indexes = self.input.fields.indexes(field_names)
        
    def run(self):
        thresholds = []
        for t in self.thresholds:
            if len(t) == 1:
                # We have only field name, then use default threshold: 0
                thresholds.append( (0, ) )
            elif len(t) == 2:
                thresholds.append( (t[1], ) )
            elif len(t) >= 2:
                thresholds.append( (t[1], t[2]) )
            elif len(t) == 0:
                raise ValueError("Invalid threshold specification: should be field name, low and optional high")
        
        if not self.bin_names:
            bin_names = ("low", "medium", "high")
        else:
            bin_names = self.bin_names
        
        for row in self.input.rows():
            for i, t in enumerate(thresholds):
                value = row[self.threshold_field_indexes[i]]
                bin = None
                if len(t) == 1:
                    if value < t[0]:
                        bin = bin_names[0]
                    else:
                        bin = bin_names[-1]
                elif len(t) > 1:
                    if value < t[0]:
                        bin = bin_names[0]
                    if value > t[1]:
                        bin = bin_names[-1]
                    else:
                        bin = bin_names[1]

                row.append(bin)
            self.put(row)

class DeriveNode(base.Node):
    """Dreive a new field from other fields using an expression or callable function.

    The parameter names of the callable function should reflect names of the fields:

    .. code-block:: python

        def get_half(i, **args):
            return i / 2

        node.formula = get_half

    You can use ``**record`` to catch all or rest of the fields as dictionary:

    .. code-block:: python

        def get_half(**record):
            return record["i"] / 2
            
        node.formula = get_half
        

    The formula can be also a string with python expression where local variables are record field
    values:

    .. code-block:: python

        node.formula = "i / 2"

    """

    node_info = {
        "label" : "Derive Node",
        "description" : "Derive a new field using an expression.",
        "attributes" : [
            {
                 "name": "field_name",
                 "description": "Derived field name",
                 "default": "new_field"
            },
            {
                 "name": "formula",
                 "description": "Callable or a string with python expression that will evaluate to " \
                                "new field value"
            },
            {
                "name": "analytical_type",
                 "description": "Analytical type of the new field",
                 "default": "unknown"
            },
            {
                "name": "storage_type",
                 "description": "Storage type of the new field",
                 "default": "unknown"
            }
        ]
    }


    def __init__(self, formula = None, field_name = "new_field", analytical_type = "unknown",
                        storage_type = "unknown"):
        """Creates and initializes selection node
        """
        super(DeriveNode, self).__init__()
        self.formula = formula
        self.field_name = field_name
        self.analytical_type = analytical_type
        self.storage_type = storage_type
        self._output_fields = None

    @property
    def output_fields(self):
        return self._output_fields

    def initialize(self):
        if isinstance(self.formula, basestring):
            self._expression = compile(self.formula, "SelectNode condition", "eval")
            self._formula_callable = self._eval_expression
        else:
            self._formula_callable = self.formula

        self._output_fields = brewery.FieldList()

        for field in self.input.fields:
            self._output_fields.append(field)

        new_field = brewery.Field(self.field_name, analytical_type = self.analytical_type, 
                                  storage_type = self.storage_type)
        self._output_fields.append(new_field)

    def _eval_expression(self, **record):
        return eval(self._expression, None, record)

    def run(self):
        for record in self.input.records():
            if self._formula_callable:
                record[self.field_name] = self._formula_callable(**record)
            else:
                record[self.field_name] = None

            self.put_record(record)

class BinningNode(base.Node):
    """Derive a bin/category field from a value.

    .. warning::
    
        Not yet implemented
    
    Binning modes:
    
    * fixed width (for example: by 100)
    * fixed number of fixed-width bins
    * n-tiles by count or by sum
    * record rank
    
        
    """
    
    node_info = {
        "type": "field",
        "label" : "Binning",
        "icon": "histogram_node",
        "description" : "Derive a field based on binned values (histogram)"
    }
           