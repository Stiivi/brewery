# -*- coding: utf-8 -*-
from .errors import *
from .common import get_logger, to_identifier, decamelize
from .metadata import *
from collections import OrderedDict

class Transformation(object):
    def __init__(self, source, fields):
        """Creates a transformation object for single `source`"""
        self.source = source
        self.fields = fields
        self.output = OrderedDict()

    def __enter__(self):
        self.target = TransformationTarget()
        self.source = TransformationSource(self.source, self.fields)
        return (self.target, self.source)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            return False
        else:
            self.output.update(self.target.output)
            # node = TransformationNode(transformation)
            print "--- transformation:\n%s" % (self.output, )
            print "=== transformation finished"
            # self.fork += node
            return True

    def compile_transformation(self, target):
        for key, value in target.output.items():
            out = self.compile_field(key, value)

    def compile_field(self, name, value):
        if isinstance(value, Field):
            action = "copy"
        if isinstance(value, int):
            pass
        else:
            # FIXME: check whether it is literal object
            action = "set"

class TransformationTarget(object):
    def __init__(self):
        self.output = OrderedDict()

    def __setitem__(self, name, value):
        if isinstance(value, TransformationSource):
            value = IdentityElement(value)
        elif not isinstance(value, TransformationElement):
            value = ValueElement(value)

        self.output[name] = value

class TransformationSource(object):
    def __init__(self, context, fields):
        self.context = context
        self.fields = fields

    def __getitem__(self, item):
        """
        target["foo"] = source["bar"]

        """
        if isinstance(item, basestring):
            return FieldElement(self, self.fields[item])
        elif isinstance(item, Field):
            return FieldElement(self, item)
        elif isinstance(item, list) or isinstance(item, tuple):
            fields = self.fields.fields(item)
            return FieldListElement(self, fields)
        else:
            raise BreweryError("Unknown source item %s" % (item, ))

    def same(self):
        return IdentityElement(self)

    def __repr__(self):
        return "TransformationSource(fields=%s)" % (self.fields.names(), )

class TransformationElement(object):
    def __init__(self, source):
        self.source = source

    def mapping(self, mapping):
        return MappingElement(self, mapping)

    def missing(self, value):
        return MissingValueElement(self, value)

    def function(self, function, *kwargs):
        return FunctionElement(self, function, kwargs)

    def value(self, value):
        return ValueElement(self, value)

class IdentityElement(TransformationElement):
    """Element representing identity mapping - same source field as intended
    target field."""
    def __repr__(self):
        return "IdentityElement()"

class FieldListElement(TransformationElement):
    def __init__(self, source, fields, missing_value=None):
        self.source = source
        self.fields = fields
        self.missing_value = missing_value

    def missing(self, value):
        return FieldListElement(self.source, self.fields, value)

    def __repr__(self):
        return "FieldListElement(%s, %s)" % \
                                (repr(self.source), repr(self.fields.names()))

class FieldElement(TransformationElement):
    def __init__(self, source, field, missing_value=None):
        self.source = source
        self.field = field
        self.missing_value = missing_value

    def missing(self, value):
        return FieldElement(self.source, self.field, value)

    def __repr__(self):
        if self.missing_value is None:
            return "FieldElement(%s, '%s')" % \
                                (repr(self.source), str(self.field))
        else:
            return "FieldElement(%s, '%s', %s)" % \
                                (repr(self.source), str(self.field),
                                        repr(self.missing_value))

class ValueElement(TransformationElement):
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "ValueElement(%s)" % repr(self.value)

class MissingValueElement(TransformationElement):
    def __init__(self, source, value):
        self.source = source
        self.value = value
    def __repr__(self):
        return "MissingValueElement(%s, %s)" % \
                                    (repr(self.source), repr(self.value))

class MappingElement(TransformationElement):
    def __init__(self, source, mapping, missing_value=None):
        self.source = source
        self.mapping = mapping
        self.missing_value = missing_value

    def __repr__(self):
        if self.missing_value is None:
            return "MappingElement(%s, %s)" % \
                                    (repr(self.source), repr(self.mapping))
        else:
            return "MappingElement(%s, %s, %s)" % \
                                    (repr(self.source),
                                     repr(self.mapping), repr(missing_value))
    def missing(self, value):
        return MappingElement(self.source, self.mapping, value)

class FunctionElement(TransformationElement):
    def __init__(self, source, function, *kwargs):
        self.source = source
        self.function = function
        self.kwargs = kwargs

    def __repr__(self):
        return "FunctionElement(%s, %s, %s)" % \
                                    (repr(self.source), repr(self.function),
                                            repr(self.kwargs))

class CompiledTransformation(object):
    def __init__(self, transformations):
        self.transformations = transformations

    def transform(self, row):
        out = [trans(row) for trans in self.transformations]
        return out

class TransformationFunction(object):
    def __init__(self, fields, source, missing_value=None):
        self.source_index = fields.index(source)
        self.missing_value = missing_value

class CopyValueTransformation(object):
    def __init__(self, fields, source, missing_value=None):
        self.source_index = fields.index(source)
        if issubclass(self.missing_value, TransformationFunction):
            self.is_concrete = False
        else:
            self.is_concrete = True
        self.missing_value = missing_value
    def __call__(self, row):
        if self.is_concrete:
            missing_value = self.missing_value
        else:
            missing_value = self.missing_value(row)

        return row[self.source_index] or missing_value

class SetValueTransformation(object):
    def __init__(self, value):
        self.value = value
    def __call__(self, row):
        if issubclass(self.value, TransformationFunction):
            return self.value(row)
        else:
            return self.value

class TransformationCompiler(object):
    def __init__(self, target, source):
        """Creates a transformation compiler.

        Attributes:

        * `source` – data object that serves as a source.
        """
        self.target = target
        self.source = source
        self.fields = source.fields

        self.field_names = [str(field) for field in self.fields]
        self.field_indexes = dict(zip(self.field_names,range(0,len(self.fields))))

        self.values = []
        self.functions = []
        self.mappings = []

    def compile(self):
        result = OrderedDict()

        for key, value in self.target.output.items():
            result[key] = self.compile_value(key, value)

    def compile_element(self, key, element):
            identifier = to_identifier(decamelize(element.__class__.__name__))
            method = getattr(self, "visit_%s" % identifier)
            return method(key, element)

    def visit_field_element(self, key, element):
        missing_value = self.compile_element(key, element)
        return element.field

    def visit_identity_element(self, key, element):
        return self.source_fields[key]
        
