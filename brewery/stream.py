# -*- coding: utf-8 -*-

from .errors import *
from .graph import *
from .common import get_backend, get_logger
from .metadata import *
from .nodes.base import node_dictionary
from collections import namedtuple

__all__ = [
        "Stream"
        ]

# Sources - list of FieldLists
# target - 
NodeContext = namedtuple("NodeContext",
                         ["node", "name", "sources", "target"])

class Stream(Graph):

    def __init__(self, nodes=None, connections=None, backend=None):
        super(Stream, self).__init__(nodes, connections)

        # FIXME: use factory
        self.field_arrays = {}
        self.log = get_logger()

        if backend:
            self.backend = get_backend(backend)
        else:
            self.backend = get_backend("default")

    def fork(self):
        """Creates a construction fork of the stream. Used for constructing streams in functional
        fashion. Example::

            stream = Stream()

            fork = stream.fork()
            fork.csv_source("fork.csv")
            fork.formatted_printer()

            stream.run()

        Fork responds to node names as functions. The function arguments are the same as node
        constructor (__init__ method) arguments. Each call will append new node to the fork and
        will connect the new node to the previous node in the fork.

        To configure current node you can use ``fork.node``, like::

            fork.csv_source("fork.csv")
            fork.node.read_header = True

        To set actual node name use ``set_name()``::

            fork.csv_source("fork.csv")
            fork.set_name("source")

            ...

            source_node = stream.node("source")

        To fork a fork, just call ``fork()``
        """
        return StreamFork(self)

    def initialize(self, nodes):
        """Initializes the `nodes`. Does nothing – obsolete.
        """
        pass


    def finalize(self, nodes):
        """Allows nodes to clean-up after execution or exception."""
        failed = []
        for node in nodes:
            try:
                if hasattr(node, finalize):
                    node.finalize()
            except Exception as e:
                failed.append( (node, e) )
		# FIXME: raise exception with list of all failed nodes

    def _last_compatible_origin(self, field):
        """Returns origin that is a `Field` of compatible storage type.
        Returns `field` if no other origin is found."""
        # FIXME: currently unused, keeping here for future use
        visited = set()
        last_field = field

        while field:
            if not isinstance(field.origin, Field):
                return field

            if field.storage_type != field.origin.storage_type or \
                    field.concrete_storage_type != \
                        field.origin.concrete_storage_type:
                return field

            field = field.origin

            if field in visited:
                raise FieldOriginError("Circular field origin %s" % field)

            visited.add(field)

        return last_field

    def get_array(self, field):
        """Gets an array for `field`. If no array for field exists, then new
        one is created using array backend."""
        # FIXME: currently unused, keeping here for future use

        origin = self.last_compatible_origin(field)
        if not origin:
            raise FieldOriginError("Unable to find origin of field %s" % field)

        if not origin.is_frozen:
            raise MetadataError("Field %s is not frozen" % field)

        array = self.field_arrays.get(origin)

        if array is None:
            self.log.debug("creating array for field '%s'" % (field,))
            array = self.backend.create_array(field)
            self.field_arrays[field] = array

        return array

    def run(self, nodes=None):
        """Runs the stream"""

        # prepare(inputs)
        # where inputs is list of (fields, representations)

        node_outputs = {}
        nodes = self.sorted_nodes(nodes)
        # FIXME: use some context object instead
        context = ExecutionContext()

        for node in nodes:
            self.logger.info("evaluating node %s" % node)
            conns = self.connections_with_target(node)
            self.logger.debug("    number of connections: %d" % len(conns))
            sources = {}
            for i, c in enumerate(conns):
                source_key = i if c.source_outlet is None else c.source_outlet
                target_key = i if c.target_outlet is None else c.target_outlet

                try:
                    out = node_outputs[c.source][source_key]
                except KeyError:
                    raise BreweryError("Unknown outlet '%s' in node %s" %
                                            (key, c.source))
                sources[target_key] = out

            result = node.evaluate(context, sources)

            if not isinstance(result, dict):
                outputs = {0:result}
            else:
                outputs = result

            self.logger.debug("result: %s" % (outputs, ))
            node_outputs[node] = outputs

class ExecutionContext(object):
    def __init__(self):
        self.logger = get_logger()
        self.actions = []

    def action(self, node, action, options):
        self.actions.append( (node, action, options) )

    def error(self, *args, **kwargs):
        self.logger.error(*args, **kwargs)
    def warn(self, *args, **kwargs):
        self.logger.warn(*args, **kwargs)
    def info(self, *args, **kwargs):
        self.logger.info(*args, **kwargs)
    def debug(self, *args, **kwargs):
        self.logger.debug(*args, **kwargs)


class StreamFork(object):
    """docstring for StreamFork"""
    def __init__(self, stream, node=None):
        """Creates a stream fork - class for building streams."""
        super(StreamFork, self).__init__()
        self.stream = stream
        self.node = node

    def __iadd__(self, node):
        """Appends a node to the actual stream. The new node becomes actual node of the
        for."""

        self.stream.add(node)
        if self.node:
            self.stream.connect(self.node, node)
        self.node = node

        return self

    def source(self, obj):
        """Create a generic source node with data object `obj`. Assusres
        source version of the object by calling `as_source()`"""
        # TODO: if there is specific node class that can wrap the source, then
        # use it
        obj = obj.as_source()
        self += DataObjectSource(obj)
        return self

    def target(self, store, name, **ops):
        """Create a generic target node with data object `obj`. Assures target
        version of the object by calling `as_target()`"""
        # TODO: if there is specific node class that can wrap the target, then
        # use it
        obj = obj.as_target()
        self += DataObjectTarget(obj)
        return self

    def set_name(self, name):
        """Sets name of current node."""
        self.stream.set_node_name(self.node, name)

    def fork(self):
        """Forks current fork. Returns a new fork with same actual node as the fork being
        forked."""
        fork = StreamFork(self.stream, self.node)
        return fork

    def merge(self, obj, **kwargs):
        """Joins two streams using the MergeNode (please refer to the node documentaton
        for more information).

        `obj` is a fork or a node to be merged. `kwargs` are MergeNode configuration arguments,
        such as `joins`.

        """
        raise NotImplementedError
        # if type(obj) == StreamFork:
        #     node = obj.node
        # else:
        #     node = obj
        #
        # self.stream.append(node)
        #
        # merge = MergeNode(**kwargs)
        # self.stream.append(merge)
        # self.stream.connect()

    def transform(self):
        """Provides transformation context. """
        return TransformationBuilder(self, self.node)

    def append(self, obj):
        """Appends data from nodes using AppendNode"""
        raise NotImplementedError

    def __getattr__(self, name):
        """Returns node class"""
        # FIXME: use create_node here
        class_dict = node_dictionary()

        try:
            node_class = class_dict[name]
        except KeyError:
            raise StreamError("Node of type %s does not exist" % name)

        builder = StreamForkBuilder(self, node_class)
        return builder

class StreamForkBuilder(object):
    """Helper class to append new node."""
    def __init__(self, fork, node_class):
        self.fork = fork
        self.node_class = node_class

    def __call__(self, *args, **kwargs):
        node = self.node_class(*args, **kwargs)
        self.fork += node
        return self.fork


class TransformationContext(object):
    def __init__(self, fork, node, fields):
        self.node = node
        self.fork = fork
        self.fields = fields

    def __enter__(self):
        return (TransformationTarget(context, fields),
                TransformationSource(context, fields))

    def __exit__(exc_type, exc_val, exc_tb):
        if exc_type:
            return False
        else:
            return True
