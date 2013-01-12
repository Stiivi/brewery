# -*- coding: utf-8 -*-

from errors import *
from graph import *
from .common import get_backend, get_logger
from .metadata import *
from collections import namedtuple
from .nodes.base import node_dictionary
import logging

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

        if __debug__:
            self.log.setLevel(logging.DEBUG)

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

        return _StreamFork(self)

    def initialize(self, nodes):
        """Initializes the `nodes`. If any node fails in initialization, then
        finalization is called for each already initialized node and exception is
        raised."""
        # FIXME: check for repeating fields (each node should generate its own)
        # FIXME: handle exception during node initialization (finalize)

        for node in nodes:
            self.log.debug("initializing node %s" % node)
            sources = self.node_sources(node)
            fieldlists = [source.output_fields for source in sources]

            node.initialize_fields(fieldlists)

    def finalize(self, nodes):
		failed = []
		for node in nodes:
			try:
				if hasattr(node, finalize):
					node.finalize()
			except Exception as e:
				failed.append( (node, e) )
		# FIXME: raise exception with list of all failed nodes

    def last_compatible_origin(self, field):
        """Returns origin that is a `Field` of compatible storage type.
        Returns `field` if no other origin is found."""

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

        nodes = self.sorted_nodes(nodes)

        # csv.0 -> meh.1
        # in meh, for 1
        # key: 1
        node_outputs = {}
        for node in nodes:
            self.logger.info("evaluating node %s" % node)
            conns = self.connections_with_target(node)
            self.logger.info("    number of connections: %d" % len(conns))
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
            result = node.evaluate(sources)
            if not isinstance(result, dict):
                outputs = {0:result}
            else:
                outputs = result

            self.logger.info("result: %s" % (outputs, ))
            node_outputs[node] = outputs

    def _run_arrayed(self, nodes=None):
        """Runs the stream"""

        """Notes:
            * run should take context as argument, where context is:
                * backend used
                * source tables
        """
        # FIXME: temporarily depreciated
        raise Exception("Temporarily depreciated")
        nodes = self.sorted_nodes(nodes)

        self.initialize(nodes)

        # Create node wrappers
        wrappers = []
        tables = {}

        for node in nodes:
            self.log.debug("wrapping node %s" % node)
            name = self.node_name(node)

            sources = []
            for inode in self.node_sources(node):
                table = tables[inode]
                if table is not None:
                    sources.append(table)
                else:
                    raise Exception("No table for node %s (%d)" % (inode,
                        len(tables)))

            fields = node.output_fields
            if fields:
                self.log.debug("constructing table with fields: %s" % (fields,))
                arrays = [self.get_array(field) for field in fields]
                table = self.backend.create_table(arrays, fields)
            else:
                self.log.debug("no table (is target)")
                table = None
            # print "--- storing table '%s' for %s" % (repr(table), node)
            tables[node] = table

            wrappers.append(NodeContext(node,name,sources,table))
        self.log.debug("total arrays created: %d" % len(self.field_arrays))
        self.log.info("running %d nodes" % len(wrappers))
        for wrapper in wrappers:
            self.log.info("running node %s (%s)" % (wrapper.name,
                                                        wrapper.node.identifier()))


            wrapper.node.run(sources=wrapper.sources,target=wrapper.target)
        self.log.info("finished running")
        self.finalize(nodes)


class _StreamFork(object):
    """docstring for StreamFork"""
    def __init__(self, stream, node=None):
        """Creates a stream fork - class for building streams."""
        super(_StreamFork, self).__init__()
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

    def set_name(self, name):
        """Sets name of current node."""
        self.stream.set_node_name(self.node, name)

    def fork(self):
        """Forks current fork. Returns a new fork with same actual node as the fork being
        forked."""
        fork = _StreamFork(self.stream, self.node)
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

    def append(self, obj):
        """Appends data from nodes using AppendNode"""
        raise NotImplementedError

    def __getattr__(self, name):
        """Returns node class"""
        # FIXME: use create_node here

        class_dict = node_dictionary()

        node_class = class_dict[name]

        constructor = _StreamForkConstructor(self, node_class)
        return constructor

class _StreamForkConstructor(object):
    """Helper class to append new node."""
    def __init__(self, fork, node_class):
        self.fork = fork
        self.node_class = node_class

    def __call__(self, *args, **kwargs):
        node = self.node_class(*args, **kwargs)
        self.fork += node
        return self.fork

