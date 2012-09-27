# -*- coding: utf-8 -*-

from errors import *
from graph import *
from .utils import get_logger
from .metadata import *
from collections import namedtuple
from .common import get_backend

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
        self.logger = get_logger()

        if backend:
            self.backend = get_backend(backend)
        else:
            self.backend = get_backend("default")

    def initialize(self, nodes):
        """Initializes the `nodes`. If any node fails in initialization, then
        finalization is called for each already initialized node and exception is
        raised."""
        # FIXME: check for repeating fields (each node should generate its own)
        # FIXME: handle exception during node initialization (finalize)

        for node in nodes:
            self.logger.debug("creating pipes for node %s" % node)
            print "--> initializing node %s" % node
            sources = self.node_sources(node)
            fieldlists = [source.output_fields for source in sources]
            self.logger.debug("initializing node of type %s" % node.__class__)
            node.initialize_fields(fieldlists)
            node.initialize()
            # FIXME: remove ^^

			# raise exception with failed finalizations and failed initializations

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
        origin = self.last_compatible_origin(field)
        if not origin:
            raise FieldOriginError("Unable to find origin of field %s" % field)

        if not origin.is_frozen:
            raise MetadataError("Field %s is not frozen" % field)

        array = self.field_arrays.get(origin)
        if not array:
            array = self.backend.create_array(field)
            self.field_arrays[field] = array

        return array

    def run(self, nodes=None):
        nodes = self.sorted_nodes(nodes)

        self.initialize(nodes)

        # Create node wrappers
        wrappers = []
        tables = {}

        for node in nodes:
            print "=== wrapping node %s" % node
            name = self.node_name(node)

            sources = []
            for inode in self.node_sources(node):
                table = tables[inode]
                if table is not None:
                    sources.append(table)
                else:
                    raise Exception("No table for node %s (%d)" % (inode,
                        len(tables)))
            print "--- sources: %d" % len(sources)
            fields = node.output_fields
            if fields:
                print "--- constructing table with fields: %s" % (fields, )
                arrays = [self.get_array(field) for field in fields]
                table = self.backend.create_table(arrays, fields)
            else:
                print "--- no table (is target)"
                table = None
            # print "--- storing table '%s' for %s" % (repr(table), node)
            tables[node] = table

            wrappers.append(NodeContext(node,name,sources,table))

        for wrapper in wrappers:
            print ("=== Running node %s (%s)" % (wrapper.name,
                                                        wrapper.node))


            wrapper.node.run(sources=wrapper.sources,target=wrapper.target)
            # print "RESULT:"
            # print "========================="
            # print wrapper.target
            # print "========================="

        self.finalize(nodes)


