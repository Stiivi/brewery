from table import PythonTable
from errors import *
from graph import *
from .utils import get_logger
from .metadata import *

__all__ = [
        "Stream"
        ]

class Stream(Graph):

    def __init__(self, nodes=None, connections=None):
        super(Stream, self).__init__(nodes, connections)

        # FIXME: use factory
        self.field_arrays = {}
        self.logger = get_logger()



    def initialize(self, nodes):
        """Initializes the `nodes`. If any node fails in initialization, then
        finalization is called for each already initialized node and exception is
        raised."""
        # FIXME: check for repeating fields (each node should generate its own)
        # FIXME: handle exception during node initialization (finalize)

        for node in nodes:
            self.logger.debug("creating pipes for node %s" % node)

            sources = self.node_sources(node)
            fieldlists = [source.output_fields for source in sources]
            self.logger.debug("initializing node of type %s" % node.__class__)
            node.initalize_fields(fieldlists)
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
        if array:
            return array

        return self.create_array(field)

    def create_array(self, field):
        """Create an array for `field`."""

        # FIXME: use array factory
        array = list()
        self.field_arrays[field] = array
        return array

    def run(self):
        nodes = self.sorted_nodes()
        print "NODES: %s" % nodes
        self.initialize(nodes)

        for node in nodes:
            self.logger.info("Running node %s" % node)

            fields = node.output_fields

            arrays = [self.get_array(field) for field in fields]
            table = self.create_table(arrays, fields)

            node.run(table)

        self.finalize(nodes)

    def create_table(self, arrays, fields):
        return PythonTable(arrays, fields)


