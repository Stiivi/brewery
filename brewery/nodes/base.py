#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brewery.common as common

__all__ = (
    "create_node",
    "node_dictionary",
    "node_catalogue",
    "get_node_info",
    "NodeFinished",
    "Node",
    "SourceNode",
    "TargetNode"
)

# FIXME: temporary dictionary to record displayed warnings about __node_info__
_node_info_warnings = set()

def create_node(identifier, *args, **kwargs):
    """Creates a node of type specified by `identifier`. Options are passed to
    the node initializer"""

    d = node_dictionary()
    node_class = d[identifier]
    node = node_class(*args, **kwargs)
    return node

def node_dictionary():
    """Return a dictionary containing node name as key and node class as
    value. This will be depreciated soon in favour of
    :func:`node_catalogue()`"""

    classes = node_subclasses(Node)
    dictionary = {}

    for c in classes:
        try:
            name = c.identifier()
            dictionary[name] = c
        except AttributeError:
            # If node does not provide identifier, we consider it to be
            # private or abstract class
            pass

    return dictionary

def node_catalogue():
    """Returns a dictionary of information about all available nodes. Keys are
    node identifiers, values are dictionaries. The information dictionary contains
    all the keys from the node's `node_info` dictionary plus keys: `factory`
    with node class, `type` (if not provided) is set to one of ``source``,
    ``processing`` or ``target``.
    """

    classes = node_subclasses(Node)

    catalogue = {}

    for node_class in classes:
        try:
            name = node_class.identifier()
        except AttributeError:
            # If node does not provide identifier, we consider it to be
            # private or abstract class
            continue

        # Get copy of node info
        info = dict(get_node_info(node_class))
        info["name"] = name
        info["factory"] = node_class

        # Get node type based on superclass, if not provided

        if "type" not in info:
            if issubclass(node_class, SourceNode):
                info["type"] = "source"
            elif not issubclass(node_class, SourceNode) \
                    and not issubclass(node_class, TargetNode):
                info["type"] = "processing"
            elif issubclass(node_class, TargetNode):
                info["type"] = "target"
            else:
                info["type"] = "unknown"

        catalogue[name] = info

    return catalogue

def node_subclasses(root, abstract = False):
    """Get all subclasses of node.

    :Parameters:
        * `abstract`: If set to ``True`` all abstract classes are included as well. Default is
          ``False``
    """
    classes = []
    for c in common.subclass_iterator(root):
        try:
            info = get_node_info(c)

            node_type = info.get("type")
            if node_type != "abstract":
                classes.append(c)
        except AttributeError:
            pass

    return classes

def get_node_info(cls):
    """Get node info attribute of a node - transient function during
    depreciation"""

    if hasattr(cls, "__node_info__") and cls not in _node_info_warnings:

        common.get_logger().warn("depreciated __node_info__ present in %s, rename to node_info" \
                    " (this warning will be shown only once)" % str(cls))
        _node_info_warnings.add(cls)

        return cls.__node_info__
    else:
        return cls.node_info

class NodeFinished(Exception):
    """Exception raised when node has no active outputs - each output node signalised that it
    requires no more data."""
    pass

class Node(object):
    """Base class for procesing node

    .. abstract_node
    """
    def __init__(self):
        """Creates a new data processing node.

        :Attributes:
            * `inputs`: input pipes
            * `outputs`: output pipes
            * `description`: custom node annotation
        """

        super(Node, self).__init__()
        self.output_fields = None
        # Experimental: dictionary to be used to retype output fields
        # Currently used only in CSV source node.
        self._retype_dictionary = {}

    def initialize(self):
        """Initializes the node. Initialization is separated from creation.
        Put any Node subclass initialization in this method. Default
        implementation does nothing.

        .. note:
            Why the ``initialize()`` method? Node initiaization is different
            action from node object instance initialization in the
            ``__init__()`` method. Before executing node contents, the node
            has to be initialized - files or network connections opened,
            temporary tables created, data that are going to be used for
            configuration fetched, ... Initialization might require node to be
            fully configured first: all node attributes set to desired values.
        """
        pass
        # FIXME: obsolete

    def initialize_fields(self, sources):
        """Initialize fields based on source nodes. `sources` contains a list
           of `FieldList` objects. """
        pass

    def finalize(self):
        """Finalizes the node. Default implementation does nothing."""
        pass

    def run(self, table=None):
        """Main method for running the node code on top of optional (virtual)
        table structure. Subclasses should implement this method.  """

        raise NotImplementedError("Subclasses of Node should implement the run() method")

    @property
    def input(self):
        """Return single node input if exists. Convenience property for nodes
        which process only one input. Raises exception if there are no inputs
        or are more than one imput."""

        if len(self.inputs) == 1:
            return self.inputs[0]
        else:
            raise Exception("Single input requested. Node has none or more than one input (%d)."
                                    % len(self.inputs))

    def get_output_fields(self):
        """Return fields passed to the output by the node.

        Subclasses should override this method. Default implementation returns
        same fields as input has, raises exception when there are more inputs
        or if there is no input connected."""

        if not len(self.inputs) == 1:
            raise ValueError("Can not get default list of output fields: node has more than one input"
                             " or no input is provided. Subclasses should override this method")

        if not self.input.fields:
            raise ValueError("Can not get default list of output fields: input pipe fields are not "
                             "initialized")

        return self.input.fields

    @classmethod
    def identifier(cls):
        """Returns an identifier name of the node class. Identifier is used
        for construction of streams from dictionaries or for any other
        out-of-program constructions.

        Node identifier is specified in the `node_info` dictioanry as
        ``name``. If no explicit identifier is specified, then decamelized
        class name will be used with `node` suffix removed. For example:
        ``CSVSourceNode`` will be ``csv_source``.
        """

        logger = common.get_logger()

        # FIXME: this is temporary warning
        info = get_node_info(cls)
        ident = None

        if info:
            ident = info.get("name")

        if not ident:
            ident = common.to_identifier(common.decamelize(cls.__name__))
            if ident.endswith("_node"):
                ident = ident[:-5]

        return ident

    def configure(self, config, protected = False):
        """Configure node.

        :Parameters:
            * `config` - a dictionary containing node attributes as keys and values as attribute
              values. Key ``type`` is ignored as it is used for node creation.
            * `protected` - if set to ``True`` only non-protected attributes are set. Attempt
              to set protected attribute will result in an exception. Use `protected` when you are
              configuring nodes through a user interface or a custom tool. Default is ``False``: all
              attributes can be set.

        If key in the `config` dictionary does not refer to a node attribute specified in node
        description, then it is ignored.
        """

        attributes = dict((a["name"], a) for a in get_node_info(self)["attributes"])

        for attribute, value in config.items():
            info = attributes.get(attribute)

            if not info:
                continue
                # raise KeyError("Unknown attribute '%s' in node %s" % (attribute, str(type(self))))

            if protected and info.get("protected"):
                # FIXME: use some custom exception
                raise Exception("Trying to set protected attribute '%s' of node '%s'" %
                                        (attribute, str(type(self))))
            else:
                setattr(self, attribute, value)

class SourceNode(Node):
    """Abstract class for all source nodes

    All source nodes should provide an attribute or implement a property (``@property``) called
    ``output_fields``.

    .. abstract_node

    """
    def __init__(self):
        super(SourceNode, self).__init__()


class TargetNode(Node):
    """Abstract class for all target nodes

    .. abstract_node

    """
    def __init__(self):
        super(TargetNode, self).__init__()
        self.fields = None

