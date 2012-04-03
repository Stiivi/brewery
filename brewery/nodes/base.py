#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brewery.utils as utils

__all__ = (
    "create_node",
    "node_dictionary",
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
    """Return a dictionary containing node name as key and node class as value."""
    
    classes = node_subclasses(Node)
    dictionary = {}
    
    for c in classes:
        try:
            name = c.identifier()
            dictionary[name] = c
        except AttributeError as e:
            # If node does not provide identifier, we consider it to be
            # private or abstract class
            pass

    return dictionary

def node_subclasses(root, abstract = False):
    """Get all subclasses of node.
    
    :Parameters:
        * `abstract`: If set to ``True`` all abstract classes are included as well. Default is
          ``False``
    """
    classes = []
    for c in utils.subclass_iterator(root):
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
        logger.warn("depreciated __node_info__ present in %s, rename to node_info" \
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
        self.inputs = []
        self.outputs = []
        self._active_outputs = []
        self.description = None
        
        # Experimental: dictionary to be used to retype output fields
        # Currently used only in CSV source node.
        self._retype_dictionary = {}

    def initialize(self):
        """Initializes the node. Initialization is separated from creation. Put any Node subclass
        initialization in this method. Default implementation does nothing.
        
        .. note:
            Why the ``initialize()`` method? Node initiaization is different action from node object
            instance initialization in the ``__init__()`` method. Before executing node contents, the
            node has to be initialized - files or network connections opened, temporary tables created,
            data that are going to be used for configuration fetched, ... Initialization might require
            node to be fully configured first: all node attributes set to desired values. 
        """
        pass

    def finalize(self):
        """Finalizes the node. Default implementation does nothing."""
        pass

    def run(self):
        """Main method for running the node code. Subclasses should implement this method.
        """
        
        raise NotImplementedError("Subclasses of Node should implement the run() method")
        
    @property
    def input(self):
        """Return single node imput if exists. Convenience property for nodes which process only one
        input. Raises exception if there are no inputs or are more than one imput."""
        
        if len(self.inputs) == 1:
            return self.inputs[0]
        else:
            raise Exception("Single input requested. Node has none or more than one input (%d)."
                                    % len(self.inputs))
    
    def add_input(self, pipe):
        if pipe not in self.inputs:
            self.inputs.append(pipe)
        else:
            raise Exception("Input %s already connected" % pipe)

    def add_output(self, pipe):
        if pipe not in self.outputs:
            self.outputs.append(pipe)
        else:
            raise Exception("Output %s already connected" % pipe)
    
    def retype(self, name, **attributes):
        """Retype an output field `name` to field `field`.
        
        .. note:
        
            This function is not set in stone and might change. Consider it to
            be experimental feature.
        """
        self._retype_dictionary[name] = attributes
        
    def reset_type(self, name):
        """Remove all retype information for field `name`"""
        del self._retype_dictionary[name]
    
    def put(self, obj):
        """Put row into all output pipes.
        
        Raises `NodeFinished` exception when node's target nodes are not receiving data anymore.
        In most cases this exception might be ignored, as it is handled in the node thread
        wrapper. If you want to perform necessary clean-up in the `run()` method before exiting,
        you should handle this exception and then re-reaise it or just simply return from `run()`.

        This method can be called only from node's `run()` method. Do not call it from
        `initialize()` or `finalize()`.
        """
        active_outputs = 0
        for output in self.outputs:
            if not output.closed():
                output.put(obj)
                active_outputs += 1
        
        # This is not very safe, as run() might not expect it
        if not active_outputs:
            raise NodeFinished
  
    def put_record(self, obj):
        """Put record into all output pipes. Convenience method. Not recommended to be used.

        .. warning::

        Depreciated.

        """
        for output in self.outputs:
            output.put_record(obj)

    @property
    def input_fields(self):
        """Return fields from input pipe, if there is one and only one input pipe."""
        return self.input.fields
        
    @property
    def output_fields(self):
        """Return fields passed to the output by the node.
        
        Subclasses should override this method. Default implementation returns same fields as
        input has, raises exception when there are more inputs or if there is no input
        connected."""
        if not len(self.inputs) == 1:
            raise ValueError("Can not get default list of output fields: node has more than one input"
                             " or no input is provided. Subclasses should override this method")

        if not self.input.fields:
            raise ValueError("Can not get default list of output fields: input pipe fields are not "
                             "initialized")

        return self.input.fields
    
    @property
    def output_field_names(self):
        """Convenience method for gettin names of fields generated by the node. For more information
        see :meth:`brewery.nodes.Node.output_fields`"""
        raise PendingDeprecationWarning
        return self.output_fields.names()
        
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
        
        logger = utils.get_logger()
        
        # FIXME: this is temporary warning
        info = get_node_info(cls)
        ident = None

        if info:
            ident = info.get("name")
            
        if not ident:
            ident = utils.to_identifier(utils.decamelize(cls.__name__))
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

    @property
    def output_fields(self):
        raise NotImplementedError("SourceNode subclasses should implement output_fields")

    def add_input(self, pipe):
        raise Exception("Should not add input pipe to a source node")

class TargetNode(Node):
    """Abstract class for all target nodes
    
    .. abstract_node
    
    """
    def __init__(self):
        super(TargetNode, self).__init__()
        self.fields = None

    @property
    def output_fields(self):
        raise RuntimeError("Output fields asked from a target object.")

    def add_output(self, pipe):
        raise RuntimeError("Should not add output pipe to a target node")
    