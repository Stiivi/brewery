# -*- coding: utf-8 -*-

import threading
import sys
from brewery.nodes.base import node_dictionary, TargetNode, NodeFinished
from brewery.utils import get_logger
from brewery.nodes import *
from brewery.common import *
from .graph import *

__all__ = [
    "Stream",
    "Pipe",
    "stream_from_dict",
    "create_builder"
]

JOIN_TIMEOUT = None

def stream_from_dict(desc):
    """Create a stream from dictionary `desc`."""
    stream = Stream()
    stream.update(desc)
    return stream

class SimpleDataPipe(object):
    """Dummy pipe for testing nodes"""
    def __init__(self):
        self.buffer = []
        self.fields = None
        self._closed = False

    def closed(self):
        return self._closed

    def rows(self):
        return self.buffer

    def records(self):
        """Get data objects from pipe as records (dict objects). This is convenience method with
        performance costs. Nodes are recommended to process rows instead."""
        if not self.fields:
            raise Exception("Can not provide records: fields for pipe are not initialized.")
        fields = self.fields.names()
        for row in self.rows():
            yield dict(zip(fields, row))

    def put_record(self, record):
        """Convenience method that will transform record into a row based on pipe fields."""
        row = [record.get(field) for field in self.fields.names()]

        self.put(row)

    def put(self, obj):
        self.buffer.append(obj)

    def done_receiving(self):
        self._closed = True
        pass

    def done_sending(self):
        pass

    def empty(self):
        self.buffer = []

class Pipe(SimpleDataPipe):
    """Data pipe:
    Contains buffer for data that should be thransferred to another node.
    Data are being sent t other node when the buffer is full. Pipe is one-directional where
    one thread is sending data to another thread. There is only one backward signalling: closing
    the pipe from remote object.


    """

    def __init__(self, buffer_size=1000):
        """Creates uni-drectional data pipe for passing data between two threads in batches of size
        `buffer_size`.

        If receiving node is finished with source data and does not want anything any more, it
        should send ``done_receiving()`` to the pipe. In most cases, stream runner will send
        ``done_receiving()`` to all input pipes when node's ``run()`` method is finished.

        If sending node is finished, it should send ``done_sending()`` to the pipe, however this
        is not necessary in most cases, as the method for running stream flushes outputs
        automatically on when node ``run()`` method is finished.
        """

        super(Pipe, self).__init__()
        self.buffer_size = buffer_size

        # Should it be deque or array?
        self.staging_buffer = []
        self._ready_buffer = None

        self._done_sending = False
        self._done_receiving = False
        self._closed = False

        # Taken from Python Queue implementation:

        # mutex must beheld whenever the queue is mutating.  All methods
        # that acquire mutex must release it before returning.  mutex
        # is shared between the three conditions, so acquiring and
        # releasing the conditions also acquires and releases mutex.
        self.mutex = threading.Lock()
        # Notify not_empty whenever an item is added to the queue; a
        # thread waiting to get is notified then.
        self.not_empty = threading.Condition(self.mutex)
        # Notify not_full whenever an item is removed from the queue;
        # a thread waiting to put is notified then.
        self.not_full = threading.Condition(self.mutex)

    def is_full(self):
        return len(self.staging_buffer) >= self.buffer_size

    def is_consumed(self):
        return self._ready_buffer is None

    def put(self, obj):
        """Put data object into the pipe buffer. When buffer is full it is enqueued and receiving node
        can get all buffered data objects.

        Puttin object into pipe is not thread safe. Only one thread sohuld write to the pipe.
        """
        self.staging_buffer.append(obj)

        if self.is_full():
            self._flush()
    def _note(self, note):
        # print note
        pass

    def _flush(self, close=False):
        self._note("P flushing: close? %s closed? %s" % (close, self._closed))
        self._note("P _nf acq?")
        self.not_full.acquire()
        if self._closed:
            self._note("P _not_full rel!")
            self.not_full.release()
            return
        elif len(self.staging_buffer) == 0:
            try:
                self._closed = close
                self.not_empty.notify()
            finally:
                self._note("P _not_full rel!")
                self.not_full.release()
            return

        try:
            self._note("P _not_full wait ...")
            while not self.is_consumed() and not self._closed:
                self.not_full.wait()
            self._note("P _not_full got <")
            if not self._closed:
                self._ready_buffer = self.staging_buffer
                self.staging_buffer = []
                self._closed = close
                self._note("P _not_empty notify >")
                self.not_empty.notify()

        finally:
            self._note("P _not_full rel!")
            self.not_full.release()

    def rows(self):
        """Get data object from pipe. If there is no buffer ready, wait until source object sends
        some data."""

        done_sending = False
        while not done_sending:
            self._note("C _not_empty acq?")
            self.not_empty.acquire()
            try:
                self._note("C _not_empty wait ...")
                while not self._ready_buffer and not self._closed:
                    self.not_empty.wait()
                self._note("C _not_empty got <")

                if self._ready_buffer:
                    rows = self._ready_buffer
                    self._ready_buffer = None
                    self._note("C _not_full notify >")
                    self.not_full.notify()

                    for row in rows:
                        yield row
                else:
                    self._note("C no buffer")


                done_sending = self._closed
            finally:
                self._note("_not_empty rel!")
                self.not_empty.release()

    def closed(self):
        """Return ``True`` if pipe is closed - not sending or not receiving data any more."""
        return self._closed

    def done_sending(self):
        """Close pipe from sender side"""
        self._flush(True)

    def done_receiving(self):
        """Close pipe from either side"""
        self._note("C not_empty acq? r")
        self.not_empty.acquire()
        self._note("C closing")
        self._closed = True
        self._note("C notif close")
        self.not_full.notify()
        self.not_empty.release()

        self._note("C not_empty rel! r")

class Stream(Graph):
    """Data processing stream"""
    def __init__(self, nodes=None, connections=None):
        """Creates a data stream.

        :Parameters:
            * `nodes` - dictionary with keys as node names and values as nodes
            * `connections` - list of two-item tuples. Each tuple contains source and target node
              or source and target node name.
            * `stream` - another stream or
        """
        super(Stream, self).__init__(nodes, connections)
        self.logger = get_logger()

        self.exceptions = []

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

    def update(self, nodes = None, connections = None):
        """Adds nodes and connections specified in the dictionary. Dictionary might contain
        node names instead of real classes. You can use this method for creating stream
        from a dictionary that was created from a JSON file, for example.
        """

        node_dict = node_dictionary()

        # FIXME: use either node type identifier or fully initialized node, not
        #        node class (Warning: might break some existing code,
        #        depreciate it first

        nodes = nodes or {}
        connections = connections or []

        for (name, obj) in nodes.items():
            if isinstance(obj, Node):
                node_instance = obj
            elif isinstance(obj, type) and issubclass(obj, Node):
                self.logger.warn("Using classes in Stream.update is depreciated")
                node_instance = obj()
            else:
                if not "type" in obj:
                    raise Exception("Node dictionary has no 'type' key")
                node_type = obj["type"]

                if node_type in node_dict:
                    node_class = node_dict[node_type]
                    node_instance = node_class()

                    node_instance.configure(obj)
                else:
                    raise Exception("No node class of type '%s'" % node_type)

            self.add(node_instance, name)

        if connections:
            for connection in connections:
                self.connect(connection[0], connection[1])

    def configure(self, config=None):
        """Configure node properties based on configuration. Only named nodes can be configured at the
        moment.

        `config` is a list of dictionaries with keys: ``node`` - node name, ``parameter`` - node parameter
        name, ``value`` - parameter value

        .. warning:

            This method might change to a list of dictionaries where one
            dictionary will represent one node, keys will be attributes.

        """

        # FIXME: this is wrong, it should be a single dict per node (or not?)
        # List of attributes:
        #     * can reflect a form for configuring whole stream
        #     * can have attribute order regardless of their node ownership
        # List of nodes:
        #     * bundled attributes in single dictioary
        # FIXME: this is inconsistent with node configuration! node.config()
        if config is None:
            config = {}
        configurations = {}

        # Collect configurations for each node

        for attribute in config:
            node_name = attribute["node"]
            attribute_name = attribute["attribute"]
            value = attribute.get("value")

            if not node_name in configurations:
                config = {}
                configurations[node_name] = config
            else:
                config = configurations[node_name]

            config[attribute_name] = value

        # Configure nodes

        for (node_name, config) in configurations.items():
            node = self.coalesce_node(node_name)
            node.configure(config)

    def _initialize(self):
        """Initializes the data processing stream:

        * sorts nodes based on connection dependencies
        * creates pipes between nodes
        * initializes each node
        * initializes pipe fields

        """

        self.logger.info("initializing stream")
        self.logger.debug("sorting nodes")
        sorted_nodes = self.sorted_nodes()
        self.pipes = []

        self.logger.debug("flushing pipes")
        for node in sorted_nodes:
            node.inputs = []
            node.outputs = []

        # Create pipes and connect nodes
        for node in sorted_nodes:
            self.logger.debug("creating pipes for node %s" % node)

            targets = self.node_targets(node)
            for target in targets:
                self.logger.debug("  connecting with %s" % (target))
                pipe = Pipe()
                node.add_output(pipe)
                target.add_input(pipe)
                self.pipes.append(pipe)

        # Initialize fields
        for node in sorted_nodes:
            self.logger.debug("initializing node of type %s" % node.__class__)
            self.logger.debug("  node has %d inputs and %d outputs"
                                % (len(node.inputs), len(node.outputs)))
            node.initialize()

            # Ignore target nodes
            if isinstance(node, TargetNode):
                self.logger.debug("  node is target, ignoring creation of output pipes")
                continue

            fields = node.output_fields
            self.logger.debug("  node output fields: %s" % fields.names())
            for output_pipe in node.outputs:
                output_pipe.fields = fields

    def run(self):
        """Run all nodes in the stream.

        Each node is being wrapped and run in a separate thread.

        When an exception occurs, the stream is stopped and all catched exceptions are stored in
        attribute `exceptions`.

        """
        self._initialize()

        # FIXME: do better exception handling here: what if both will raise exception?
        try:
            self._run()
        finally:
            self._finalize()

    def _run(self):
        self.logger.info("running stream")

        threads = []
        sorted_nodes = self.sorted_nodes()

        self.logger.debug("launching threads")
        for node in sorted_nodes:
            self.logger.debug("launching thread for node %s" % node_label(node))
            thread = _StreamNodeThread(node)
            thread.start()
            threads.append((thread, node))

        self.exceptions = []
        for (thread, node) in threads:
            self.logger.debug("joining thread for %s" % node_label(node))
            while True:
                thread.join(JOIN_TIMEOUT)
                if thread.isAlive():
                    pass
                    # self.logger.debug("thread join timed out")
                else:
                    if thread.exception:
                        self._add_thread_exception(thread)
                    else:
                        self.logger.debug("thread joined")
                    break
                if self.exceptions:
                    self.logger.info("node exception occured, trying to kill threads")
                    self.kill_threads()

        if self.exceptions:
            self.logger.info("run finished with exception")
            # Raising only first exception found
            raise self.exceptions[0]
        else:
            self.logger.info("run finished sucessfully")

    def _add_thread_exception(self, thread):
        """Create a StreamRuntimeError exception object and fill attributes with all necessary
        values.
        """
        node = thread.node
        exception = StreamRuntimeError(node=node, exception=thread.exception)

        exception.traceback = thread.traceback
        exception.inputs = [pipe.fields for pipe in node.inputs]

        if not isinstance(node, TargetNode):
            try:
                exception.ouputs = node.output_fields
            except:
                pass

        node_info = node.node_info

        attrs = {}
        if node_info and "attributes" in node_info:
            for attribute in node_info["attributes"]:
                attr_name = attribute.get("name")
                if attr_name:
                    try:
                        value = getattr(node, attr_name)
                    except AttributeError:
                        value = "<attribute %s does not exist>" % attr_name
                    except Exception , e:
                        value = e
                    attrs[attr_name] = value

        exception.attributes = attrs

        self.exceptions.append(exception)


    def kill_threads(self):
        self.logger.info("killing threads")

    def _finalize(self):
        self.logger.info("finalizing nodes")

        # FIXME: encapsulate finalization in exception handler, collect exceptions
        for node in self.sorted_nodes():
            self.logger.debug("finalizing node %s" % node_label(node))
            node.finalize()

def node_label(node):
    """Debug label for a node: node identifier with python object id."""
    return "%s(%s)" % (node.identifier() or str(type(node)), id(node))

class _StreamNodeThread(threading.Thread):
    def __init__(self, node):
        """Creates a stream node thread.

        :Attributes:
            * `node`: a Node object
            * `exception`: attribute will contain exception if one occurs during run()
            * `traceback`: will contain traceback if exception occurs

        """
        super(_StreamNodeThread, self).__init__()
        self.node = node
        self.exception = None
        self.traceback = None
        self.logger = get_logger()

    def run(self):
        """Wrapper method for running a node"""

        label = node_label(self.node)
        self.logger.debug("%s: start" % label)
        try:
            self.node.run()
        except NodeFinished:
            self.logger.info("node %s finished" % label)
        except Exception as e:
            tb = sys.exc_info()[2]
            self.traceback = tb

            self.logger.debug("node %s failed: %s" % (label, e.__class__.__name__), exc_info=sys.exc_info)
            self.exception = e

        # Flush pipes after node is finished
        self.logger.debug("%s: finished" % label)
        self.logger.debug("%s: flushing outputs" % label)
        for pipe in self.node.outputs:
            if not pipe.closed():
                pipe.done_sending()
        self.logger.debug("%s: flushed" % label)
        self.logger.debug("%s: stopping inputs" % label)
        for pipe in self.node.inputs:
            if not pipe.closed():
                pipe.done_sending()
        self.logger.debug("%s: stopped" % self)

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
        print "CALLING %s - %s" % (self.node_class, node)
        self.fork += node
        return self.fork

def create_builder():
    """Creates a stream builder for incremental stream building."""
    stream = Stream()
    return stream.fork()

