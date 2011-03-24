import logging
import threading
import traceback 
import sys
import inspect
import utils
import StringIO

from brewery.nodes import *

__all__ = [
    "Stream",
    "StreamRuntimeError",
    "FieldError",
    "Pipe"
]

class FieldError(Exception):
    """Exception raised on field incompatibility or missing fields."""
    pass

class StreamRuntimeError(Exception):
    """Exception raised when a node fails during `run()` phase.

    Attributes:
        * `message`: exception message
        * `node`: node where exception was raised
        * `exception`: exception that was raised while running the node
        * `traceback`: stack traceback
        * `inputs`: array of field lists for each input
        * `output`: output field list
    """
    def __init__(self, message = None, node = None, exception = None):
        super(StreamRuntimeError, self).__init__()
        if message:
            self.message = message
        else:
            self.message = ""

        self.node = node
        self.exception = exception
        self.traceback = None
        self.inputs = []
        self.output = []
        self.attributes = {}

    def print_exception(self, output = None):
        """Prints exception. You can specify IO stream object in `output` parameter. By default
        text is printed to standard output."""
        
        if not output:
            output = sys.stdout
            
        text =  u"stream failed. reason: %s\n" % self.message
        text += u"exception: %s: \n" % self.exception.__class__.__name__

        text += u"node: %s\n" % self.node

        try:
            text += unicode(self.exception)
        except Exception, e:
            text += u"<unable to get exception string: %s>" % e

        text += "\ntraceback\n"

        try:
            text += u"".join(self.traceback)
        except Exception, e:
            text += u"<unable to get traceback string: %s>" % e
        
        text += "\n"
        
        if self.inputs:
            for i, fields in enumerate(self.inputs):
                text += "input %i:\n" % i
                input_text = ""
                for (index, field) in enumerate(fields):
                    input_text += u"% 5d %s (storage:%s analytical:%s)\n" \
                                % (index, field.name, field.storage_type, field.analytical_type)
                text += unicode(input_text)
        else:
            text += "input: none"

        text += "\n"    

        if self.output:
            text += "output:\n"
            for field in self.output:
                text += u"    %s (storage:%s analytical:%s)\n" \
                            % (field.name, field.storage_type, field.analytical_type)
        else:
            text += "ouput: none"
            
        text += "\n"    
        
        if self.attributes:
            text += "attributes:\n"
            for name, attribute in self.attributes.items():
                try:
                    value = unicode(attribute)
                except Exception, e:
                    value = "unable to convert to string (exception: %s)" % e
                text += "    %s: %s\n" % (name, value)
        else:
            text += "attributes: none"
        print "TEEEXT: %s"  % type(text)
        output.write(text)
        
    def __str__(self):
        s = StringIO.StringIO()
        try:
            self.print_exception(s)
            v = s.getvalue()
        except Exception as e:
            v = "Unable to print strem exception. Reason: %s (%s)" % (e, type(e))
        finally:
            s.close()
        
        return v

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
        row = []
        for field in self.fields.names():
            row.append(record.get(field))
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

    def __init__(self, buffer_size = 1000):
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

    def _flush(self, close = False):
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
        while(not done_sending):
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

class Stream(object):
    """Data processing stream"""
    def __init__(self, nodes = None, connections = None):
        """Creates a data stream.

        :Parameters:
            * `nodes` - dictionary with keys as node names and values as nodes
            * `connections` - list of two-item tuples. Each tuple contains source and target node
              or source and target node name.
            * `stream` - another stream or 
        """
        super(Stream, self).__init__()
        self.nodes = []
        self.node_dict = {}
        self.connections = set()
        
        if nodes:
            if type(nodes) == dict:
                for name, node in nodes.items():
                    self.add(node, name)
            else:
                raise StreamError("Nodes should be a dictionary, is %s" % type(nodes))

        if connections:
            for connection in connections:
                self.connect(connection[0], connection[1])
        
        self.exceptions = []
        
    def node(self, node):
        """Returns a node in the stream or node with name. This method is used for coalescing in
        other methods, where you can pass either node name or node object.
        
        :Parameters:
            * `node` - node object or node name
        """
        
        if type(node) == str or type(node) == unicode:
            if not node in self.node_dict:
                raise KeyError("Node with name '%s' does not exist" % node)
            return self.node_dict[node]
        else:
            return node

    def add(self, node, name = None):
        """Add a `node` into the stream."""
        if name:
            if name in self.node_dict:
                raise KeyError("Node with name %s already exists" % name)
            self.node_dict[name] = node

        if node not in self.nodes:
            self.nodes.append(node)
    
    def set_node_name(self, node, name):
        """Sets a name for `node`. If `name` is ``None`` then node name will be removed.
        
        Raises an exception if the `node` is not part of the stream or there is already node with
        same name.
        """
        if node not in self.nodes:
            raise KeyError("Node %s does not belong to stream" % node)

        if name:
            if name in self.node_dict:
                raise KeyError("Node with name %s already exists" % name)
            
            self.node_dict[name] = node
        else:
            del self.node_dict[name]
    
    def remove(self, node):
        """Remove a `node` from the stream. Also all connections will be removed."""

        node = self.node(node)
        
        self.nodes.remove(node)
        for (name, current_node) in self.node_dict.items():
            if current_node == node:
                del self.node_dict[name]

        to_be_removed = set()
        for connection in self.connections:
            if connection[0] == node or connection[1] == node:
                to_be_removed.add(connection)
                
        for connection in to_be_removed:
            self.connections.remove(connection)
    
    def connect(self, source, target):
        """Connects source node and target node. Nodes can be provided as objects or names."""
        
        source_node = self.node(source)
        target_node = self.node(target)
        self.connections.add( (source_node, target_node) )
        
    def remove_connection(self, source, target):
        """Remove connection between source and target nodes, if exists."""
        source_node = self.node(source)
        target_node = self.node(target)

        self.connections.discard( (source_node, target_node) )


    def sorted_nodes(self):
        """
        Return topologically sorted nodes.
        
        Algorithm::
        
            L = Empty list that will contain the sorted elements
            S = Set of all nodes with no incoming edges
            while S is non-empty do
                remove a node n from S
                insert n into L
                for each node m with an edge e from n to m do
                    remove edge e from the graph
                    if m has no other incoming edges then
                        insert m into S
            if graph has edges then
                raise exception: graph has at least one cycle
            else 
                return proposed topologically sorted order: L
        """
        def is_source(node, connections):
            for connection in connections:
                if node == connection[1]:
                    return False
            return True
       
        def source_connections(node, connections):
            conns = set()
            for connection in connections:
                if node == connection[0]:
                    conns.add(connection)
            return conns
        
        nodes = set(self.nodes)
        connections = self.connections.copy()
        sorted_nodes = []
        source_nodes = set()

        # Find source nodes:
        for node in nodes:
            if is_source(node, connections):
                source_nodes.add(node)
        
        # while S is non-empty do
        while source_nodes:
            # remove a node n from S
            node = source_nodes.pop()
            # insert n into L
            sorted_nodes.append(node)

            # for each node m with an edge e from n to m do
            s_connections = source_connections(node, connections)
            for connection in s_connections:
                #     remove edge e from the graph
                m = connection[1]
                connections.remove(connection)
                #     if m has no other incoming edges then
                #         insert m into S
                if is_source(m, connections):
                    source_nodes.add(m)

        # if graph has edges then
        #     output error message (graph has at least one cycle)
        # else 
        #     output message (proposed topologically sorted order: L)

        if connections:
            raise Exception("Steram has at least one cycle")
            
        return sorted_nodes
    def fork(self, node = None):
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
        
        return _StreamFork(self, self.node(node))
        
        
    def update(self, dictionary):
        """Adds nodes and connections specified in the dictionary. Dictionary might contain
        node names instead of real classes. You can use this method for creating stream
        from a dictionary that was created from a JSON file, for example.
        """
        
        nodes = dictionary.get("nodes")
        connections = dictionary.get("connections")
        
        class_dict = Node.class_dictionary()
        
        for (name, obj) in nodes.items():
            if isinstance(obj, Node):
                node_instance = obj
            elif isinstance(obj, type) and issubclass(obj, brewery.nodes.Node):
                node_instance = obj() 
            else:
                if not "type" in obj:
                    raise Exception("Node dictionary has no type key")
                node_type = obj["type"]
                
                if node_type in class_dict:
                    node_class = class_dict[node_type]
                    node_instance = node_class()
                    node_instance.configure(obj)
                else:
                    raise Exception("No node class of type '%s'" % obj)

            self.add(node_instance, name)

        if connections:
            for connection in connections:
                self.connect(connection[0], connection[1])
        
    def node_targets(self, node):
        """Return nodes that `node` passes data into."""
        nodes = []
        node = self.node(node)
        for connection in self.connections:
            if connection[0] == node:
                nodes.append(connection[1])
        return nodes
        
    def node_sources(self, node):
        """Return nodes that provide data for `node`."""
        nodes = []
        node = self.node(node)
        for connection in self.connections:
            if connection[1] == node:
                nodes.append(connection[0])
        return nodes

    def _initialize(self):
        """Initializes the data processing stream:
        
        * sorts nodes based on connection dependencies
        * creates pipes between nodes
        * initializes each node
        * initializes pipe fields
        
        """

        logging.info("initializing stream")
        logging.debug("sorting nodes")
        sorted_nodes = self.sorted_nodes()
        self.pipes = []

        logging.debug("flushing pipes")
        for node in sorted_nodes:
            node.inputs = []
            node.outputs = []

        # Create pipes and connect nodes
        for node in sorted_nodes:
            logging.debug("creating pipes for node %s" % node)

            targets = self.node_targets(node)
            for target in targets:
                logging.debug("  connecting with %s" % (target))
                pipe = Pipe()
                node.add_output(pipe)
                target.add_input(pipe)
                self.pipes.append(pipe)
                
        # Initialize fields
        for node in sorted_nodes:
            logging.debug("initializing node of type %s" % node.__class__)
            logging.debug("  node has %d inputs and %d outputs" 
                                % (len(node.inputs), len(node.outputs)))
            node.initialize()

            # Ignore target nodes
            if isinstance(node, TargetNode):
                logging.debug("  node is target, ignoring creation of output pipes" )
                continue

            fields = node.output_fields
            logging.debug("  node output fields: %s" % fields.names())
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
        
        
        logging.info("running stream")
        
        threads = []
        sorted_nodes = self.sorted_nodes()

        logging.debug("launching threads")
        for node in sorted_nodes:
            logging.debug("launching thread for node %s" % node)
            thread = _StreamNodeThread(node)
            thread.start()
            threads.append( (thread, node) )

        self.exceptions = []
        for (thread, node) in threads:
            logging.debug("joining thread for %s" % node)
            while True:
                thread.join(0.2)
                if thread.isAlive():
                    pass
                    # logging.debug("thread join timed out")
                else:
                    if thread.exception:
                        self._add_thread_exception(thread)
                    else:
                        logging.debug("thread joined")
                    break
                if self.exceptions:
                    logging.info("node exception occured, trying to kill threads")
                    self.kill_threads()

        if self.exceptions:
            logging.info("run finished with exception")
            # Raising only first exception found
            raise self.exceptions[0]
        else:
            logging.info("run finished sucessfully")

    def _add_thread_exception(self, thread):
        """Create a StreamRuntimeError exception object and fill attributes with all necessary
        values.
        """
        node = thread.node
        exception = StreamRuntimeError(node = node, exception = thread.exception)

        exception.traceback = thread.traceback

        array = []
        for pipe in node.inputs:
            array.append(pipe.fields)
        exception.inputs = array
        
        if not isinstance(node, TargetNode):
            try:
                exception.ouputs = node.output_fields
            except:
                pass
        
        node_info = node.__class__.__dict__.get("__node_info__")

        attrs = {}
        if node_info and "attributes" in node_info:
            for attribute in node_info["attributes"]:
                attr_name = attribute.get("name")
                if attr_name:
                    try:
                        value = getattr(node, attr_name)
                    except AttributeError:
                        value = "<attribute %s does not exist>" % attr_name
                    except Exception ,e:
                        value = e
                    attrs[attr_name] = value
        
        exception.attributes = attrs
        
        self.exceptions.append(exception)
        
        
    def kill_threads(self):
        logging.info("killing threads")

    def _finalize(self):
        logging.info("finalizing nodes")
        
        # FIXME: encapsulate finalization in exception handler, collect exceptions
        for node in self.sorted_nodes():
            logging.debug("finalizing node %s" % node)
            node.finalize()

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
        
    def run(self):
        """Wrapper method for running a node"""
        logging.debug("%s: start" % self)
        try:
            self.node.run()
        except NodeFinished, e:
            logging.info("node %s finished" % (self.node))
        except Exception, e:
            logging.info("node %s failed: %s" % (self.node, e.__class__.__name__))
            tb = sys.exc_info()[2]
            self.traceback = traceback.format_list(traceback.extract_tb(tb))
            print self.traceback
            self.exception = e
            del tb

        # Flush pipes after node is finished
        logging.debug("%s: finished" % self)
        logging.debug("%s: flushing outputs" % self)
        for pipe in self.node.outputs:
            if not pipe.closed():
                pipe.done_sending()
        logging.debug("%s: flushed" % self)
        logging.debug("%s: stopping inputs" % self)
        for pipe in self.node.inputs:
            if not pipe.closed():
                pipe.done_sending()
        logging.debug("%s: stopped" % self)

class _StreamFork(object):
    """docstring for StreamFork"""
    def __init__(self, stream, node = None):
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
        pass
        
    def append(self, obj):
        """Appends data from nodes using AppendNode"""
        pass
    
    def __getattr__(self, name):
        """Returns node class"""
        class_dict = Node.class_dictionary()

        if not name in class_dict:
            raise AttributeError(name)

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
        