import logging
import base
import threading
import traceback 
import sys

class StreamError(Exception):
    pass

class Stream(object):
    """Data processing stream"""
    def __init__(self, nodes = None, connections = None):
        """Creates a data stream.
        :Parameters:
            * `nodes` - dictionary with keys as node names and values as nodes
            * `connections` - list of two-item tuples. Each tuple contains source and target node
              or source and target node name.
        
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

    def initialize(self):
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
                pipe = base.Pipe()
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
            if isinstance(node, base.TargetNode):
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
        
        
        logging.info("running stream")
        
        threads = []
        sorted_nodes = self.sorted_nodes()

        logging.debug("launching threads")
        for node in sorted_nodes:
            logging.debug("launching thread for node %s" % node)
            thread = StreamNodeThread(node)
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
                        self.exceptions.append( (thread.node, thread.exception) )
                    else:
                        logging.debug("thread joined")
                    break
                if self.exceptions:
                    logging.info("node exception occured, trying to kill threads")
                    self.kill_threads()
                    

        for (thread, node) in threads:
            if thread.exception:
                logging.error("exception in node %s: %s" % (node, thread.exception))
                logging.debug("exception traceback: \n%s" % ("".join(thread.traceback)))
                # for line in thread.traceback:
                #     logging.debug("%s" % line)

        logging.info("run finished")
        
        
    def kill_threads(self):
        logging.info("killing threads")

    def finalize(self):
        logging.info("finalizing nodes")
        for node in self.sorted_nodes():
            logging.debug("finalizing node %s" % node)
            node.finalize()

class StreamNodeThread(threading.Thread):
    def __init__(self, node):
        """Creates a stream node thread.
        
        :Attributes:
            * `node`: a Node object
            * `exception`: attribute will contain exception if one occurs during run()
            * `traceback`: will contain traceback if exception occurs
        
        """
        super(StreamNodeThread, self).__init__()
        self.node = node
        self.exception = None
        self.traceback = None
        
    def run(self):
        """Wrapper method for running a node"""
        logging.debug("%s: start" % self)
        try:
            self.node.run()
        except base.NodeFinished, e:
            logging.info("node %s finished" % (self.node))
        except Exception, e:
            logging.error("node %s failed: %s" % (self.node, e))
            self.exception = e
            tb = sys.exc_info()[2]
            self.traceback = traceback.format_list(traceback.extract_tb(tb))
            del tb

        # Flush pipes after node is finished
        logging.debug("%s: finished" % self)
        logging.debug("%s: flushing outputs" % self)
        for pipe in self.node.outputs:
            pipe.flush()
        logging.debug("%s: flushed" % self)
        logging.debug("%s: stopping inputs" % self)
        for pipe in self.node.inputs:
            pipe.stop()
        logging.debug("%s: stopped" % self)
        