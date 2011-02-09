class NetworkError(Exception):
    pass

class Network(object):
    """Data processing network"""
    def __init__(self):
        super(Network, self).__init__()
        self.nodes = []
        self.node_dict = {}
        self.connections = set()
        
    def node(self, node):
        """Returns a node in the network or node with name. This method is used for coalescing in
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
        """Add a `node` into the network."""
        if name:
            if name in self.node_dict:
                raise KeyError("Node with name %s already exists" % name)
            self.node_dict[name] = node

        if node not in self.nodes:
            self.nodes.append(node)
                        
    def remove(self, node):
        """Remove a `node` from the network. Also all connections will be removed."""

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


    def _sort_nodes(self):
        """
        L ← Empty list that will contain the sorted elements
        S ← Set of all nodes with no incoming edges
        while S is non-empty do
            remove a node n from S
            insert n into L
            for each node m with an edge e from n to m do
                remove edge e from the graph
                if m has no other incoming edges then
                    insert m into S
        if graph has edges then
            output error message (graph has at least one cycle)
        else 
            output message (proposed topologically sorted order: L)
        """
        pass
    def initialize(self):
        """Initializes the data processing network:
        
        * sorts nodes based on connection dependencies
        * creates pipes between nodes
        * initializes each node
        * initializes pipe fields
        
        """

        self.pipes = []
        
        # self._sort_nodes()
        # sorted_nodes = self.sort
        
        