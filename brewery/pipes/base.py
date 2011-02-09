import Queue
import threading
import logging

class SimpleDataPipe(object):
    """Dummy pipe for testing nodes"""
    def __init__(self):
        self.buffer = []
        self.fields = None

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
            
    def stop(self):
        pass
        
    def flush(self):
        pass
        
    def empty(self):
        self.buffer = []
        
class Pipe(SimpleDataPipe):
    """Pipe for transfer of structured data between processing nodes"""
    def __init__(self, buffer_size = 1000, queue_size = 1):
        """Create a pipe for transfer of structured data between processing nodes.
        
        :Parameters:
            * `buffer_size`: number of data objects (rows or records) to be collected before they can
              be acquired by receiving object. Default is 1000.
            * `queue_size`: number of buffers in a processing queue. Default is 1. Set to 0 for
              unlimited.
        """
        super(Pipe, self).__init__()
        self.buffer_size = buffer_size
        self.queue_size = queue_size
        self.queue = Queue.Queue(queue_size)
        self.stop_sending_lock = threading.RLock()
        
        self.buffer = []

        self.finished = False
        self.stop_sending = False

    def put(self, obj):
        """Put data object into the pipe buffer. When buffer is full it is enqueued and receiving node
        can get all buffered data objects."""

        if self.stop_sending:
            return

        self.buffer.append(obj)
        if len(self.buffer) >= self.buffer_size:
            self._send_buffer()
            self.buffer = []
        
    def _send_buffer(self):
        logging.debug("sending buffer of size %d" % len(self.buffer))
        if self.stop_sending:
            logging.debug("stop sending - not sending anything")
            return

        self.queue.put(self.buffer)

    def flush(self):
        """Send all remaining data objects into the pipe buffer and signalize end of source."""
        self._send_buffer()
        self.finished = True

    def rows(self):
        """Get data object from pipe. If there is no buffer ready, wait until source object sends some
        data."""

        while True:
            data_buffer = self.queue.get()
            for obj in data_buffer:
                yield obj

            self.queue.task_done()

            if self.finished and self.queue.empty():
                break

    def records(self):
        """Get data objects from pipe as records (dict objects). This is convenience method with
        performance costs. Nodes are recommended to process rows instead."""
        if not self.fields:
            raise Exception("Can not provide records: fields for pipe are not initialized.")

        fields = self.fields.names()
        for row in self.rows():
            yield dict(zip(fields, row))

    def stop(self):
        """Close the pipe from target node: no more data needed."""
        self.stop_sending = True

        while True:
            try:
                self.queue.get_nowait()
            except Queue.Empty:
                break

class Node(object):
    """Base class for procesing node
    
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
        self.description = None

    def initialize(self):
        """Initializes the node. Initialization is separated from creation. Put any Node subclass
        initialization in this method. Default implementation does nothing."""
        pass

    def finalize(self):
        """Finalizes the node. Default implementation does nothing."""
        pass

    def run(self):
        """Main method for running the node code. Subclasses should implement this method."""
        
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
    
    def put(self, obj):
        """Put row into all output pipes. Convenience method."""
        for output in self.outputs:
            output.put(obj)

    def put_record(self, obj):
        """Put record into all output pipes. Convenience method. Not recommended to be used."""
        for output in self.outputs:
            output.put_record(obj)
    

    @property
    def input_fields(self):
        """Return fields from input pipe, if there is one and only one input pipe."""
        return self.input.fields
        
    @property
    def output_fields(self):
        """Return fields passed to the output by the node. Subclasses should override this method.
        Default implementation raises a NotImplementedError."""
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
        see :meth:`brewery.pipes.output_fields`"""
        return self.output_fields.names()

class SourceNode(Node):
    """Abstract class for all source nodes"""
    def __init__(self):
        super(SourceNode, self).__init__()
        self.fields = None
    @property
    def output_fields(self):
        raise NotImplementedError("SourceNode subclasses should implement output_fields")

    def add_input(self, pipe):
        raise Exception("Should not add input pipe to a source node")

class TargetNode(Node):
    """Abstract class for all target nodes"""
    def __init__(self):
        super(TargetNode, self).__init__()
        self.fields = None

    def output_fields(self):
        raise RuntimeError("Output fields asked from a target object.")

    def add_output(self, pipe):
        raise RuntimeError("Should not add output pipe to a target node")
    