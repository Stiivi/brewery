import base
import brewery.ds

class StreamTargetNode(base.TargetNode):
    """Generic data stream target. Wraps a :mod:`brewery.ds` data target and feeds data from the 
    input to the target stream.

    The data target should match stream fields.

    Note that this node is only for programatically created processing streams. Not useable
    in visual, web or other stream modelling tools.
    """
    
    __node_info__ = {
        "label" : "Data Stream Target",
        "icon": "row_list_target_node",
        "description" : "Generic data stream data target node.",
        "attributes" : [
            {
                 "name": "stream",
                 "description": "Data target object."
            }
        ]
    }
    def __init__(self, stream):
        super(StreamTargetNode, self).__init__()
        self.stream = stream

    def initialize(self):
        # if self.stream_type not in data_sources:
        #     raise ValueError("No data source of type '%s'" % stream_type)
        # stream_info = data_sources[self.stream_type]
        # if "class" not in stream_info:
        #     raise ValueError("No stream class specified for data source of type '%s'" % stream_type)

        # self.stream = stream_class(**kwargs)
        # self.stream.fields = 
        self.stream.initialize()
            
    def run(self):
        for row in self.input.rows():
            stream.append(row)
        
    def finalize(self):
        self.stream.finalize()

class RowListTargetNode(base.TargetNode):
    """Target node that stores data from input in a list of rows (as tuples).
    
    To get list of fields, ask for `output_fields`.
    """

    __node_info__ = {
        "label" : "Row List Target",
        "description" : "Store data as list of tuples",
        "attributes" : [
            {
                 "name": "list",
                 "description": "Created list of tuples."
            }
        ]
    }

    def __init__(self, a_list = None):
        super(RowListTargetNode, self).__init__()
        if a_list:
            self.list = a_list
        else:
            self.list = []

    def run(self):
        for row in self.input.rows():
            self.list.append(row)
        
class RecordListTargetNode(base.TargetNode):
    """Target node that stores data from input in a list of records (dictionary objects)
    object.
    
    To get list of fields, ask for `output_fields`.
    
    """

    __node_info__ = {
        "label" : "Record List Target",
        "description" : "Store data as list of dictionaries (records)",
        "attributes" : [
            {
                 "name": "list",
                 "description": "Created list of records represented as dictionaries."
            }
        ]
    }
    def __init__(self, a_list = None):
        super(RecordListTargetNode, self).__init__()
        if a_list:
            self.list = a_list
        else:
            self.list = []

    def run(self):
        for record in self.input.records():
            self.list.append(record)

