import base
import brewery.ds

class StreamTargetNode(base.TargetNode):
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
    def __init__(self, a_list = None):
        super(RecordListTargetNode, self).__init__()
        if a_list:
            self.list = a_list
        else:
            self.list = []

    def run(self):
        for record in self.input.records():
            self.list.append(record)

