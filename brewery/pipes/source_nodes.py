import base
import brewery.ds
import logging

# data_sources = {
#     "csv": {"class": CSVDataSource},
#     "xls": {"class": XLSDataSource},
#     "yamldir": {"class": YamlDirectoryDataSource},
#     "mongodb": {"class": MongoDBDataSource},
#     "sql": {"class": SQLDataSource}
# }

class RowListSourceNode(base.SourceNode):
    """Source node that feeds rows (list/tuple of values) from a list (or any other iterable)
    object."""
    def __init__(self, a_list = [], fields = None):
        self.list = a_list
        self.fields = fields
        
    @property
    def output_fields(self):
        if not self.fields:
            raise ValueError("Fields are not initialized")
        return self.fields

    def run(self):
        for row in self.list:
            logging.debug("generating row: %s" % row)
            self.put(row)

class RecordListSourceNode(base.SourceNode):
    """Source node that feeds records (dictionary objects) from a list (or any other iterable)
    object."""

    def __init__(self, a_list = [], fields = None):
        self.list = a_list
        self.fields = fields

    @property
    def output_fields(self):
        if not self.fields:
            raise ValueError("Fields are not initialized")
        return self.fields

    def run(self):
        for record in self.list:
            self.put_record(record)
        
    
class StreamSourceNode(base.SourceNode):
    def __init__(self, stream):
        super(StreamSourceNode, self).__init__()
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

    @property
    def output_fields(self):
        return self.stream.fields
        
    def run(self):
        for row in stream.rows():
            self.put(row)
        
    def finalize(self):
        self.stream.finalize()

