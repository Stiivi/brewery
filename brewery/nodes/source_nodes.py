import base
import brewery.ds as ds
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

    __node_info__ = {
        "label" : "Row List Source",
        "description" : "Provide list of lists or tuples as data source.",
        "attributes" : [
            {
                 "name": "list",
                 "description": "List of rows represented as lists or tuples."
            },
            {
                 "name": "fields",
                 "description": "Fields in the list."
            }
        ]
    }
    def __init__(self, a_list = None, fields = None):
        if a_list:
            self.list = a_list
        else:
            self.list = []
        self.fields = fields
        
    @property
    def output_fields(self):
        if not self.fields:
            raise ValueError("Fields are not initialized")
        return self.fields

    def run(self):
        for row in self.list:
            self.put(row)

class RecordListSourceNode(base.SourceNode):
    """Source node that feeds records (dictionary objects) from a list (or any other iterable)
    object."""

    __node_info__ = {
        "label" : "Record List Source",
        "description" : "Provide list of dict objects as data source.",
        "attributes" : [
            {
                 "name": "list",
                 "description": "List of records represented as dictionaries."
            },
            {
                 "name": "fields",
                 "description": "Fields in the list."
            }
        ]
    }

    def __init__(self, a_list = None, fields = None):
        if a_list:
            self.list = a_list
        else:
            self.list = []
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
    """Generic data stream source. Wraps a :mod:`brewery.ds` data source and feeds data to the 
    output.

    The source data stream should configure fields on initialize().

    Note that this node is only for programatically created processing streams. Not useable
    in visual, web or other stream modelling tools.
    """
    
    __node_info__ = {
        "label" : "Data Stream Source",
        "icon": "row_list_source_node",
        "description" : "Generic data stream data source node.",
        "attributes" : [
            {
                 "name": "stream",
                 "description": "Data stream object."
            }
        ]
    }

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

class CSVSourceNode(base.SourceNode):
    """Source node that reads comma separated file from a filesystem or a remote URL.

    It is recommended to configure node fields before running. If you do not do so, fields are
    read from the file header if specified by `read_header` flag. Field storage types are set to
    `string` and analytical type is set to `typeless`.

    """
    __node_info__ = {
        "label" : "CSV Source",
        "icon": "csv_file_source_node",
        "description" : "Read data from a comma separated values (CSV) file.",
        "attributes" : [
            {
                 "name": "resource",
                 "description": "File name or URL containing comma separated values"
            },
            {
                 "name": "fields",
                 "description": "fields contained in the file",
            },
            {
                 "name": "read_header",
                 "description": "flag determining whether first line contains header or not",
                 "default": "True"
            },
            {
                 "name": "skip_rows",
                 "description": "number of rows to be skipped"
            },
            {
                 "name": "encoding",
                 "description": "resource data encoding, by default no conversion is performed"
            },
            {
                 "name": "delimiter",
                 "description": "record delimiter character, default is comma ','"
            },
            {
                 "name": "quotechar",
                 "description": "character used for quoting string values, default is double quote"
            }
        ]
    }
    def __init__(self, *args, **kwargs):
        super(CSVSourceNode, self).__init__()
        self.args = args
        self.kwargs = kwargs
        self.stream = None
        self._fields = None
        
    @property
    def output_fields(self):
        if not self.stream:
            raise ValueError("Stream is not initialized")

        if not self.stream.fields:
            raise ValueError("Fields are not initialized")

        return self.stream.fields

    def __set_fields(self, fields):
        self._fields = fields
        if self.stream:
            self.stream.fields = fields

    def __get_fields(self):
        return self._fields

    fields = property(__get_fields, __set_fields)

    def initialize(self):
        self.stream = ds.CSVDataSource(*self.args, **self.kwargs)
        
        if self._fields:
            self.stream.fields = self._fields
        
        self.stream.initialize()
        self._fields = self.stream.fields

    def run(self):
        for row in self.stream.rows():
            self.put(row)
            
    def finalize(self):
        self.stream.finalize()

class YamlDirectorySourceNode(base.SourceNode):
    """Source node that reads data from a directory containing YAML files.
    
    The data source reads files from a directory and treats each file as single record. For example,
    following directory will contain 3 records::
    
        data/
            contract_0.yml
            contract_1.yml
            contract_2.yml
    
    Optionally one can specify a field where file name will be stored.
    """
    __node_info__ = {
        "label" : "YAML Directory Source",
        "icon": "yaml_directory_source_node",
        "description" : "Read data from a directory containing YAML files",
        "attributes" : [
            {
                 "name": "path",
                 "description": "Path to a directory"
            },
            {
                 "name": "extension",
                 "description": "file extension to look for, default is yml. If none is given, "
                                "then all regular files in the directory are read.",
                 "default": "yml"
            },
            {
                 "name": "filename_field",
                 "description": "name of a new field that will contain file name",
                 "default": "True"
            }
        ]
    }
    def __init__(self, *args, **kwargs):
        super(YamlDirectorySourceNode, self).__init__()
        self.kwargs = kwargs
        self.args = args
        self.stream = None
        self.fields = None

    @property
    def output_fields(self):
        if not self.stream:
            raise ValueError("Stream is not initialized")

        if not self.stream.fields:
            raise ValueError("Fields are not initialized")

        return self.stream.fields

    def initialize(self):
        self.stream = ds.YamlDirectoryDataSource(*self.args, **self.kwargs)

        self.stream.fields = self.fields
        self.stream.initialize()

    def run(self):
        for row in self.stream.rows():
            # logging.debug("putting yaml row. pipe status: %s" % self.outputs[0].stop_sending)
            self.put(row)

    def finalize(self):
        self.stream.finalize()
