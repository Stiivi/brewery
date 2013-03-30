from ..nodes import SourceNode
from .objects import CSVDataSource

class CSVSourceNode(SourceNode):
    """Source node that reads comma separated file from a filesystem or a remote URL.

    It is recommended to configure node fields before running. If you do not do so, fields are
    read from the file header if specified by `read_header` flag. Field storage types are set to
    `string` and analytical type is set to `typeless`.

    """
    node_info = {
        "label" : "CSV Source",
        "icon": "csv_file_source_node",
        "description" : "Read data from a comma separated values (CSV) file.",
        "attributes" : [
            {
                 "name": "resource",
                 "description": "File name or URL containing comma separated values",
            },
            {
                 "name": "fields",
                 "description": "fields contained in the file",
                 "type": "fields"
            },
            {
                 "name": "read_header",
                 "description": "flag determining whether first line contains header or not",
                 "type": "flag",
                 "default": "True"
            },
            {
                 "name": "skip_rows",
                 "description": "number of rows to be skipped",
                 "type": "flag"
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
    def __init__(self, resource=None, *args, **kwargs):
        super(CSVSourceNode, self).__init__()
        self.resource = resource
        self.args = args
        self.kwargs = kwargs

    # def initialize(self):
    #     self.stream = CSVDataSource(self.resource, *self.args, **self.kwargs)

    #     if self.fields:
    #         self.stream.fields = self.fields

    #     self.stream.initialize()

    #     self.output_fields = self.stream.fields.copy()
    #     # self._output_fields.retype(self._retype_dictionary)
    #     for field in self.output_fields:
    #         field.origin = self
    #         field.freeze()

    def evaluate(self, context, sources=None):
        return CSVDataSource(self.resource, *self.args, **self.kwargs)

