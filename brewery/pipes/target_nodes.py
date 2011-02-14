# -*- coding: utf-8 -*-

import base
import brewery.ds
import sys

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
                 "name": "rows",
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
        self.list = []
        for row in self.input.rows():
            self.list.append(row)
    @property
    def rows(self):
        return self.list        
        
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
                 "name": "records",
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
        self.list = []
        for record in self.input.records():
            self.list.append(record)

    @property
    def records(self):
        return self.list
        
class FormattedPrinterNode(base.TargetNode):
    """Target node that will print output based on format.

    Refer to the python formatting guide:
    
        http://docs.python.org/library/string.html

    Example:
    
    Consider we have a data with information about donations. We want to pretty print two fields:
    `project` and `requested_amount` in the form::
    
        Hlavička - makovička                                            27550.0
        Obecná knižnica - symbol moderného vzdelávania                 132000.0
        Vzdelávanie na európskej úrovni                                 60000.0
    
    Node for given format is created by:
    
    .. code-block:: python
    
        node = FormattedPrinterNode(format = u"{project:<50.50} {requested_amount:>20}")

    """

    __node_info__ = {
        "label" : "Formatted Printer",
        "icong": "formatted_printer_node",
        "description" : "Print input using a string formatter to an output IO stream",
        "attributes" : [
            {
                 "name": "format",
                 "description": "Format string to be used"
            },
            {
                 "name": "output",
                 "description": "IO object. If not set then sys.stdout will be used"
            },
            {
                 "name": "delimiter",
                 "description": "Record delimiter. By default it is new line character."
            }
        ]
    }
    def __init__(self, format = None, output = sys.stdout, delimiter = None):
        super(FormattedPrinterNode, self).__init__()
        self.format = format
        self.output = sys.stdout

        if delimiter:
            self.delimiter = delimiter
        else:
            self.delimiter = '\n'

    def run(self):
        names = self.input_fields.names()

        if self.format:
            format_string = self.format
        else:
            fields = []
            for name in names:
                fields.append("{" + name + "}")
                
            format_string = u"" + "\t".join(fields)
                
            
        for record in self.input.records():
            self.output.write(format_string.format(**record))
            if self.delimiter:
                self.output.write(self.delimiter)

        self.output.flush()