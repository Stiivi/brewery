#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import absolute_import
from .base import Node
from ..objects import IterableDataSource, IterableRecordsDataSource

from ..backends.sql import SQLTable
from ..backends.xls import XLSDataSource

# FIXME: change this to data objects
from ..ds.elasticsearch_streams import ESDataSource
from ..ds.gdocs_streams import GoogleSpreadsheetDataSource
from ..ds.yaml_dir_streams import YamlDirectoryDataSource

class RowListSourceNode(Node):
    """Source node that feeds rows (list/tuple of values) from a list (or any other iterable)
    object."""

    node_info = {
        "label" : "Row List Source",
        "description" : "Provide list of lists or tuples as data source.",
        "protected": True,
        "attributes" : [
            {
                 "name": "data",
                 "description": "List of rows represented as lists or tuples."
            },
            {
                 "name": "fields",
                 "description": "Fields in the list."
            }
        ]
    }
    def __init__(self, data=None, fields=None):
        if data is not None:
            self.data = data
        else:
            self.data = []
        self.fields = fields

    def evaluate(self, context, sources=None):
        return IterableDataSource(self.data, self.fields)


class RecordListSourceNode(Node):
    """Source node that feeds records (dictionary objects) from a list (or any other iterable)
    object."""

    node_info = {
        "label" : "Record List Source",
        "description" : "Provide list of dict objects as data source.",
        "protected": True,
        "attributes" : [
            {
                 "name": "data",
                 "description": "List of records represented as dictionaries."
            },
            {
                 "name": "fields",
                 "description": "Fields in the list."
            }
        ]
    }

    def __init__(self, data=None, fields=None):
        super(RecordListSourceNode, self).__init__()
        if data is not None:
            self.data = data
        else:
            self.data = []
        self.fields = fields

    def evaluate(self, context, sources=None):
        return IterableRecordsDataSource(self.data, self.fields)

class DataObjectSourceNode(Node):
    """Generic data object source. Wraps a :mod:`brewery.objects` objects,
    output is the wrapped object.

    Note that this node is only for programatically created processing
    streams. Not useable in visual, web or other stream modelling tools.
    """

    node_info = {
        "label" : "Data Object Source",
        "icon": "row_list_source_node",
        "description" : "Generic source node for any data object.",
        "protected": True,
        "attributes" : [
            {
                 "name": "obj",
                 "description": "Data object."
            }
        ]
    }

    def __init__(self, source):
        super(DataObjectSourceNode, self).__init__()
        self.source = source

    def evaluate(self, context, sources=None):
        return self.source

class XLSSourceNode(Node):
    """Source node that reads Excel XLS files.

    It is recommended to configure node fields before running. If you do not do so, fields are
    read from the file header if specified by `read_header` flag. Field storage types are set to
    `string` and analytical type is set to `typeless`.

    """
    node_info = {
        "label" : "XLS Source",
        "icon": "xls_file_source_node",
        "description" : "Read data from an Excel (XLS) spreadsheet file.",
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
                 "name": "sheet",
                 "description": "Sheet index number (as int) or sheet name (as string)"
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
            }
        ]
    }
    def __init__(self, *args, **kwargs):
        super(XLSSourceNode, self).__init__()
        self.args = args
        self.kwargs = kwargs

    def evaluate(self, context, sources=None):
        return XLSDataSource(*self.args, **self.kwargs)


class YamlDirectorySourceNode(Node):
    """Source node that reads data from a directory containing YAML files.

    The data source reads files from a directory and treats each file as single record. For example,
    following directory will contain 3 records::

        data/
            contract_0.yml
            contract_1.yml
            contract_2.yml

    Optionally one can specify a field where file name will be stored.
    """
    node_info = {
        "label" : "YAML Directory Source",
        "icon": "yaml_directory_source_node",
        "description" : "Read data from a directory containing YAML files",
        "protected": True,
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
        self.stream = YamlDirectoryDataSource(*self.args, **self.kwargs)

        self.stream.fields = self.fields
        self.stream.initialize()

    def run(self):
        for row in self.stream.rows():
            # logging.debug("putting yaml row. pipe status: %s" % self.outputs[0].stop_sending)
            self.put(row)

    def finalize(self):
        self.stream.finalize()

class GoogleSpreadsheetSourceNode(Node):
    """Source node that reads Google Spreadsheet.

    You should provide either spreadsheet_key or spreadsheet_name, if more than one spreadsheet with
    given name are found, then the first in list returned by Google is used.

    For worksheet selection you should provide either worksheet_id or worksheet_name. If more than
    one worksheet with given name are found, then the first in list returned by Google is used. If
    no worksheet_id nor worksheet_name are provided, then first worksheet in the workbook is used.

    For details on query string syntax see the section on sq under
    http://code.google.com/apis/spreadsheets/reference.html#list_Parameters
    """
    node_info = {
        "label" : "Google Spreadsheet Source",
        "icon": "google_spreadsheet_source_node",
        "description" : "Read data from a Google Spreadsheet.",
        "attributes" : [
            {
                 "name": "spreadsheet_key",
                 "description": "The unique key for the spreadsheet"
            },
            {
                 "name": "spreadsheet_name",
                 "description": "The title of the spreadsheets",
            },
            {
                 "name": "worksheet_id",
                 "description": "ID of a worksheet"
            },
            {
                 "name": "worksheet_name",
                 "description": "name of a worksheet"
            },
            {
                 "name": "query_string",
                 "description": "optional query string for row selection"
            },
            {
                 "name": "username",
                 "description": "Google account user name"
            },
            {
                 "name": "password",
                 "description": "Google account password"
            }
        ]
    }
    def __init__(self, *args, **kwargs):
        super(GoogleSpreadsheetSourceNode, self).__init__()
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

    def __getattr__(self, key):
        try:
            return getattr(self.stream, key)
        except AttributeError:
            return object.__getattr__(self, key)

    def __set_fields(self, fields):
        self._fields = fields
        if self.stream:
            self.stream.fields = fields

    def __get_fields(self):
        return self._fields

    fields = property(__get_fields, __set_fields)

    def initialize(self):
        self.stream = GoogleSpreadsheetDataSource(*self.args, **self.kwargs)

        if self._fields:
            self.stream.fields = self._fields

        self.stream.initialize()
        self._fields = self.stream.fields

    def run(self):
        for row in self.stream.rows():
            self.put(row)

    def finalize(self):
        self.stream.finalize()


class ESSourceNode(Node):
    """Source node that reads from an ElasticSearch index.

    See ElasticSearch home page for more information:
    http://www.elasticsearch.org/
    """

    node_info = {
        "label" : "ElasticSearch Source",
        "icon": "generic_node",
        "description" : "Read data from ElasticSearch engine",
        "attributes" : [
            {
                "name": "document_type",
                "description": "ElasticSearch document type name"
            },
            {
                "name": "expand",
                "description": "expand dictionary values and treat children as "\
                " top-level keys with dot '.' separated key path to the child"
            },
            {
                "name": "database",
                "description": "database name"
            },
            {
                "name": "host",
                "description": "database server host, default is localhost"
            },
            {
                "name": "port",
                "description": "database server port, default is 27017"
            }
        ]
    }
    def __init__(self, *args, **kwargs):
        super(ESSourceNode, self).__init__()
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
        self.stream = ESDataSource(*self.args, **self.kwargs)
        self.stream.initialize()
        self._fields = self.stream.fields

    def run(self):
        for row in self.stream.rows():
            self.put(row)

    def finalize(self):
        self.stream.finalize()

class GeneratorFunctionSourceNode(Node):
    """Source node uses a callable to generate records."""

    node_info = {
        "label" : "Callable Generator Source",
        "description" : "Uses a callable as record generator",
        "protected": True,
        "attributes" : [
            {
                 "name": "function",
                 "description": "Function (or any callable)"
            },
            {
                 "name": "fields",
                 "description": "Fields the function generates"
            },
            {
                 "name": "args",
                 "description": "Function arguments"
            },
            {
                 "name": "kwargs",
                 "description": "Function key-value arguments"
            }
        ]
    }

    def __init__(self, function=None, fields=None, *args, **kwargs):
        super(GeneratorFunctionSourceNode, self).__init__()

        self.function = function
        self.fields = fields
        self.args = args
        self.kwargs = kwargs

    def evaluate(self, context, sources=None):
        i = self.function(*self.args, **self.kwargs)
        return IteratorDataObject(i, fields)

