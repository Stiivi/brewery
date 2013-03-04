import os.path
import subprocess
import re
from collections import defaultdict, OrderedDict

from ..metadata import *
from .base import *
from .text import CSVDataSource

# type mapping from csv file to sqlalchemy type
type_mapping = {
	"INTEGER" : "integer",
	"SERIAL" : "integer",
	"VARCHAR" : "string",
	"TEXT" : "text",
	"TIMESTAMP" : "time",
	"BOOL" : "boolean",
	"CHAR": "string",
	"INT8": "integer",
	"POSTGRES_UNKNOWN": "text"
}

def mdb_tool(tool_name, args, tools_path=None, universal_newlines=False):
    tool = "mdb-" + tool_name
    if tools_path:
        tool_path = os.path.join(tools_path, tool)
    else:
        tool_path = tool

    #   check if mdb export tool exists
    if not os.path.exists(tool_path):
        raise Exception('mdb tool %s could not be found' % tool_path)

    return subprocess.Popen([tool_path] + args, stdout=subprocess.PIPE,
            universal_newlines=universal_newlines)


class MDBDataStore(DataStore):
    def __init__(self, mdb_file, tools_path=None):
        self.tools_path = tools_path
        self.mdb_file = mdb_file
        self._cached_objects = OrderedDict()
        # TODO: raise exception when the mdb_file does not exist
        # Or ... should we? What about a "promise" that it will exist at the
        # time it will be needed?

    def objects(self, names=None, autoload=False):
        # Parsing code by Daniel Igaz (igipoplegolas)

        if not autoload and self._cached_objects:
            return self._cached_objects.values()

        pipe = mdb_tool("schema", [self.mdb_file, 'postgres'],
                        self.tools_path)

        p_tbl_name = re.compile(r'(CREATE TABLE\s*)(("[^"]+")|(\w+))')
        p_fields = re.compile(r'(?P<name>("[^"]+")|(\w+))\s+(?P<type>\w+)')
        p_tbl_end = re.compile(r'\);')

        tables_found = []

        table_name = None
        #   go through the file line by line
        table_fields = defaultdict(FieldList)
        table_names = []
        for line in pipe.stdout:
            #   browsing through table definition
            if table_name:
                m_fields = p_fields.search(line)
                #   line with field definitions found
                if m_fields:
                    field_name = m_fields.group('name').strip('"')
                    concrete_type = m_fields.group('type').upper()
                    field_type = type_mapping.get(concrete_type)
                    field = Field(name=field_name,
                                    storage_type=field_type,
                                    concrete_storage_type=concrete_type)
                    table_fields[table_name].append(field)
                    continue
                #   end of table, clear table name
                if p_tbl_end.search(line):
                    table_name = None
            else:
                #   check if the line contains table name
                search = p_tbl_name.search(line)
                if search:
                    table_name = search.group(2).strip('"')
                    table_names.append(table_name)

        for name in table_names:
            obj = MDBDataSource(self.mdb_file, name,
                                tools_path=self.tools_path, store=self,
                                fields=table_fields[name])

            self._cached_objects[name] = obj

        return self._cached_objects.values()

    def get_object(self, name):
        if not self._cached_objects:
            # Dum request of objects to fill cache
            self.objects()
        try:
            return self._cached_objects[name]
        except KeyError:
            raise NoSuchObjectError(name)

class MDBDataSource(DataObject):
    def __init__(self, mdb_file, name, fields=None, encoding=None, store=None,
                    tools_path=None):
        self.mdb_file = mdb_file
        self.tools_path = tools_path
        self.name = name
        self.store = store
        self.encoding = encoding
        self.fields = fields

        if not fields:
            # FIXME: fields should be read from the file
            # Currently the only way how to get mdb data source correctly is
            # trough mdb data store.objects() method
            raise DataObjectError("No fields specified")

    def representations(self):
        return ["rows"]

    def rows(self):
        pipe = mdb_tool("export",
                        ['-H', '-b', 'raw', '-D%d/%m/%y', self.mdb_file, self.name],
                        self.tools_path, universal_newlines=True)

        source = CSVDataSource(pipe.stdout, self.fields, encoding=self.encoding)

        for row in source.rows():
            # Treat empty strings as NULLs
            row = [v or None for v in row]
            yield row


