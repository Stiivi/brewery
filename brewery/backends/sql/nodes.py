# -*- coding: utf-8 -*-

from ...nodes import *

class SQLSourceNode(Node):
    """Source node that reads from a sql table.
    """
    node_info = {
        "label" : "SQL Source",
        "icon": "sql_source_node",
        "description" : "Read data from a sql table.",
        "attributes" : [
            {
                 "name": "uri",
                 "description": "SQLAlchemy URL"
            },
            {
                 "name": "table",
                 "description": "table name",
            },
        ]
    }
    def __init__(self, *args, **kwargs):
        super(SQLSourceNode, self).__init__()
        self.args = args
        self.kwargs = kwargs
        self.source = None

    def evaluate(self, context, sources=None):
         return SQLTable(*self.args, **self.kwargs)


class SQLTableTargetNode(Node):
    """Feed data rows into a relational database table.
    """
    node_info = {
        "label": "SQL Table Target",
        "icon": "sql_table_target",
        "description" : "Feed data rows into a relational database table",
        "attributes" : [
            {
                 "name": "url",
                 "description": "Database URL in form: adapter://user:password@host/database"
            },
            {
                 "name": "connection",
                 "description": "SQLAlchemy database connection - either this or url should be specified",
            },
            {
                 "name": "table",
                 "description": "table name"
            },
            {
                 "name": "truncate",
                 "description": "If set to ``True`` all data table are removed prior to node "
                                "execution. Default is ``False`` - data are appended to the table"
            },
            {
                 "name": "create",
                 "description": "create table if it does not exist or not"
            },
            {
                 "name": "replace",
                 "description": "Set to True if creation should replace existing table or not, "
                                "otherwise node will fail on attempt to create a table which "
                                "already exists"
            },
            {
                "name": "buffer_size",
                "description": "how many records are collected before they are "
                              "inserted using multi-insert statement. "
                              "Default is 1000"
            },
            {
                 "name": "options",
                 "description": "other SQLAlchemy connect() options"
            }
        ]
    }

    def __init__(self, *args, **kwargs):

        super(SQLTableTargetNode, self).__init__()
        self.args = args
        self.kwargs = kwargs

    def evaluate(self, context, sources=None):
        source = sources[0]
        target = SQLTable(*self.args,
                               fields=source.fields,
                               **self.kwargs)
        target.append_from(source)
        target.flush()



# Original name is depreciated
DatabaseTableTargetNode = SQLTableTargetNode
