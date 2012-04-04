#!/usr/bin/env python
# -*- coding: utf-8 -*-

import base
import brewery.metadata

try:
    import sqlalchemy

    # (sql type, storage type, analytical type)
    _sql_to_brewery_types = (
        (sqlalchemy.types.UnicodeText, "text", "typeless"),
        (sqlalchemy.types.Text, "text", "typeless"),
        (sqlalchemy.types.Unicode, "string", "set"),
        (sqlalchemy.types.String, "string", "set"),
        (sqlalchemy.types.Integer, "integer", "discrete"),
        (sqlalchemy.types.Numeric, "float", "range"),
        (sqlalchemy.types.DateTime, "date", "typeless"),
        (sqlalchemy.types.Date, "date", "typeless"),
        (sqlalchemy.types.Time, "unknown", "typeless"),
        (sqlalchemy.types.Interval, "unknown", "typeless"),
        (sqlalchemy.types.Boolean, "boolean", "flag"),
        (sqlalchemy.types.Binary, "unknown", "typeless")
    )

    concrete_sql_type_map = {
        "string": sqlalchemy.types.Unicode,
        "text": sqlalchemy.types.UnicodeText,
        "date": sqlalchemy.types.Date,
        "time": sqlalchemy.types.DateTime,
        "integer": sqlalchemy.types.Integer,
        "float": sqlalchemy.types.Numeric,
        "boolean": sqlalchemy.types.SmallInteger
    }
except:
    from brewery.utils import MissingPackage
    sqlalchemy = MissingPackage("sqlalchemy", "SQL streams", "http://www.sqlalchemy.org/",
                                comment = "Recommended version is > 0.7")
    _sql_to_brewery_types = ()
    concrete_sql_type_map = {}

def split_table_schema(table_name):
    """Get schema and table name from table reference.

    Returns: Tuple in form (schema, table)
    """

    split = table_name.split('.')
    if len(split) > 1:
        return (split[0], split[1])
    else:
        return (None, split[0])

        
class SQLContext(object):
    """Holds context of SQL store operations."""
    
    def __init__(self, url = None, connection = None, schema = None):
        """Creates a SQL context"""

        if not url and not connection:
            raise AttributeError("Either url or connection should be provided" \
                                 " for SQL data source")

        super(SQLContext, self).__init__()

        if connection:
            self.connection = connection
            self.should_close = False
        else:
            engine = sqlalchemy.create_engine(url)
            self.connection = engine.connect()
            self.should_close = True
            
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.connection.engine
        self.schema = schema
    
    def close(self):
        if self.should_close and self.connection:
            self.connection.close()
            
    def table(self, name, autoload=True):
        """Get table by name"""
        
        return sqlalchemy.Table(name, self.metadata, 
                                autoload=autoload, schema=self.schema)

def fields_from_table(table):
    """Get fields from a table. Field types are normalized to the Brewery
    data types. Analytical type is set according to a default conversion
    dictionary."""
    
    fields = []

    for column in table.columns:
        field = brewery.metadata.Field(name=column.name)
        field.concrete_storage_type = column.type

        for conv in _sql_to_brewery_types:
            if issubclass(column.type.__class__, conv[0]):
                field.storage_type = conv[1]
                field.analytical_type = conv[2]
                break

        if not field.storage_type:
            field.storaget_tpye = "unknown"

        if not field.analytical_type:
            field.analytical_type = "unknown"

        fields.append(field)

    return brewery.metadata.FieldList(fields)

def concrete_storage_type(field, type_map={}):
    """Derives a concrete storage type for the field based on field conversion
       dictionary"""

    concrete_type = field.concrete_storage_type
        
    if not isinstance(concrete_type, sqlalchemy.types.TypeEngine):
        if type_map:
            concrete_type = type_map.get(field.storage_type)

        if not concrete_type:
            concrete_type = concrete_sql_type_map.get(field.storage_type)
        
        if not concrete_type:
            raise ValueError("unable to find concrete storage type for field '%s' "
                             "of type '%s'" % (field.name, field.storage_type))

    return concrete_type

class SQLDataSource(base.DataSource):
    """docstring for ClassName
    """
    def __init__(self, connection=None, url=None,
                    table=None, statement=None, schema=None, autoinit = True,
                    **options):
        """Creates a relational database data source stream.
        
        :Attributes:
            * url: SQLAlchemy URL - either this or connection should be specified
            * connection: SQLAlchemy database connection - either this or url should be specified
            * table: table name
            * statement: SQL statement to be used as a data source (not supported yet)
            * autoinit: initialize on creation, no explicit initialize() is 
              needed
            * options: SQL alchemy connect() options
        """

        super(SQLDataSource, self).__init__()

        if not table and not statement:
            raise AttributeError("Either table or statement should be " \
                                 "provided for SQL data source")

        if statement:
            raise NotImplementedError("SQL source stream based on statement " \
                                      "is not yet implemented")

        if not options:
            options = {}

        self.url = url
        self.connection = connection

        self.table_name = table
        self.statement = statement
        self.schema = schema
        self.options = options

        self.context = None
        self.table = None
        self.fields = None
        
        if autoinit:
            self.initialize()

    def initialize(self):
        """Initialize source stream. If the fields are not initialized, then
        they are read from the table.
        """
        if not self.context:
            self.context = SQLContext(self.url, self.connection, self.schema)
        if not self.table:
            self.table = self.context.table(self.table_name)
        if not self.fields:
            self.read_fields()

    def finalize(self):
        self.context.close()

    def read_fields(self):
        self.fields = fields_from_table(self.table)
        return self.fields

    def rows(self):
        if not self.dataset:
            raise RuntimeError("Stream is not initialized")
        return self.context.table.select().execute()

    def records(self):
        if not self.dataset:
            raise RuntimeError("Stream is not initialized")
        fields = self.field_names
        for row in self.rows():
            record = dict(zip(fields, row))
            yield record

class SQLDataTarget(base.DataTarget):
    """docstring for ClassName
    """
    def __init__(self, connection=None, url=None,
                    table=None, schema=None, truncate=False,
                    create=False, replace=False,
                    add_id_key=False, id_key_name=None,
                    buffer_size=None, fields=None, concrete_type_map=None,
                    **options):
        """Creates a relational database data target stream.
        
        :Attributes:
            * url: SQLAlchemy URL - either this or connection should be specified
            * connection: SQLAlchemy database connection - either this or url should be specified
            * table: table name
            * truncate: whether truncate table or not
            * create: whether create table on initialize() or not
            * replace: Set to True if creation should replace existing table or not, otherwise
              initialization will fail on attempt to create a table which already exists.
            * options: other SQLAlchemy connect() options
            * add_id_key: whether to add auto-increment key column or not. Works only if `create`
              is ``True``
            * id_key_name: name of the auto-increment key. Default is 'id'
            * buffer_size: size of INSERT buffer - how many records are collected before they are
              inserted using multi-insert statement. Default is 1000
            * fields : fieldlist for a new table
        
        Note: avoid auto-detection when you are reading from remote URL stream.
        
        """
        if not options:
            options = {}

        self.url = url
        self.connection = connection
        self.table_name = table
        self.schema = schema
        self.options = options
        self.replace = replace
        self.create = create
        self.truncate = truncate
        self.add_id_key = add_id_key

        self.table = None
        self.fields = fields
        
        self.concrete_type_map = concrete_type_map

        if id_key_name:
            self.id_key_name = id_key_name
        else:
            self.id_key_name = 'id'

        if buffer_size:
            self.buffer_size = buffer_size
        else:
            self.buffer_size = 1000

    def initialize(self):
        """Initialize source stream:
        """

        self.context = SQLContext(url=self.url, 
                                  connection=self.connection,
                                  schema=self.schema)

        if self.create:
            self.table = self._create_table()
        else:
            self.table = self.context.table(self.table_name)

        if self.truncate:
            self.table.delete().execute()

        if not self.fields:
            self.fields = fields_from_table(self.table)
        
        self.field_names = self.fields.names()

        self.insert_command = self.table.insert()
        self._buffer = []

    def _create_table(self):
        """Create a table."""

        if not self.fields:
            raise Exception("Can not create a table: No fields provided")

        table = self.context.table(self.table_name, autoload=False)

        if table.exists():
            if self.replace:
                table = self.context.table(self.table_name, autoload=False)
                table.drop(checkfirst=False)
            else:
                raise ValueError("Table '%s' already exists" % self.table_name)

        table = sqlalchemy.Table(self.table_name, self.context.metadata)

        if self.add_id_key:
            id_key_name = self.id_key_name or 'id'

            sequence_name = "seq_" + name + "_" + id_key_name
            sequence = sqlalchemy.schema.Sequence(sequence_name, optional=True)

            col = sqlalchemy.schema.Column(id_key_name,
                                           sqlalchemy.types.Integer,
                                           sequence, primary_key=True)
            table.append_column(col)

        for field in self.fields:
            # FIXME: hey, what about duck-typing?
            if not isinstance(field, brewery.metadata.Field):
                raise ValueError("field %s is not subclass of brewery.metadata.Field" % (field))

            concrete_type = concrete_storage_type(field, self.concrete_type_map)

            col = sqlalchemy.schema.Column(field.name, concrete_type)
            table.append_column(col)

        table.create()

        return table


    def finalize(self):
        """Closes the stream, flushes buffered data"""

        self._flush()
        self.context.close()

    def append(self, obj):
        if type(obj) == dict:
            record = obj
        else:
            record = dict(zip(self.field_names, obj))

        self._buffer.append(record)
        if len(self._buffer) >= self.buffer_size:
            self._flush()

    def _flush(self):
        if len(self._buffer) > 0:
            self.context.connection.execute(self.insert_command, self._buffer)
            self._buffer = []
