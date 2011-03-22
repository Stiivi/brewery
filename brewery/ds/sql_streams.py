import csv
import codecs
import cStringIO
import base
import inspect

# Soft requirement - graceful fail
try:
    import sqlalchemy

    # (sql type, storage type, analytical type)
    _sql_to_brewery_types = (
        (sqlalchemy.types.UnicodeText, "text",    "typeless"),
        (sqlalchemy.types.Text,        "text",    "typeless"),
        (sqlalchemy.types.Unicode,     "string",  "set"),
        (sqlalchemy.types.String,      "string",  "set"),
        (sqlalchemy.types.Integer,     "integer", "discrete"),
        (sqlalchemy.types.Numeric,     "float",   "range"),
        (sqlalchemy.types.DateTime,    "date",    "typeless"),
        (sqlalchemy.types.Date,        "date",    "typeless"),
        (sqlalchemy.types.Time,        "unknown", "typeless"),
        (sqlalchemy.types.Interval,    "unknown", "typeless"),
        (sqlalchemy.types.Boolean,     "boolean", "flag"),
        (sqlalchemy.types.Binary,      "unknown", "typeless")
    )

    _brewery_to_sql_type = {
        "string": sqlalchemy.types.Unicode,
        "text": sqlalchemy.types.UnicodeText,
        "date": sqlalchemy.types.Date,
        "time": sqlalchemy.types.DateTime,
        "integer": sqlalchemy.types.Integer,
        "float": sqlalchemy.types.Numeric,
        "boolean": sqlalchemy.types.SmallInteger
    }
except:
    _sql_to_brewery_types = []
    _brewery_to_sql_type = {}

def split_table_schema(table_name):
    """Get schema and table name from table reference.

    Returns: Tuple in form (schema, table)
    """

    split = table_name.split('.')
    if len(split) > 1:
        return (split[0], split[1])
    else:
        return (None, split[0])

class SQLDataStore(object):
    def __init__(self, url = None, connection = None, schema = None, **options):
        if connection:
            self.connection = connection
            self.engine = self.connection.engine
            self.close_connection = False
        else:
            self.engine = sqlalchemy.create_engine(url, **options)
            self.connection = self.engine.connect()
            self.close_connection = True
        
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.engine
        self.schema = schema

    def close(self):
        if self.close_connection:
            self.connection.close()

    def dataset(self, name):
        return SQLDataset(self._table(name))

    def has_dataset(self, name):
        table = self._table(name, autoload = False)
        return table.exists()

    def create_dataset(self, name, fields, replace = False,
                       add_id_key = False, id_key_name = None):
        """Create a table."""

        if self.has_dataset(name):
            if not replace:
                raise ValueError("Dataset '%s' already exists" % name)
            else:
                table = self._table(name, autoload = False)
                table.drop(checkfirst=False)

        table = self._table(name, autoload = False)

        if add_id_key:
            if not id_key_name:
                id_key_name = 'id'

            sequence_name = "seq_" + name + "_" + id_key_name
            sequence = sqlalchemy.schema.Sequence(sequence_name, optional = True)
            
            col = sqlalchemy.schema.Column(id_key_name, sqlalchemy.types.Integer, 
                                            sequence, primary_key=True)
            table.append_column(col)

        for field in fields:
            if not isinstance(field, base.Field):
                raise ValueError("field %s is not subclass of brewery.Field" % (field))

            concrete_type = field.concrete_storage_type
            
            if not isinstance(concrete_type, sqlalchemy.types.TypeEngine):
                concrete_type = _brewery_to_sql_type.get(field.storage_type)
                if not concrete_type:
                    raise ValueError("unable to find concrete storage type for field '%s' "
                                     "of type '%s'" % (field.name, field.storage_type))

            col = sqlalchemy.schema.Column(field.name, concrete_type)
            table.append_column(col)

        table.create()

        dataset = SQLDataset(table)
        return dataset

    def _table(self, name, autoload = True):
        split = split_table_schema(name)
        schema = split[0]
        table_name = split[1]

        if not schema:
            schema = self.schema

        table = sqlalchemy.Table(table_name, self.metadata, autoload = autoload, schema = schema)
        return table

class SQLDataset(object):
    def __init__(self, table):
        super(SQLDataset, self).__init__()
        self.table = table
        self._fields = None
        
    @property
    def field_names(self):
        names = [column.name for column in self.table.columns]
        return names
        
    @property
    def fields(self):
        if self._fields:
            return self._fields

        fields = []
        for column in self.table.columns:
            field = base.Field(name = column.name)
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

        self._fields = fields

        return fields

class SQLDataSource(base.DataSource):
    """docstring for ClassName
    """
    def __init__(self, connection = None, url = None,
                    table = None, statement = None, schema = None, **options):
        """Creates a relational database data source stream.
        
        :Attributes:
            * url: SQLAlchemy URL - either this or connection should be specified
            * connection: SQLAlchemy database connection - either this or url should be specified
            * table: table name
            * statement: SQL statement to be used as a data source (not supported yet)
            * options: SQL alchemy connect() options
        
        Note: avoid auto-detection when you are reading from remote URL stream.
        
        """
        if not url and not connection:
            raise AttributeError("Either url or connection should be provided for SQL data source")

        if not table and not statement:
            raise AttributeError("Either table or statement should be provided for SQL data source")

        if statement:
            raise NotImplementedError("SQL source stream based on statement is not yet implemented")

        if not options:
            options = {}

        self.url = url
        self.connection = connection
        self.table_name = table
        self.statement = statement
        self.schema = schema
        self.options = options
        
        self._fields = None
                
    def initialize(self):
        """Initialize source stream:
        """
        self.datastore = SQLDataStore(self.url, self.connection, self.schema, **self.options)
        self.dataset = self.datastore.dataset(self.table_name)

    def finalize(self):
        self.datastore.close()

    @property
    def fields(self):
        if self._fields:
            return self._fields
        self._fields = self.dataset.fields
        return self._fields

    def read_fields(self):
        self._fields = self.dataset.fields
        return self._fields

    def rows(self):
        if not self.dataset:
            raise RuntimeError("Stream is not initialized")
        return self.dataset.table.select().execute()

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
    def __init__(self, connection = None, url = None,
                    table = None, schema = None, truncate = False, 
                    create = False, replace = False,
                    add_id_key = False, id_key_name = None, 
                    buffer_size = None, **options):
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
        
        Note: avoid auto-detection when you are reading from remote URL stream.
        
        """
        if not url and not connection:
            raise AttributeError("Either url or connection should be provided for SQL data source")

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

        self._fields = None
                
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

        self.datastore = SQLDataStore(self.url, self.connection, self.schema, **self.options)

        if self.create:
            self.dataset = self.datastore.create_dataset(self.table_name, 
                                                         self.fields, 
                                                         self.replace, 
                                                         self.add_id_key, self.id_key_name)
        else:
            self.dataset = self.datastore.dataset(self.table_name)
            
        if self.truncate:
            self.dataset.table.delete().execute()

        if not self._fields:
            self._fields = self.dataset.fields

        self.insert_command = self.dataset.table.insert()
        self._buffer = []

    def finalize(self):
        """Closes the stream, flushes buffered data"""
        
        self._flush()
        self.datastore.close()

    def __get_fields(self):
        return self._fields

    def __set_fields(self, fields):
        self._fields = fields

    fields = property(__get_fields, __set_fields)

    def __append(self, obj):
        if type(obj) == dict:
            record = obj
        else:
            record = dict(zip(self.field_names, obj))
            # if self.add_id_key:
            #     record[self.id_key_name] = None
        self.insert_command.execute(record)

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
            self.datastore.connection.execute(self.insert_command, self._buffer)
            self._buffer = []
