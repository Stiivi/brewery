# -*- coding: utf-8 -*-
from .base import *
from ..errors import *
import brewery.metadata

__all__ = (
        "SQLDataStore",
        "SQLDataSource",
        "SQLDataTarget"
    )

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

    from sqlalchemy.sql.expression import Executable, ClauseElement
    from sqlalchemy.ext.compiler import compiles

    class CreateTableAsSelect(Executable, ClauseElement):
        def __init__(self, table, select):
            self.table = table
            self.select = select

    @compiles(CreateTableAsSelect)
    def visit_create_table_as_select(element, compiler, **kw):
        return "CREATE TABLE %s AS (%s)" % (
            element.table,
            compiler.process(element.select)
        )

    class InsertIntoAsSelect(Executable, ClauseElement):
        def __init__(self, table, select):
            self.table = table
            self.select = select

    @compiles(InsertIntoAsSelect)
    def visit_insert_into_as_select(element, compiler, **kw):
        return "INSERT INTO %s %s" % (
            compiler.process(element.table, asfrom=True),
            compiler.process(element.select)
        )

except:
    from brewery.common import MissingPackage
    sqlalchemy = MissingPackage("sqlalchemy", "SQL streams", "http://www.sqlalchemy.org/",
                                comment = "Recommended version is > 0.7")
    _sql_to_brewery_types = ()
    concrete_sql_type_map = {}


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

"""List of default shared stores."""
_default_stores = {}

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

def default_store(connectable=None, url=None, schema=None):
    """Gets a default store for connectable or URL. If store does not exist
    one is created and added to shared default store pool."""

    if url and connectable:
        raise ArgumentError("Only one of url or connectable should be " \
                            "specified, not both")

    if url:
        try:
            store = _default_stores[url]
        except KeyError:
            store = SQLDataStore(url=url, schema=schema)
            _default_stores[url] = store
            _default_stores[store.connectable] = store
    else:
        try:
            store = _default_store[connectable]
        except KeyError:
            store = SQLDataStore(connectable=connectable)
            _default_stores[store.connectable] = store

    return store


class SQLDataStore(object):
    """Holds context of SQL store operations."""

    def __init__(self, url=None, connectable=None, schema=None,
            concrete_type_map=None):
        """Opens a SQL data store"""

        if not url and not connectable:
            raise AttributeError("Either url or connectable should be provided" \
                                 " for SQL data source")

        super(SQLDataStore, self).__init__()

        if connectable:
            self.connectable = connectable
            self.should_close = False
        else:
            self.connectable = sqlalchemy.create_engine(url)
            self.should_close = True

        self.concrete_type_map = concrete_type_map or concrete_sql_type_map

        self.metadata = sqlalchemy.MetaData(bind=self.connectable)
        self.schema = schema

    def close(self):
        """Closes data store."""
        pass

    def objects(self, names=None):
        """Return list of tables and views.

        * `names`: only objects with given names are returned
        """

        self.metadata.reflect(schema=self.schema, views=True, only=names)
        tables = self.metadata.sorted_tables

        objects = []
        for table in tables:
            obj = SQLDataObject(table=table, connectable=self.connectable,
                                schema=self.schema, store=self)
            objects.append(obj)

        return objects

    def create(self, name, fields, replace=False, from_obj=None, schema=None,
               id_column=None):
        """Creates a table and returns `SQLTargetObject`. See `create_table()`
        for more information"""
        table = self.create_table(name, fields=fields, replace=replace,
                                  from_obj=from_obj, schema=schema,
                                  id_column=id_column)
        return SQLDataTarget(store=self, table=table, schema=schema)

    def create_table(self, name, fields, replace=False, from_obj=None, schema=None,
               id_column=None):
        """Creates a new table.

        * `fields`: field list for new columns
        * `replace`: if table exists, it will be dropped, otherwise an
          exception is raised
        * `from_obj`: object with SQL selectable compatible representation
          (table or statement)
        * `schema`: schema where new table is created. When ``None`` then
          store's default schema is used.
        """

        schema = schema or self.schema

        table = self.table(name, schema, autoload=False)
        if table.exists():
            if replace:
                self.delete(name, schema)
                # Create new table object
                table = self.table(name, schema, autoload=False)
            else:
                schema_str = " (in schema '%s')" % schema if schema else ""
                raise Exception("Table %s%s already exists" % (table, schema_str))

        if from_obj:
            if id_column:
                raise Exception("id_column should not be specified when "
                                "creating table from another object")

            return self._create_table_from(table, from_obj)
        elif id_column:
            sequence_name = "seq_%s_%s" % (name, id_column)
            sequence = sqlalchemy.schema.Sequence(sequence_name,
                                                  optional=True)
            col = sqlalchemy.schema.Column(id_column,
                                           sqlalchemy.types.Integer,
                                           sequence, primary_key=True)
            table.append_column(col)

        for field in fields:
            concrete_type = concrete_storage_type(field, self.concrete_type_map)
            if field.name == id_column:
                sequence_name = "seq_%s_%s" % (name, id_column)
                sequence = sqlalchemy.schema.Sequence(sequence_name,
                                                      optional=True)
                col = sqlalchemy.schema.Column(id_key_name,
                                           concrete_type,
                                           sequence, primary_key=True)
            else:
                col = sqlalchemy.schema.Column(field.name, concrete_type)

            table.append_column(col)

        table.create()

        return table

    def _create_table_from(table, from_obj):
        """Creates a table using ``CREATE TABLE ... AS SELECT ...``. The
        `from_obj` should have SQL selectable compatible representation."""

        source = selectable_from_object(from_obj, self.store)
        statement = CreateTableAsSelect(table, source)
        self.connectable.execute(statement)
        return self.table(name=table, autoload=True)

    def delete(self, name, schema):
        """Drops table"""
        schema = schema or self.schema
        table = self.table(name, schema, autoload=False)
        if not table.exists():
            raise Exception("Trying to delete table '%s' that does not exist" \
                                                                    % name)
        table.drop(checkfirst=False)
        self.metadata.drop_all(tables=[table])
        self.metadata.remove(table)

    def table(self, table, schema=None, autoload=True):
        """Returns a table with `name`. If schema is not provided, then
        store's default schema is used."""
        if table is None:
            raise Exception("Table name should not be None")
        if isinstance(table, sqlalchemy.schema.Table):
            return table

        schema = schema or self.schema

        return sqlalchemy.Table(table, self.metadata,
                                autoload=autoload, schema=schema)

    def execute(self, statement, *args, **kwargs):
        """Executes `statement` in store's connectable"""
        return self.connectable.execute(statement, *args, **kwargs)

def selectable_from_object(obj, store):
    """Returns SQL selectable statement from object `obj` compatible with
    store `store`."""
    if not hasattr(obj, "store"):
        raise DataObjectError("Object %s does not look like SQL data object - "
                              "it has no store attribute" % str(obj))
    if obj.store != store:
        raise DataObjectError("Can not get selectable from object in different store.")

    reps = obj.representations()

    if "sql_table" in reps:
        selectable = obj.sql_table().select()
    elif "sql_statement" in reps:
        selectable = obj.sql_statement()
    else:
        raise Exception("Object %s does not have SQL selectable compatible "
                        "representation (table or statement)" % obj)

    return selectable

class SQLDataSource(DataObject):
    """docstring for ClassName
    """
    def __init__(self, connectable=None, url=None,
                        table=None, statement=None, schema=None,
                        store=None, **options):
        """Creates a relational database data object.

        Attributes:

        * `url`: SQLAlchemy URL - either this or connection should be specified
        * `connectable`: SQLAlchemy database connectable (engine or
          connection) - either this or url should be specified
        * `table`: table name
        * `statement`: SQL statement to be used as a data source (not
          supported yet)
        * `options`: SQL alchemy connect() options
        * `store`: SQL data store the object belongs to

        If `store` is not provided, then default store is used for given
        connectable or URL. If no store exists, one is created.
        """
        # FIXME: URL should be depreciated in favor of data store

        super(SQLDataSource, self).__init__()

        if table is None and statement is None:
            raise ArgumentError("Either table name or statement should be " \
                                 "provided for SQL data source")
        if not options:
            options = {}

        if store:
            self.store = store
        else:
            self.store = default_store(url=url, connectable=connectable,
                                        schema=schema)

        if isinstance(table, basestring):
            self.table = self.store.table(table, schema=schema, autoload=True)
        elif table is not None:
            self.table = table

        if table is not None:
            self.name = self.table.name
        else:
            self.name = None

        if statement is not None:
            self.statement = statement
        else:
            self.statement = self.table

        self.schema = schema
        self.options = options

        self._reflect_fields()

    def _reflect_fields(self):
        """Get fields from a table. Field types are normalized to the Brewery
        data types. Analytical type is set according to a default conversion
        dictionary."""

        fields = []

        for column in self.table.columns:
            field = brewery.Field(name=column.name)
            field.concrete_storage_type = column.type

            for conv in _sql_to_brewery_types:
                if issubclass(column.type.__class__, conv[0]):
                    field.storage_type = conv[1]
                    field.analytical_type = conv[2]
                    break

            field.storage_type = field.storage_type or "unknown"
            field.analytical_type = field.analytical_type or "unknown"

            fields.append(field)

        self.fields = brewery.FieldList(fields)

    def rows(self):
        return iter(self.statement.execute())

    def sql_statement(self):
        return self.statement

    def sql_table(self):
        return self.table

    def records(self):
        fields = self.fields.names()
        for row in self.rows():
            record = dict(zip(fields, row))
            yield record

    def representations(self):
        """Return list of possible object representations"""
        if self.table is not None:
            return ["rows", "sql_statement", "sql_table"]
        else:
            return ["rows", "sql_statement"]

    def __len__(self):
        """Returns number of rows in a table"""
        if self.table is not None:
            statement = self.table.count()
        else:
            statement = self.statement.count()
        result = self.store.connectable.scalar(statement)
        return result

class SQLDataTarget(object):
    """docstring for ClassName
    """
    def __init__(self, connectable=None, url=None, store=None,
                    table=None, schema=None, truncate=False,
                    create=False, replace=False, id_key_name=None,
                    buffer_size=None, fields=None, **options):
        """Creates a relational database data target stream.

        :Attributes:
            * `url`: SQLAlchemy URL - either this or connection should be specified
            * `connection`: SQLAlchemy database connection or engine - either
              this or url should be specified
            * `table`: table name
            * `truncate`: whether truncate table or not
            * `create`: whether create table on initialize() or not
            * `replace`: Set to True if creation should replace existing table
              or not, otherwise initialization will fail on attempt to create
              a table which already exists.
            * `options`: other SQLAlchemy connect() options
            * `id_key_name`: name of the auto-increment key. If specified,
              then key column is created.
            * `buffer_size`: size of INSERT buffer - how many records are
              collected before they are inserted using multi-insert statement.
              Default is 1000
            * `fields`: fieldlist for a new table

        Note: avoid auto-detection when you are reading from remote URL stream.

        """
        if not options:
            options = {}

        self._buffer = []

        if store:
            self.store = store
        else:
            self.store = default_store(url=url, connectable=connectable,
                                        schema=schema)
        self.schema = schema
        self.options = options
        self.replace = replace
        self.create = create
        self.truncate = truncate
        self.fields = fields

        self.id_key_name = id_key_name

        if buffer_size:
            self.buffer_size = buffer_size
        else:
            self.buffer_size = 1000

        # Initialize
        if self.create:
            self.table = self.store.create_table(table, self.fields,
                                                 replace=self.replace,
                                                 schema=self.schema,
                                                 id_column=self.id_key_name)
        else:
            self.table = self.store.table(table, schema=schema, autoload=True)

        self.table_name = table.name

        if self.truncate:
            self.table.delete().execute()

        if not self.fields:
            self.fields = fields_from_table(self.table)

        self.field_names = self.fields.names()

        self.insert_command = self.table.insert()

    def as_source(self):
        """Returns source representation of the target table"""
        return SQLDataSource(store=self.store,
                             schema=self.schema,
                             table=self.table,
                             fields=self.fields)

    def __del__(self):
        """Closes the stream, flushes buffered data"""
        # self.flush()

    def rows(self):
        raise IsNotSourceError("SQLDataTarget is not a source. "
                                "Use 'as_source()' to convert it.")
    def __iter__(self):
        return self.rows()

    def append(self, row):

        # FIXME: remove this once SQLAlchemy will allow list based multi-insert
        row = dict(zip(self.fields.names(), row))

        self._buffer.append(row)
        if len(self._buffer) >= self.buffer_size:
            self.flush()

    def append_from(self, obj):
        """Appends data from object `obj` which might be a `DataObject`
        instance or an iterable. If `obj` is a `DataObject`, then it should
        have one of following representations, listed in order of preferrence:

        * sql_table
        * sql_statement
        * rows

        If the `obj` is just an iterable, then it is treated as `rows`
        representation of data object.

        `flush()` is called:

        * before SQL insert from a table or a statement
        * after insert of all rows of `rows` representation
        """

        try:
            reprs = obj.representations()
        except AttributeError:
            reprs = ["rows"]

        if "sql_table" in reprs or "sql_statement" in reprs:
            # Flush all data that were added through append() to preserve
            # insertion order (just in case)
            self.flush()

            # Preare INSERT INTO ... SELECT ... statement
            source = selectable_from_object(obj, self.store)
            statement = InsertIntoAsSelect(self.table, source)

            self.store.connectable.execute(statement)

        elif "rows" in reprs:
            # Assumption: all data objects with "rows" representation
            # implement Python iteraotr protocol
            for row in obj:
                self.append(row)

            # Clean-up after bulk insertion
            self.flush()

        else:
            raise RepresentationError(
                            "Incopatible representations '%s'", (reprs, ) )

    def truncate(self):
        if not table:
            raise RepresentationError("Can not truncate: "
                                      "SQL object is a statement not a table")
        self.engine.execute(self.table.delete())

    def flush(self):
        if self._buffer:
            insert = self.table.insert()
            self.store.connectable.execute(insert, self._buffer)
            self._buffer = []
