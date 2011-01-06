"""SQLAlchemy Datastore Adapter
"""
from __future__ import absolute_import
import sqlalchemy
import brewery

storage_field_map = {
                "string": sqlalchemy.types.Unicode,
                "text": sqlalchemy.types.UnicodeText,
                "date": sqlalchemy.types.Date,
                "time": sqlalchemy.types.DateTime,
                "integer": sqlalchemy.types.Integer,
                "numeric": sqlalchemy.types.Numeric,
                "boolean": sqlalchemy.types.SmallInteger
            }

def datastore(description):
    if "url" not in description:
        raise KeyError("connection url expected in sqlalchemy adapter")
    url = description["url"]
    options = description.get("options")

    ds = SQLAlchemyDatastore(url, options)
    return ds

def split_table_schema(table_name):
    """Get schema and table name from table reference.

    Returns: Tuple in form (schema, table)
    """

    split = table_name.split('.')
    if len(split) > 1:
        return (split[0], split[1])
    else:
        return (None, split[0])

class SQLAlchemyDatastore(brewery.ds.Datastore):
    def __init__(self, url, options = None):
        
        if not options:
            options = {}
        
        self.engine = sqlalchemy.create_engine(url, **options)
        self.connection = self.engine.connect()
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.engine
        self.metadata.reflect()
        self.schema = None

    @property
    def adapter_name(self):
        return "sqlalchemy"
        
    def dataset(self, name):
        dataset = SQLAlchemyDataset(self, self._table(name))
        return dataset
        
    def has_dataset(self, name):
        table = self._table(name, autoload = False)
        return table.exists()

    def destroy_dataset(self, name, checkfirst = False):
        table = self._table(name, autoload = False)
        table.drop(checkfirst=checkfirst)

    def create_dataset(self, name, fields, replace = False):
        if replace and self.has_dataset(name):
            raise ValueError("Dataset '%s' already exists" % name)
        table = self._table(name, autoload = False)

        for field in fields:
            if not issubclass(type(field), brewery.Field):
                raise ValueError("field %s is not subclass of brewery.Field" % (field))
            field_type = storage_field_map[field.storage_type]
            col = sqlalchemy.schema.Column(field.name, field_type)
            table.append_column(col)

        table.create()

        dataset = SQLAlchemyDataset(self, table)
        return dataset

    def _table(self, name, autoload = True):
        split = split_table_schema(name)
        schema = split[0]
        table_name = split[1]

        if not schema:
            schema = self.schema

        table = sqlalchemy.Table(table_name, self.metadata, autoload = autoload, schema = schema)
        return table

    @property
    def dataset_names(self):
        names = [table.name for table in self.metadata.sorted_tables]
        return names

class SQLAlchemyDataset(object):
    """docstring for SQLAlchemyDataset"""
    def __init__(self, datastore, table):
        super(SQLAlchemyDataset, self).__init__()
        self.datastore = datastore
        self.table = table
        
    @property
    def field_names(self):
        names = [column.name for column in self.table.columns]
        return names
        
    def rows(self):
        return self.table.select().execute()

    def append(self, obj):
        self.table.insert(values = obj).execute()