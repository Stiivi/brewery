"""SQLAlchemy Datastore Adapter
"""
from __future__ import absolute_import
import sqlalchemy
import brewery

def datastore(description):
    if "url" not in description:
        raise KeyError("connection url expected in sqlalchemy adapter")
    url = description["url"]
    options = description.get("options")

    ds = SQLAlchemyDatastore(url, options)
    return ds

class SQLAlchemyDatastore(brewery.ds.Datastore):
    def __init__(self, url, options = None):
        
        if not options:
            options = {}
        
        self.engine = sqlalchemy.create_engine(url, **options)
        self.connection = self.engine.connect()
        self.metadata = sqlalchemy.MetaData()
        self.metadata.bind = self.engine
        self.metadata.reflect()

    @property
    def adapter_name(self):
        return "sqlalchemy"
        
    def dataset(self, name):
        split = brewery.ds.split_table_schema(name)
        schema = split[0]
        table_name = split[1]
        table = sqlalchemy.Table(table_name, self.metadata, autoload = True, schema = schema)
        dataset = SQLAlchemyDataset(table)
        return dataset
        
    def has_dataset(self, name):
        return name in self.dataset_names

    @property
    def dataset_names(self):
        names = [table.name for table in self.metadata.sorted_tables]
        return names

class SQLAlchemyDataset(object):
    """docstring for SQLAlchemyDataset"""
    def __init__(self, table):
        super(SQLAlchemyDataset, self).__init__()
        self.table = table
        
    @property
    def field_names(self):
        names = [column.name for column in self.table.columns]
        return names
        
    def rows(self):
        return self.table.select().execute()

