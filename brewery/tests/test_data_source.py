import unittest
import brewery
import os
import brewery.ds
import brewery.tests
import json

class DataStoreTestCase(unittest.TestCase):
    def setUp(self):
        pass

    def test_stores(self):
        
    	self.assertRaisesRegexp(Exception, "datastore with name", brewery.ds.datastore, "foo")
        desc = {"url":":memory:"}
    	self.assertRaisesRegexp(ValueError, "No adapter provided", brewery.ds.datastore, desc)

        desc = {"adapter":"foo", "path":":memory:"}
    	self.assertRaisesRegexp(KeyError, "Adapter.*foo.*not found", brewery.ds.datastore, desc)

        desc = {"adapter":"sqlalchemy", "url":"sqlite:///:memory:"}
        ds = brewery.ds.datastore(desc)
    	self.assertEqual("sqlalchemy", ds.adapter_name)
 		
class DataSourceTestCase(unittest.TestCase):
	
    def setUp(self):
        self.data_dir = os.path.join(brewery.tests.tests_path, 'data')
        sql_store_desc = {"adapter":"sqlite3", "path":":memory:"}
        mongo_store_desc = {"adapter":"mongo", "database":"wdmmg"}
        self.sql_store = brewery.ds.datastore(sql_store_desc)
        self.mongo_store = brewery.ds.datastore(mongo_store_desc)
        
    def data_file(self, file):
        return os.path.join(self.data_dir, file)
        
    def test_file_source(self):
        # File
        src = brewery.ds.CSVDataSource(self.data_file('test.csv'))
        src.read_field_names = False
        names = [field.name for field in src.fields]
        self.assertEqual(["field1", "field2", "field3"], names, 'Default fields do not match')

        src = brewery.ds.FileDataSource(self.data_file('test.csv'))
        src.read_field_names = True
        src.read_values()
        names = [field.name for field in src.fields]
        self.assertEqual(["date", "subject", "amount"], names, 'Read fields do not match')
        
    def test_sqlite_source(self):
        return
        src = brewery.ds.RelationalDataSource(self.connection, "test_amounts")
        names = [field.name for field in src.fields]
        self.assertEqual(["trans_date", "subject", "amount"], names, 'Read fields do not match')

        date_field = src.fields[0]
        subject_field = src.fields[1]
        amount_field = src.fields[2]
        
        self.assertEqual("trans_date", date_field.name)
        self.assertEqual("date", date_field.storage_type)

        self.assertEqual("subject", subject_field.name)
        self.assertEqual("string", subject_field.storage_type)
        self.assertEqual("unknown", subject_field.analytical_type)

        self.assertEqual("amount", amount_field.name)
        self.assertEqual("numeric", amount_field.storage_type)
        self.assertEqual("range", amount_field.analytical_type)
		
    # def test_mongo_source(self):
    #     connection_desc = { "adapter": "mongodb", "host":"localhost", "database":"wdmmg"}
    #     ds = brewery.ds.datastore(connection_desc)
    #     src = brewery.ds.data_source(datastore = ds, dataset = "classifier")
        
	    
	def test_csv_copy(self):
	    src = brewery.ds.CSVDataSource(self.data_file('test.csv'))
	    src.read_field_names = True
	    src.initialize()
	    
	    fields = src.fields
	    
	    self.datastore.delete_table("csv_copy", tolerant = true)
	    dbtarget = brewery.ds.RelationalDataTarget(self.datastore)
	    dbtarget.fields = fields
	    dbtarget.creates_table = True
	    dbtarget.initialize()
	    
	    ftarget = brewery.ds.CSVDataTarget('/tmp/__brewery_test.csv')
	    ftarget.fields = fields
	    ftarget.initialize()

	    for row in src.rows():
	        dbtarget.append(row)
	        ftarget.append(row)

	def test_some_to_some(self):
	    src = brewery.ds.DataSource("csv", self.data_file('test.csv'))
	    src.read_field_names = True
	    src.initialize()
	    
	    fields = src.fields
	    
	    self.datastore.delete_table("csv_copy", tolerant = true)
	    target = brewery.ds.RelationalDataTarget(self.datastore)
	    target.fields = fields
	    target.creates_table = True

	    target.initialize()
	    
	    for row in src.rows():
	        target.append(row)
		
if __name__ == '__main__':
    unittest.main()

