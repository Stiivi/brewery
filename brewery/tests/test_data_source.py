import unittest
import brewery
import os
import brewery.ds
import brewery.tests
import json
import csv
import shutil

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
    output_dir = None
    @classmethod
    def setUpClass(cls):
        DataSourceTestCase.output_dir = 'test_out'
        if not os.path.exists(DataSourceTestCase.output_dir):
            os.makedirs(DataSourceTestCase.output_dir)
    @classmethod
    def tearDownClass(cls):
        pass
        
    def setUp(self):
        self.data_dir = os.path.join(brewery.tests.tests_path, 'data')
        self.output_dir = DataSourceTestCase.output_dir
    
    def data_file(self, file):
        return os.path.join(self.data_dir, file)
    def output_file(self, file):
        return os.path.join(self.output_dir, file)

    def read_source(self, source):
        count = 0
        max_fields = 0
        min_fields = 0
        for row in source.rows():
            count += 1
            max_fields = max(len(row), max_fields)
            min_fields = max(len(row), min_fields)
        return { "count" : count, "max_fields": max_fields, "min_fields": min_fields }

    def test_file_source(self):
        # File
        src = brewery.ds.CSVDataSource(self.data_file('test.csv'))
        src.read_header = False
        test = lambda: src.fields()
        self.assertRaises(ValueError, test)
        
        src.read_header = True
        src.initialize()
        names = [field.name for field in src.fields]
        self.assertEqual(['name', 'type', 'amount'], names)

        result = self.read_source(src)
            
        self.assertEqual(3, result["max_fields"])
        self.assertEqual(3, result["min_fields"])
        self.assertEqual(8, result["count"])

    def test_csv_dialect(self):
        src = brewery.ds.CSVDataSource(self.data_file('test_tab.csv'), dialect = "foo")
        self.assertRaises(Exception, src.initialize)

        src = brewery.ds.CSVDataSource(self.data_file('test_tab.csv'), dialect = "excel-tab")
        src.initialize()
        result = self.read_source(src)
        self.assertEqual(3, result["max_fields"])
        self.assertEqual(3, result["min_fields"])
        self.assertEqual(8, result["count"])

    def test_xls_source(self):
        src = brewery.ds.XLSDataSource(self.data_file('test.xls'))
        src.initialize()
        result = self.read_source(src)
        self.assertEqual(3, result["max_fields"])
        self.assertEqual(3, result["min_fields"])
        self.assertEqual(8, result["count"])

    def test_copy(self):
        src = brewery.ds.CSVDataSource(self.data_file('test_tab.csv'), dialect = "excel-tab")
        src.initialize()

        fields = src.fields

        target = brewery.ds.CSVDataTarget(self.output_file('test_out.csv'))
        target.fields = fields
        target.initialize()

        for row in src.rows():
            target.append(row)
        target.finalize()
        
        src2 = brewery.ds.CSVDataSource(self.output_file('test_out.csv'))
        src2.initialize()
        result = self.read_source(src2)
            
        self.assertEqual(3, result["max_fields"])
        self.assertEqual(3, result["min_fields"])
        self.assertEqual(8, result["count"])

        
    # def test_sqlite_source(self):
    #     return
    #     src = brewery.ds.RelationalDataSource(self.connection, "test_amounts")
    #     names = [field.name for field in src.fields]
    #     self.assertEqual(["trans_date", "subject", "amount"], names, 'Read fields do not match')
    # 
    #     date_field = src.fields[0]
    #     subject_field = src.fields[1]
    #     amount_field = src.fields[2]
    #     
    #     self.assertEqual("trans_date", date_field.name)
    #     self.assertEqual("date", date_field.storage_type)
    # 
    #     self.assertEqual("subject", subject_field.name)
    #     self.assertEqual("string", subject_field.storage_type)
    #     self.assertEqual("unknown", subject_field.analytical_type)
    # 
    #     self.assertEqual("amount", amount_field.name)
    #     self.assertEqual("numeric", amount_field.storage_type)
    #     self.assertEqual("range", amount_field.analytical_type)
		
    # def test_mongo_source(self):
    #     connection_desc = { "adapter": "mongodb", "host":"localhost", "database":"wdmmg"}
    #     ds = brewery.ds.datastore(connection_desc)
    #     src = brewery.ds.data_source(datastore = ds, dataset = "classifier")
        
		
if __name__ == '__main__':
    unittest.main()

