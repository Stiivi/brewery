#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import os
import brewery.ds
import brewery

TESTS_PATH = os.path.dirname(os.path.abspath(__file__))

class DataSourceUtilsTestCase(unittest.TestCase):
    def test_expand_collapse(self):
        record = { "name": "foo", 
                    "entity.name": "bar", 
                    "entity.number" : 10, 
                    "entity.address.country": "Uganda" }
        ex_record = { "name": "foo", 
                     "entity": { "name": "bar", 
                                 "number" : 10, 
                                 "address": {"country": "Uganda" }
                                }
                    }
                
        self.assertEqual(ex_record, brewery.expand_record(record))
        self.assertEqual(record, brewery.collapse_record(ex_record))

# class DataStoreTestCase(unittest.TestCase):
#     def setUp(self):
#         pass
# 
#     def test_stores(self):
#         
#       self.assertRaisesRegexp(Exception, "datastore with name", brewery.ds.datastore, "foo")
#         desc = {"url":":memory:"}
#       self.assertRaisesRegexp(ValueError, "No adapter provided", brewery.ds.datastore, desc)
# 
#         desc = {"adapter":"foo", "path":":memory:"}
#       self.assertRaisesRegexp(KeyError, "Adapter.*foo.*not found", brewery.ds.datastore, desc)
# 
#         desc = {"adapter":"sqlalchemy", "url":"sqlite:///:memory:"}
#         ds = brewery.ds.datastore(desc)
#       self.assertEqual("sqlalchemy", ds.adapter_name)
 		
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
        self.data_dir = os.path.join(TESTS_PATH, 'data')
        self.output_dir = DataSourceTestCase.output_dir
    
    def data_file(self, file):
        return os.path.join(self.data_dir, file)
    def output_file(self, file):
        return os.path.join(self.output_dir, file)

    def read_source(self, source):
        count = 0
        max_fields = 0
        min_fields = 0
        self.rows = []
        for row in source.rows():
            count += 1
            max_fields = max(len(row), max_fields)
            min_fields = max(len(row), min_fields)
            self.rows.append(row)
            
        return { "count" : count, "max_fields": max_fields, "min_fields": min_fields }

    def test_file_source(self):
        # File
        src = brewery.ds.CSVDataSource(self.data_file('test.csv'))
        src.read_header = False
        # test = lambda: src.get_fields()
        self.assertRaises(RuntimeError, src.rows)
        
        src.read_header = True
        src.initialize()
        names = [field.name for field in src.fields]
        self.assertEqual(['id', 'name', 'type', 'location.name', 'location.code', 'amount'], names)

        result = self.read_source(src)
            
        self.assertEqual(6, result["max_fields"])
        self.assertEqual(6, result["min_fields"])
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


    def test_csv_field_type(self):
        src = brewery.ds.CSVDataSource(self.data_file('test.csv'), skip_rows=1,read_header=False)
        fields = ['id', 'name', 'type', 'location.name', 'location.code', ['amount', 'integer']]
        src.fields = brewery.FieldList(fields)
        src.initialize()
        self.assertEqual("integer", src.fields[5].storage_type)

        result = self.read_source(src)
        self.assertEqual(True, isinstance(self.rows[0][1], basestring))
        self.assertEqual(True, isinstance(self.rows[0][5], int))
    
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

    def test_row_record(self):
        pass
        # * Test whether all streams support correctly reading/writing both records and rows
        # * Streams should raise an exception when writing a row into a stream without initalized
        #   fields
        # * If fields are set to source stream, it should not return other fields as specified
    
    def test_auditor(self):
        src = brewery.ds.CSVDataSource(self.data_file('test.csv'))
        src.initialize()

        auditor = brewery.ds.StreamAuditor()
        auditor.fields = src.fields
        auditor.initialize()
        
        # Perform audit for each row from source:
        for row in src.rows():
            auditor.append(row)

        # Finalize results, close files, etc.
        auditor.finalize()

        # Get the field statistics
        stats = auditor.field_statistics
        
        self.assertEqual(len(stats), 6)
        stat = stats["type"].dict()
        self.assertTrue("record_count" in stat)
        self.assertTrue("unique_storage_type" in stat)
        utype = stat["unique_storage_type"]
        ftype = stat["storage_types"][0]
        self.assertEqual(utype, ftype)
        
        src.finalize()

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

