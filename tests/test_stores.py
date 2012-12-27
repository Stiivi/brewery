import unittest
import brewery
from brewery.errors import *
import os.path

class DataStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.store = self.store_description

class CSVDataStoreTestCase(DataStoreTestCase):
    store_description = {
                "path":os.path.join(os.path.realpath(__file__), 'data', 'csv')
            }

def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(DataStoreTestCase))

   return suite

