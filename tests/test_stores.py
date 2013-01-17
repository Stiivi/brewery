import unittest
import brewery
from brewery.objects import *
from brewery.errors import *
import os.path


class DataStoreTestCase(unittest.TestCase):
    def setUp(self):
        self.store = self.store_description

class CSVDataStoreTestCase(DataStoreTestCase):
    store_description = {
                "path":os.path.join(os.path.realpath(__file__), 'data', 'csv')
            }

class BasicDataStoreTestCase(unittest.TestCase):
    def test_iterable_source(self):
        data = [[i,1,2,3] for i in range(0, 10)]

        obj = IterableDataSource(data, None)
        self.assertListEqual(["rows", "records"], obj.representations())

        out = list(obj.rows())
        self.assertEqual(len(data), len(out))



def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(BasicDataStoreTestCase))

   return suite


if __name__ == '__main__':
    unittest.main()
