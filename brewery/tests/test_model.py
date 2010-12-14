import unittest
import brewery
import os
import brewery.cubes
import brewery.tests
import json

class ModelTestCase(unittest.TestCase):
	
    def setUp(self):
        self.model_path = os.path.join(brewery.tests.tests_path, 'model')

    def test_dimension_from_file(self):
        dim_file = os.path.join(self.model_path, "dim_date.json")
        exit
        file = open(dim_file)
        info = json.load(file)
        dim = brewery.cubes.Dimension("date", info)
        self.assertEqual(len(dim.levels), 3, "invalid number of levels for date dimension")
		
if __name__ == '__main__':
    unittest.main()

