import unittest
import brewery
import os
import brewery.cubes
import brewery.tests
import json

class ModelTestCase(unittest.TestCase):
	
    def setUp(self):
        self.model_path = os.path.join(brewery.tests.tests_path, 'model')

    def _model_file_dict(self, file_name):
        path = os.path.join(self.model_path, file_name)
        file = open(path)
        return json.load(file)

    def test_dimension_from_file(self):
        info = self._model_file_dict("dim_date.json")
        dim = brewery.cubes.Dimension("date", info)
        self.assertEqual(len(dim.levels), 3, "invalid number of levels for date dimension")
        self.assertEqual(len(dim.hierarchies), 2, "invalid number of hierarchies for date dimension")
        self.assertItemsEqual(dim.levels.keys(), ["year", "day", "month"],
                                        "invalid levels %s" % dim.levels.keys())
        self.assertItemsEqual(dim.hierarchies.keys(), ["default", "ymd"],
                                        "invalid hierarchies %s" % dim.hierarchies.keys())

    def test_cube_from_file(self):
        info = self._model_file_dict("cube_contracts.json")
        self.skipTest("Cubes are not yet implemented")

    def test_model_from_path(self):
        self.skipTest("Model from path is not yet implemented")
        # model = brewery.cubes.model_from_path(model_path)

    def model_validation(self):
        self.skipTest("Model validation is not yet implemented")
		
if __name__ == '__main__':
    unittest.main()

