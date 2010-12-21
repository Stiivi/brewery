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
        self.assertEqual(dim.hierarchies["default"], dim.default_hierarchy, "Default hierarchy does not match")

        hlevels = dim.default_hierarchy.levels
        self.assertEqual(len(hlevels), 2, "Default hierarchy level count is not 2 (%s)" % hlevels)
        

        dlevels = dim.levels
        hlevels = dim.hierarchies["default"].levels
        self.assertTrue(issubclass(dlevels["year"].__class__, brewery.cubes.Level), "Level should be subclass of Level")
        self.assertTrue(issubclass(hlevels[0].__class__, brewery.cubes.Level), "Level should be subclass of Level")
        
        self.assertEqual(dlevels["year"], hlevels[0], "Level should be equal")

    def test_cube_from_file(self):
        info = self._model_file_dict("cube_contracts.json")
        self.skipTest("Cubes are not yet implemented")

    def test_model_from_path(self):
        # model = brewery.cubes.model_from_path(self.model_path)
        file_path = os.path.join(self.model_path, "cube_contracts.json")
        self.assertRaises(RuntimeError, brewery.cubes.model_from_path, file_path)
        
        model = brewery.cubes.model_from_path(self.model_path)
        self.assertEqual(model.name, "public_procurements", "Model was not properely loaded")
        self.assertEqual(len(model.dimensions), 6, "Model dimensions were not properely loaded")
        self.assertEqual(len(model.cubes), 1, "Model cubes were not loaded")
        cube = model.cubes.get("contracts")
        self.assertNotEqual(None, cube, 'No expected "contracts" cube found')
        self.assertEqual(cube.name, "contracts", "Model cube was not properely loaded")

        result = model.validate()
        self.assertEqual(0, len(result), 'Model validation failed')
        
    def model_validation(self):
        self.skipTest("Model validation is not yet implemented")
		
if __name__ == '__main__':
    unittest.main()

