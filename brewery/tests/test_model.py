import unittest
import brewery
import os
import brewery.cubes as cubes
import brewery.tests
import json
import re

class ModelTestCase(unittest.TestCase):
	
    def setUp(self):
        self.model_path = os.path.join(brewery.tests.tests_path, 'model')

    def _model_file_dict(self, file_name):
        path = os.path.join(self.model_path, file_name)
        file = open(path)
        return json.load(file)

    def test_dimension_from_file(self):
        info = self._model_file_dict("dim_date.json")
        dim = cubes.Dimension("date", info)
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
        self.assertTrue(issubclass(dlevels["year"].__class__, cubes.Level), "Level should be subclass of Level")
        self.assertTrue(issubclass(hlevels[0].__class__, cubes.Level), "Level should be subclass of Level")
        
        self.assertEqual(dlevels["year"], hlevels[0], "Level should be equal")

    def test_cube_from_file(self):
        info = self._model_file_dict("cube_contracts.json")
        self.skipTest("Cubes are not yet implemented")

    def test_model_from_path(self):
        # model = brewery.cubes.model_from_path(self.model_path)
        file_path = os.path.join(self.model_path, "cube_contracts.json")
        self.assertRaises(RuntimeError, cubes.model_from_path, file_path)
        
        model = cubes.model_from_path(self.model_path)
        self.assertEqual(model.name, "public_procurements", "Model was not properely loaded")
        self.assertEqual(len(model.dimensions), 6, "Model dimensions were not properely loaded")
        self.assertEqual(len(model.cubes), 1, "Model cubes were not loaded")
        cube = model.cubes.get("contracts")
        self.assertNotEqual(None, cube, 'No expected "contracts" cube found')
        self.assertEqual(cube.name, "contracts", "Model cube was not properely loaded")

        self.assertModelValid(model)
        
    def model_validation(self):
        self.skipTest("Model validation is not yet implemented")

    def assertModelValid(self, model):
        results = model.validate()
        print "model validation results:"
        for result in results:
            print "  %s: %s" % result

        error_count = 0
        for result in results:
            if result[0] == 'error':
                error_count += 1


        if error_count > 0:
            self.fail("Model validation failed")
        
class ModelFromDictionaryTestCase(unittest.TestCase):
    def setUp(self):
        self.model_path = os.path.join(brewery.tests.tests_path, 'model')
        self.model = cubes.model_from_path(self.model_path)

    def test_model_from_dictionary(self):
        model_dict = self.model.to_dict()
        new_model = cubes.model_from_dict(model_dict)
        new_model_dict = new_model.to_dict()
        
        # Break-down comparison to see where the difference is
        self.assertEqual(model_dict.keys(), new_model_dict.keys(), 'model dictionaries should have same keys')

        for key in model_dict.keys():
            old_value = model_dict[key]
            new_value = new_model_dict[key]

            self.assertEqual(type(old_value), type(new_value), "model part '%s' type should be the same" % key)
            if type(old_value) == dict:
                self.assertDictEqual(old_value, new_value, "model part '%s' should be the same" % key)
                pass
            else:
                self.assertEqual(old_value, new_value, "model part '%s' should be the same" % key)

        self.assertDictEqual(model_dict, new_model_dict, 'model dictionaries should be the same')
    
class ModelValidatorTestCase(unittest.TestCase):

    def setUp(self):
        self.model = cubes.Model('test')
        self.date_levels = { "year": { "key": "year" }, "month": { "key": "month" } }
        self.date_levels2 = { "year": { "key": "year" }, "month": { "key": "month" }, "day": {"key":"day"} }
        self.date_hiers = { "ym": { "levels": ["year", "month"] } }
        self.date_hiers2 = { "ym": { "levels": ["year", "month"] }, 
                             "ymd": { "levels": ["year", "month", "day"] } }
        self.date_desc = { "name": "date", "levels": self.date_levels , "hierarchies": self.date_hiers }

    def test_dimension_validation(self):
        date_desc = { "name": "date"}
        dim = cubes.Dimension('date', date_desc)
        results = dim.validate()
        self.assertValidationError(results, "No levels in dimension")

        date_desc = { "name": "date", "levels": self.date_levels}
        dim = cubes.Dimension('date', date_desc)
        results = dim.validate()

        self.assertValidationError(results, "No hierarchies in dimension")

        date_desc = { "name": "date", "levels": self.date_levels , "hierarchies": self.date_hiers }
        dim = cubes.Dimension('date', date_desc)
        results = dim.validate()

        self.assertValidation(results, "No levels in dimension", "Dimension is invalid without levels")
        self.assertValidation(results, "No hierarchies in dimension", "Dimension is invalid without hierarchies")
        self.assertValidationError(results, "No default hierarchy name")
        
        dim.default_hierarchy_name = 'foo'
        results = dim.validate()
        self.assertValidationError(results, "Default hierarchy .* does not")
        self.assertValidation(results, "No default hierarchy name")

        dim.default_hierarchy_name = 'ym'
        results = dim.validate()
        self.assertValidation(results, "Default hierarchy .* does not")

        date_desc = { "name": "date", "levels": self.date_levels , "hierarchies": self.date_hiers2 }
        self.assertRaisesRegexp(KeyError, 'No level day in dimension', cubes.Dimension, 'date', date_desc)

        date_desc = { "name": "date", "levels": self.date_levels2 , "hierarchies": self.date_hiers2 }
        dim = cubes.Dimension('date', date_desc)
        results = dim.validate()
        self.assertValidationError(results, "No defaut hierarchy .* more than one")

    def assertValidation(self, results, expected, message = None):
        if not message:
            message = "Validation pass expected (match: '%s')" % expected

        for result in results:
            if re.match(expected, result[1]):
                self.fail(message)

    def assertValidationError(self, results, expected, message = None):
        if not message:
            message = "Validation error expected (match: '%s')" % expected
            
        for result in results:
            if re.match(expected, result[1]):
                return
        self.fail(message)
        		
if __name__ == '__main__':
    unittest.main()

