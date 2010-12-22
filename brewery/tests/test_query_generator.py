import unittest
import brewery
import os
import brewery.cubes as cubes
import brewery.tests
import json

class QueryGeneratorTestCase(unittest.TestCase):
	
    def setUp(self):
        self.model = cubes.Model('test')
        
        date_desc = { "name": "date", 
                      "levels": { 
                                    "year": { "key": "year", "attributes": ["year"] }, 
                                    "month": { "key": "month", "attributes": ["month", "month_name"] }
                                } , 
                       "hierarchies": { 
                                "default": { 
                                    "levels": ["year", "month"]
                                } 
                        }
                    }
        date_dim = cubes.Dimension("date", date_desc)

        self.cube = self.model.create_cube("testcube")
        self.cube.add_dimension(date_dim)
        
        self.cube.measures = ["amount"]
        self.cube.mappings = {
                                "amount": "fact.amount",
                                "date.year": "dm_date.year",
                                "date.month": "dm_date.month",
                                "date.month_name": "dm_date.month_name",
                             }

    def test_star(self):
        print "\nVALIDATION: %s\n" % self.model.validate()
        self.assertEqual(True, self.model.is_valid(), 'Model is not valid (contains errors)')
        stmt = brewery.cubes.cube_select_statement(self.cube)
        
        print "SELECT: %s" % stmt

		
if __name__ == '__main__':
    unittest.main()

