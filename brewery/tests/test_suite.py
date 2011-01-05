import unittest
import brewery
import os
import brewery.cubes as cubes
import brewery.tests
import json
import re

from test_model import *
from test_query_generator import *
from test_data_source import *

test_cases = [DataStoreTestCase,
              ModelValidatorTestCase,
              ModelFromDictionaryTestCase, 
              ModelTestCase,
              QueryGeneratorTestCase,
              DataSourceTestCase
                ]

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

# 
# 
# if __name__ == '__main__':
#     unittest.main()
