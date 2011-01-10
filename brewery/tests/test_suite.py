import unittest
import brewery
import os
import brewery.tests
import json
import re

from test_data_source import *

test_cases = [DataStoreTestCase,
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
