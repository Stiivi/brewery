import unittest
import brewery
import os
import brewery.cubes as cubes
import brewery.tests
import json
import re

from test_model import *

test_cases = [ModelValidatorTestCase, ModelTestCase]

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
