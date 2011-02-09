import unittest
import brewery
import os
import brewery.tests
import json
import re

from test_data_source import *
from test_pipes import *
from test_nodes import *
from test_field_list import *
# from test_streams import *

test_cases = [FieldListCase,
              DataSourceUtilsTestCase,
              DataSourceTestCase,
              PipeTestCase,
              NodesTestCase
#              CSVDataStreamsTestCase
                ]

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

