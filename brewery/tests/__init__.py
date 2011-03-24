import unittest
import os
import json
import re
from common import TESTS_PATH

from test_data_source import *
from test_pipes import *
from test_nodes import *
from test_field_list import *
from test_node_stream import *

test_cases = [FieldListCase,
              DataSourceUtilsTestCase,
              DataSourceTestCase,
              PipeTestCase,
              Pipe2TestCase,
              NodesTestCase,
              StreamBuildingTestCase,
              StreamInitializationTestCase
                ]

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

