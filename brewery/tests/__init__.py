# -*- coding: utf-8 -*-

import unittest
import os
import json
import re

from test_data_source import *
from test_pipes import *
from test_nodes import *
from test_field_list import *
from test_node_stream import *
from test_data_quality import *
from test_sql_streams import *
from test_forks import *

test_cases = [FieldListCase,
              DataSourceUtilsTestCase,
              DataSourceTestCase,
              PipeTestCase,
              Pipe2TestCase,
              NodesTestCase,
              StreamBuildingTestCase,
              StreamInitializationTestCase,
              DataQualityTestCase,
              StreamConfigurationTestCase,
              SQLStreamsTestCase,
              ForksTestCase
                ]

def load_tests(loader, tests, pattern):
    suite = unittest.TestSuite()
    for test_class in test_cases:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    return suite

