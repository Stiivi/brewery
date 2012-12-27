import unittest
import os
import brewery

@unittest.skip("demonstrating skipping")
class ForksTestCase(unittest.TestCase):
    def test_basic(self):
        main = brewery.create_builder()
        main.csv_source("foo")

        self.assertEqual(1, len(main.stream.nodes))
        self.assertEqual("csv_source", main.node.identifier())
