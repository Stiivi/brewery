#!/usr/bin/env python
# -*- coding: utf-8 -*-

import brewery
import brewery.ds as ds
import unittest
import logging
import time
import StringIO

from brewery.streams import *
from brewery.nodes import *
from brewery.common import *

logging.basicConfig(level=logging.WARN)

class StreamBuildingTestCase(unittest.TestCase):
    def setUp(self):
        # Stream we have here:
        #
        #  source ---+---> csv_target
        #            |
        #            +---> sample ----> html_target
        
        
        self.stream = Stream()
        self.node1 = Node()
        self.node1.description = "source"
        self.stream.add(self.node1, "source")

        self.node2 = Node()
        self.node2.description = "csv_target"
        self.stream.add(self.node2, "csv_target")

        self.node4 = Node()
        self.node4.description = "html_target"
        self.stream.add(self.node4, "html_target")

        self.node3 = Node()
        self.node3.description = "sample"
        self.stream.add(self.node3, "sample")

        self.stream.connect("source", "sample")
        self.stream.connect("source", "csv_target")
        self.stream.connect("sample", "html_target")
    
    def test_connections(self):
        self.assertEqual(4, len(self.stream.nodes))
        self.assertEqual(3, len(self.stream.connections))

        self.assertRaises(KeyError, self.stream.connect, "sample", "unknown")

        node = Node()
        self.assertRaises(KeyError, self.stream.add, node, "sample")
        
        self.stream.remove("sample")
        self.assertEqual(3, len(self.stream.nodes))
        self.assertEqual(1, len(self.stream.connections))

    def test_node_sort(self):
        # FIXME: This test is bugged
        sorted_nodes = self.stream.sorted_nodes()

        nodes = [self.node1, self.node3, self.node2, self.node4]

        self.assertEqual(self.node1, sorted_nodes[0])
        # self.assertEqual(self.node4, sorted_nodes[-1])
        
        self.stream.connect("html_target", "source")
        self.assertRaises(Exception, self.stream.sorted_nodes)
        
    def test_update(self):
        nodes = {
                "source": {"type": "row_list_source"},
                "target": {"type": "record_list_target"},
                "aggtarget": {"type": "record_list_target"},
                "sample": {"type": "sample"},
                "map":  {"type": "field_map"},
                "aggregate": {"type": "aggregate", "keys": ["str"] }
            }
        connections = [
                ("source", "sample"),
                ("sample", "map"),
                ("map", "target"),
                ("source", "aggregate"),
                ("aggregate", "aggtarget")
            ]
        
        stream = Stream()
        stream.update(nodes, connections)
        self.assertTrue(isinstance(stream.node("source"), Node))
        self.assertTrue(isinstance(stream.node("aggregate"), AggregateNode))

        node = stream.node("aggregate")
        self.assertEqual(["str"], node.keys)

class FailNode(Node):
    node_info = {
        "attributes": [ {"name":"message"} ]
    }
    
    def __init__(self):
        self.message = "This is fail node and it failed as expected"
    def run(self):
        logging.debug("intentionally failing a node")
        raise Exception(self.message)

class SlowSourceNode(Node):
    node_info = {}
    @property
    def output_fields(self):
        return brewery.fieldlist(["i"])
        
    def run(self):
        for cycle in range(0,10):
            for i in range(0, 1000):
                self.put([i])
            time.sleep(0.05)
        
class StreamInitializationTestCase(unittest.TestCase):
    def setUp(self):
        # Stream we have here:
        #
        #  source ---+---> aggregate ----> aggtarget
        #            |
        #            +---> sample ----> map ----> target

        self.fields = brewery.fieldlist(["a", "b", "c", "str"])
        self.src_list = [[1,2,3,"a"], [4,5,6,"b"], [7,8,9,"a"]]
        self.target_list = []
        self.aggtarget_list = []
        
        nodes = {
            "source": RowListSourceNode(self.src_list, self.fields),
            "target": RecordListTargetNode(self.target_list),
            "aggtarget": RecordListTargetNode(self.aggtarget_list),
            "sample": SampleNode("sample"),
            "map": FieldMapNode(drop_fields = ["c"]),
            "aggregate": AggregateNode(keys = ["str"])
        }
        
        connections = [
            ("source", "sample"),
            ("sample", "map"),
            ("map", "target"),
            ("source", "aggregate"),
            ("aggregate", "aggtarget")
        ]

        self.stream = Stream(nodes, connections)

    def test_initialization(self):
        self.stream._initialize()

        target = self.stream.node("map")
        names = target.output_fields.names()
        self.assertEqual(['a', 'b', 'str'], names)

        agg = self.stream.node("aggregate")
        names = agg.output_fields.names()
        self.assertEqual(['str', 'record_count'], names)

    def test_run(self):
        self.stream.run()

        target = self.stream.node("target")
        data = target.list
        expected = [{'a': 1, 'b': 2, 'str': 'a'}, 
                    {'a': 4, 'b': 5, 'str': 'b'}, 
                    {'a': 7, 'b': 8, 'str': 'a'}]
        self.assertEqual(expected, data)

        target = self.stream.node("aggtarget")
        data = target.list
        expected = [{'record_count': 2, 'str': 'a'}, {'record_count': 1, 'str': 'b'}]
        self.assertEqual(expected, data)
        
    def test_run_removed(self):
        self.stream.remove("aggregate")
        self.stream.remove("aggtarget")
        self.stream.run()
        
    def test_fail_run(self):
        nodes = {
            "source": RowListSourceNode(self.src_list, self.fields),
            "fail": FailNode(),
            "target": RecordListTargetNode(self.target_list)
        }
        connections = [
            ("source", "fail"),
            ("fail", "target")
        ]
        stream = Stream(nodes, connections)

        self.assertRaisesRegexp(StreamRuntimeError, "This is fail node", stream.run)
        
        nodes["fail"].message = u"Unicode message: čučoriedka ľúbivo ťukala"

        try:
            stream.run()
        except StreamRuntimeError, e:
            handle = StringIO.StringIO()
            # This should not raise an exception
            e.print_exception(handle)
            handle.close()
            
    def test_fail_with_slow_source(self):
        nodes = {
            "source": SlowSourceNode(),
            "fail": FailNode(),
            "target": RecordListTargetNode(self.target_list)
        }
        connections = [
            ("source", "fail"),
            ("fail", "target")
        ]
        
        stream = Stream(nodes, connections)

        self.assertRaises(StreamRuntimeError, stream.run)
    
class StreamConfigurationTestCase(unittest.TestCase):
    def test_create_node(self):
        self.assertEqual(RowListSourceNode, type(create_node("row_list_source")))
        self.assertEqual(AggregateNode, type(create_node("aggregate")))
        
    def test_configure(self):
        config = {
            "resource": "http://foo.com/bar.csv",
            "fields": ["field1", "field2", "field3"]
        }

        node = CSVSourceNode(self)
        node.configure(config)
        self.assertEqual(config["resource"], node.resource)
        self.assertEqual(config["fields"], node.fields)
        