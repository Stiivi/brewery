# -*- coding: utf-8 -*-

import brewery.pipes as pipes
import brewery.ds as ds
import unittest
import logging
import threading
import time
import os
import StringIO

logging.basicConfig(level=logging.WARN)

class StreamBuildingTestCase(unittest.TestCase):
    def setUp(self):
        # Steram we have here:
        #
        #  source ---+---> csv_target
        #            |
        #            +---> sample ----> html_target
        
        
        self.stream = pipes.Stream()
        self.node1 = pipes.Node()
        self.node1.description = "source"
        self.stream.add(self.node1, "source")

        self.node2 = pipes.Node()
        self.node2.description = "csv_target"
        self.stream.add(self.node2, "csv_target")

        self.node4 = pipes.Node()
        self.node4.description = "html_target"
        self.stream.add(self.node4, "html_target")

        self.node3 = pipes.Node()
        self.node3.description = "sample"
        self.stream.add(self.node3, "sample")

        self.stream.connect("source", "sample")
        self.stream.connect("source", "csv_target")
        self.stream.connect("sample", "html_target")
    
    def test_connections(self):
        self.assertEqual(4, len(self.stream.nodes))
        self.assertEqual(3, len(self.stream.connections))

        self.assertRaises(KeyError, self.stream.connect, "sample", "unknown")

        node = pipes.Node()
        self.assertRaises(KeyError, self.stream.add, node, "sample")
        
        self.stream.remove("sample")
        self.assertEqual(3, len(self.stream.nodes))
        self.assertEqual(1, len(self.stream.connections))

    def test_node_sort(self):
        sorted_nodes = self.stream.sorted_nodes()

        nodes = [self.node1, self.node3, self.node2, self.node4]

        self.assertEqual(self.node1, sorted_nodes[0])
        self.assertEqual(self.node4, sorted_nodes[-1])
        
        self.stream.connect("html_target", "source")
        self.assertRaises(Exception, self.stream.sorted_nodes)
        
    def test_update(self):
        stream_desc = {
            "nodes": {
                "source": {"type": "row_list_source"},
                "target": {"type": "record_list_target"},
                "aggtarget": {"type": "record_list_target"},
                "sample": {"type": "sample"},
                "map":  {"type": "field_map"},
                "aggregate": {"type": "aggregate", "keys": ["str"] }
            },
            "connections": {
                ("source", "sample"),
                ("sample", "map"),
                ("map", "target"),
                ("source", "aggregate"),
                ("aggregate", "aggtarget")
            }
        }
        
        stream = pipes.Stream()
        stream.update(stream_desc)
        self.assertTrue(isinstance(stream.node("source"), pipes.Node))
        self.assertTrue(isinstance(stream.node("aggregate"), pipes.AggregateNode))

        node = stream.node("aggregate")
        self.assertEqual(["str"], node.keys)

class FailNode(pipes.Node):
    __node_info__ = {
        "attributes": [ {"name":"message"} ]
    }
    
    def __init__(self):
        self.message = "This is fail node and it failed as expected"
    def run(self):
        logging.debug("intentionally failing a node")
        raise Exception(self.message)

class SlowSourceNode(pipes.Node):

    @property
    def output_fields(self):
        return ds.fieldlist(["i"])
        
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

        self.fields = ds.fieldlist(["a", "b", "c", "str"])
        self.src_list = [[1,2,3,"a"], [4,5,6,"b"], [7,8,9,"a"]]
        self.target_list = []
        self.aggtarget_list = []
        
        nodes = {
            "source": pipes.RowListSourceNode(self.src_list, self.fields),
            "target": pipes.RecordListTargetNode(self.target_list),
            "aggtarget": pipes.RecordListTargetNode(self.aggtarget_list),
            "sample": pipes.SampleNode("sample"),
            "map": pipes.FieldMapNode(drop_fields = ["c"]),
            "aggregate": pipes.AggregateNode(keys = ["str"])
        }
        
        connections = {
            ("source", "sample"),
            ("sample", "map"),
            ("map", "target"),
            ("source", "aggregate"),
            ("aggregate", "aggtarget")
        }

        self.stream = pipes.Stream(nodes, connections)

    def test_initialization(self):
        self.stream.initialize()

        target = self.stream.node("map")
        names = target.output_fields.names()
        self.assertEqual(['a', 'b', 'str'], names)

        agg = self.stream.node("aggregate")
        names = agg.output_fields.names()
        self.assertEqual(['str', 'record_count'], names)

    def test_run(self):
        self.stream.initialize()
        self.stream.run()
        self.stream.finalize()

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
        self.stream.initialize()
        self.stream.run()
        self.stream.finalize()
        
    def test_fail_run(self):
        nodes = {
            "source": pipes.RowListSourceNode(self.src_list, self.fields),
            "fail": FailNode(),
            "target": pipes.RecordListTargetNode(self.target_list)
        }
        connections = {
            ("source", "fail"),
            ("fail", "target")
        }
        stream = pipes.Stream(nodes, connections)

        stream.initialize()
        self.assertRaisesRegexp(pipes.StreamRuntimeError, "This is fail node", stream.run)
        stream.finalize()
        
        nodes["fail"].message = u"Unicode message: čučoriedka ľúbivo ťukala"

        stream.initialize()
        try:
            stream.run()
        except pipes.StreamRuntimeError, e:
            handle = StringIO.StringIO()
            # This should not raise an exception
            e.print_exception(handle)
            handle.close()
            
        stream.finalize()
        
    def test_fail_with_slow_source(self):
        nodes = {
            "source": SlowSourceNode(),
            "fail": FailNode(),
            "target": pipes.RecordListTargetNode(self.target_list)
        }
        connections = {
            ("source", "fail"),
            ("fail", "target")
        }
        
        stream = pipes.Stream(nodes, connections)

        stream.initialize()
        self.assertRaises(pipes.StreamRuntimeError, stream.run)
        stream.finalize()
    