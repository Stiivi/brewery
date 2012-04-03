#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import brewery
import brewery.ds as ds
import brewery.nodes

class NodesTestCase(unittest.TestCase):
    def setUp(self):
        self.input = brewery.streams.SimpleDataPipe()
        self.output = brewery.streams.SimpleDataPipe()
            
    def setup_node(self, node):
        node.inputs = [self.input]
        node.outputs = [self.output]

    def create_sample(self, count = 100, custom = None, pipe = None):
        if not pipe:
            pipe = self.input
        pipe.empty()
        pipe.fields = brewery.fieldlist(["i", "q", "str", "custom"])
        for i in range(0, count):
            pipe.put([i, float(i)/4, "item-%s" % i, custom])

    def test_node_subclasses(self):
        nodes = brewery.nodes.node_dictionary().values()
        self.assertIn(brewery.nodes.CSVSourceNode, nodes)
        self.assertIn(brewery.nodes.AggregateNode, nodes)
        self.assertIn(brewery.nodes.ValueThresholdNode, nodes)
        self.assertNotIn(brewery.streams.Stream, nodes)

    def test_node_dictionary(self):
        d = brewery.nodes.node_dictionary()
        self.assertIn("aggregate", d)
        self.assertIn("csv_source", d)
        self.assertIn("csv_target", d)
        self.assertNotIn("source", d)
        self.assertNotIn("aggregate_node", d)

    def test_sample_node(self):
        node = brewery.nodes.SampleNode()
        self.setup_node(node)
        self.create_sample()
        node.sample_size = 5
        self.initialize_node(node)
        node.run()
        node.finalize()
        
        self.assertEqual(len(self.output.buffer), 5)
        self.assertAllRows()

    def test_replace_node(self):
        node = brewery.nodes.TextSubstituteNode("str")
        self.setup_node(node)
        self.create_sample(10)
        node.add_substitution("[1-5]", "X")
        node.add_substitution("-", " ")
        self.initialize_node(node)
        node.run()
        node.finalize()
    
        for result in self.output.records():
            value = result["str"]
            self.assertRegexpMatches(value, "^item [X6-90]*$")
        self.assertAllRows()

    def test_append_node(self):
        node = brewery.nodes.AppendNode()
        self.setup_node(node)

        pipe1 = brewery.streams.SimpleDataPipe()
        self.create_sample(4, custom = "a", pipe = pipe1)

        pipe2 = brewery.streams.SimpleDataPipe()
        self.create_sample(4, custom = "b", pipe = pipe2)
        
        node.inputs = [pipe1, pipe2]
        
        self.initialize_node(node)
        ifields = pipe1.fields
        ofields = node.output_fields
        self.assertEqual(ifields, ofields)

        node.run()
        node.finalize()

        results = self.output.buffer

        self.assertEqual(len(results), 8)
        
        actual = [r[3] for r in results]
        expected = ['a'] * 4 + ['b'] * 4
        self.assertEqual(expected, actual)
        self.assertAllRows()
        
    def test_field_map(self):
        node = brewery.nodes.FieldMapNode()
        
        self.setup_node(node)
        self.create_sample(custom = "foo")
        
        node.rename_field("i", "index")
        node.drop_field("q")
        self.initialize_node(node)

        self.assertEqual(['index', 'str', 'custom'], node.output_fields.names())
        
        node.run()
        
        keys = set([])

        for result in self.output.records():
            for key in result.keys():
                keys.add(key)

        keys = list(keys)
        keys.sort()

        self.assertEqual(["custom", "index", "str"], keys)
        self.assertAllRows()

    def create_distinct_sample(self, pipe = None):
        if not pipe:
            pipe = self.input
        pipe.empty()
        pipe.fields = brewery.fieldlist(["id", "id2", "q", "type", "class"])
        for i in range(1, 10):
            pipe.put([i, i, float(i)/4, "a", "x"])
            pipe.put([i, i*10, float(i)/4, "a", "y"])
            pipe.put([i*10, i*100, float(i)/4, "b", "x"])
            pipe.put([i*100, i*1000, float(i)/4, "c", "y"])
        
    def test_distinct(self):
        node = brewery.nodes.DistinctNode()
        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)
        ifields = self.input.fields
        ofields = node.output_fields
        self.assertEqual(ifields, ofields)

        node.run()
        node.finalize()
        
        self.assertEqual(36, len(self.output.buffer)) 

        # Test one field distinct
        self.output.empty()
        self.create_distinct_sample()

        node.distinct_fields = ["type"]
        node.initialize()
        node.run()
        node.finalize()

        self.assertEqual(3, len(self.output.buffer)) 

        # Test two field distinct
        self.output.empty()
        self.create_distinct_sample()

        node.distinct_fields = ["type", "class"]
        node.initialize()
        node.run()
        node.finalize()

        self.assertEqual(4, len(self.output.buffer)) 
        
        # Test for duplicates by id
        self.output.empty()
        self.create_distinct_sample()

        node.distinct_fields = ["id"]
        node.discard = True
        node.initialize()
        node.run()
        node.finalize()
        
        values = []
        for row in self.output.buffer:
            values.append(row[0])

        self.assertEqual(9, len(self.output.buffer)) 

        # Test for duplicates by id2 (should be none)
        self.output.empty()
        self.create_distinct_sample()

        node.distinct_fields = ["id2"]
        node.discard = True
        node.initialize()
        node.run()
        node.finalize()
        
        values = []
        for row in self.output.buffer:
            values.append( row[1])

        self.assertEqual(0, len(self.output.buffer)) 
        self.assertAllRows()

    def record_results(self):
        return [r for r in self.output.records()]

    def initialize_node(self, node):
        node.initialize()
        for output in node.outputs:
            output.fields = node.output_fields

    def test_aggregate_node(self):
        node = brewery.nodes.AggregateNode()
        self.setup_node(node)
        self.create_distinct_sample()

        node.key_fields = ["type"]
        node.add_measure("id", ["sum"])
        self.initialize_node(node)
        
        fields = node.output_fields.names()
        a = ['type', 'id_sum', 'id_min', 'id_max', 'id_average', 'record_count']
        
        self.assertEqual(a, fields)
        
        node.run()
        node.finalize()

        results = self.record_results()
        self.assertEqual(3, len(results)) 

        counts = []
        sums = []
        for result in results:
            sums.append(result["id_sum"])
            counts.append(result["record_count"])

        self.assertEqual([90, 450, 4500], sums)
        self.assertEqual([18,9,9], counts)
        
        # Test no keys - only counts
        node = brewery.nodes.AggregateNode()
        self.setup_node(node)
        self.output.empty()
        self.create_distinct_sample()

        # Setup node
        node.add_measure("id", ["sum"])
        self.initialize_node(node)

        fields = node.output_fields.names()
        a = ['id_sum', 'id_min', 'id_max', 'id_average', 'record_count']
        self.assertEqual(a, fields)

        node.run()
        node.finalize()

        # Collect results
        results = self.record_results()
        self.assertEqual(1, len(results)) 
        counts = []
        sums = []
        for result in results:
            sums.append(result["id_sum"])
            counts.append(result["record_count"])

        self.assertEqual([36], counts)
        self.assertEqual([5040], sums)
        self.assertAllRows()

    def assertAllRows(self, pipe = None):
        if not pipe:
            pipe = self.output
            
        for row in pipe.rows():
            if not (type(row) == list or type(row) == tuple):
                self.fail('pipe should contain only rows (lists/tuples), found: %s' % type(row))

    def test_function_select(self):
        def select(value):
            return value < 5
        def select_greater_than(value, threshold):
            return value > threshold
            
        node = brewery.nodes.FunctionSelectNode(function = select, fields = ["i"])

        self.setup_node(node)
        self.create_sample()

        self.initialize_node(node)

        # Passed fields should be equal
        ifields = self.input.fields
        ofields = node.output_fields
        self.assertEqual(ifields, ofields)

        node.run()
        node.finalize()

        self.assertEqual(5, len(self.output.buffer)) 

        self.output.empty()
        x = lambda value: value < 10
        node.function = x
        self.setup_node(node)
        self.create_sample()

        self.initialize_node(node)
        node.run()
        node.finalize()

        self.assertEqual(10, len(self.output.buffer)) 
        
        # Test kwargs
        self.output.empty()
        self.setup_node(node)
        self.create_sample()
        node.function = select_greater_than
        node.kwargs = {"threshold" : 7}
        node.discard = True
        
        self.initialize_node(node)
        node.run()
        node.finalize()

        self.assertEqual(8, len(self.output.buffer)) 

    def test_select(self):
        def select_dict(**record):
            return record["i"] < 5
        def select_local(i, **args):
            return i < 5

        node = brewery.nodes.SelectNode(condition = select_dict)

        self.setup_node(node)
        self.create_sample()
        self.initialize_node(node)
        node.run()
        node.finalize()
        self.assertEqual(5, len(self.output.buffer)) 

        self.output.empty()
        self.setup_node(node)
        self.create_sample()
        node.condition = select_local
        self.initialize_node(node)
        node.run()
        node.finalize()
        self.assertEqual(5, len(self.output.buffer)) 

        self.output.empty()
        self.setup_node(node)
        self.create_sample()
        node.condition = "i < 5"
        self.initialize_node(node)
        node.run()
        node.finalize()
        self.assertEqual(5, len(self.output.buffer)) 

    def test_derive(self):
        def derive_dict(**record):
            return record["i"] * 10
        def derive_local(i, **args):
            return i * 10

        node = brewery.nodes.DeriveNode(formula = derive_dict)

        self.setup_node(node)
        self.create_sample()
        self.initialize_node(node)
        node.run()
        node.finalize()

        val = sum([row[4] for row in self.output.buffer])
        self.assertEqual(49500, val)

        self.output.empty()
        self.setup_node(node)
        self.create_sample()
        node.formula = derive_local
        self.initialize_node(node)
        node.run()
        node.finalize()
        val = sum([row[4] for row in self.output.buffer])
        self.assertEqual(49500, val)

        self.output.empty()
        self.setup_node(node)
        self.create_sample()
        node.formula = "i * 10"
        self.initialize_node(node)
        node.run()
        node.finalize()
        val = sum([row[4] for row in self.output.buffer])
        self.assertEqual(49500, val)

    def test_set_select(self):
        node = brewery.nodes.SetSelectNode(field = "type", value_set = ["a"])

        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual(18, len(self.output.buffer)) 

    def test_audit(self):
        node = brewery.nodes.AuditNode()
        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        self.assertEqual(6, len(node.output_fields)) 

        node.run()
        node.finalize()

        self.assertEqual(5, len(self.output.buffer)) 
        
    def test_strip(self):
        node = brewery.nodes.StringStripNode(fields = ["custom"])

        self.setup_node(node)
        self.create_sample(custom = "  foo  ")

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual("foo", self.output.buffer[0][3]) 

    def test_strip_auto(self):
        fields = brewery.fieldlist([("str1", "string"), 
                                       ("x","unknown"), 
                                       ("str2","string"), 
                                       ("f", "unknown")])
        self.input.fields = fields
        for i in range(0, 5):
            self.input.put([" foo ", " bar ", " baz ", " moo "])

        node = brewery.nodes.StringStripNode()

        self.setup_node(node)

        self.initialize_node(node)

        node.run()
        node.finalize()

        row = self.output.buffer[0]
        self.assertEqual(["foo", " bar ", "baz", " moo "], row) 

    def test_consolidate_type(self):
        fields = brewery.fieldlist([("s", "string"), 
                                       ("i","integer"), 
                                       ("f","float"), 
                                       ("u", "unknown")])
        self.input.fields = fields
        sample = [
                    ["  foo  ", 123, 123, None],
                    [123, "123", "123", None],
                    [123.0, " 123  ", "  123  ", None],
                    ["  foo  ", "1 2 3", "1 2 3  . 0", None],
                    ["  foo  ", "fail", "fail", None],
                    [None, None, None, None]
                ]

        for row in sample:
            self.input.put(row)


        node = brewery.nodes.CoalesceValueToTypeNode()

        self.setup_node(node)

        self.initialize_node(node)

        node.run()
        node.finalize()

        strings = []
        integers = []
        floats = []

        for row in self.output.buffer:
            strings.append(row[0])
            integers.append(row[1])
            floats.append(row[2])

        self.assertEqual(["foo", "123", "123.0", "foo", "foo", None], strings) 
        self.assertEqual([123, 123, 123, 123, None, None], integers) 
        self.assertEqual([123, 123, 123, 123, None, None], floats) 

    def test_merge(self):
        node = brewery.nodes.MergeNode()
        self.create_distinct_sample()

        input2 = brewery.streams.SimpleDataPipe()
        input2.fields = brewery.fieldlist(["type2", "name"])
        input2.put(["a", "apple"])
        input2.put(["b", "bananna"])
        input2.put(["c", "curry"])
        input2.put(["d", "dynamite"])

        input_len = len(self.input.buffer)

        node.inputs = [self.input, input2]
        node.outputs = [self.output]

        node.joins = [
                    (1, "type", "type2")
                ]

        node.maps = {
                        0: brewery.FieldMap(drop = ["id2"]),
                        1: brewery.FieldMap(drop = ["type2"])
                    }
        self.initialize_node(node)

        self.assertEqual(5, len(node.output_fields)) 

        node.run()
        node.finalize()

        self.assertEqual(5, len(self.output.buffer[0]))
        self.assertEqual(input_len, len(self.output.buffer)) 
        
    def test_generator_function(self):
        node = brewery.nodes.GeneratorFunctionSourceNode()
        def generator(start=0, end=10):
            for i in range(start,end):
                yield [i]
                
        node.function = generator
        node.fields = brewery.metadata.FieldList(["i"])
        node.outputs = [self.output]

        self.initialize_node(node)
        node.run()
        node.finalize()
        self.assertEqual(10, len(self.output.buffer))

        self.output.buffer = []
        node.args = [0,5]
        self.initialize_node(node)
        node.run()
        node.finalize()
        self.assertEqual(5, len(self.output.buffer))
        a = [row[0] for row in self.output.buffer]
        self.assertEqual([0,1,2,3,4], a)

