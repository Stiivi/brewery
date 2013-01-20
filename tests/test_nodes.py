#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import brewery
import brewery.nodes
from brewery.objects import *
from brewery.stream import ExecutionContext

class NodesTestCase(unittest.TestCase):
    def setUp(self):
        self.fields = brewery.FieldList(["i", "q", "str", "custom"])
        self.data = []
        count = 20
        for i in range(0, count):
            self.data.append([i, float(i)/4, "item-%s" % i, "a string"])

        self.source = RowListDataObject(self.fields, self.data)
        self.sources = {0:self.source}
        self.context = ExecutionContext()

    @unittest.skip("not yet")
    def test_node_dictionary(self):
        d = brewery.nodes.node_dictionary()
        self.assertIn("aggregate", d)
        self.assertIn("csv_source", d)
        self.assertIn("csv_target", d)
        self.assertNotIn("source", d)
        self.assertNotIn("aggregate_node", d)

    def test_sample_node(self):
        node = brewery.nodes.SampleNode()
        node.size = 5
        result = node.evaluate(self.context, self.sources)
        result = list(result)
        self.assertEqual(5, len(result))
        items = [r[0] for r in result]
        self.assertListEqual([0,1,2,3,4], items)

        node.mode = "nth"
        result = node.evaluate(self.context, self.sources)
        result = list(result)
        self.assertEqual(4, len(result))
        items = [r[0] for r in result]
        self.assertListEqual([0,5,10,15], items)

    @unittest.skip("not yet")
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
        sources = {0:RowListDataObject(self.fields, self.data),
                        1:RowListDataObject(self.fields, self.data)}

        result = node.evaluate(self.context, sources)
        result = list(result)

        self.assertEqual(40, len(result))

    @unittest.skip("not yet")
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

    def create_distinct_sample(self):

        fields = brewery.FieldList(["id", "an_int", "a_float", "type", "class"])
        obj = RowListDataObject(fields)
        for i in range(1, 10):
            obj.append([i, i, float(i)/4, "a", "x"])
            obj.append([i, i*10, float(i)/4, "a", "y"])
            obj.append([i*10, i*100, float(i)/4, "b", "x"])
            obj.append([i*100, i*1000, float(i)/4, "c", "y"])
        return obj

    def test_distinct(self):
        node = brewery.nodes.DistinctNode()
        obj = self.create_distinct_sample()
        sources = {0:obj}
        # FIXME: check for field equality
        result = node.evaluate(self.context, sources)
        rows = list(result)

        self.assertEqual(36, len(rows))

        # Test one field distinct
        node.keys = ["type"]
        result = node.evaluate(self.context, sources)
        rows = list(result)
        self.assertEqual(3, len(rows))

        # Test two field distinct
        node.keys = ["type", "class"]
        result = node.evaluate(self.context, sources)
        rows = list(result)
        self.assertEqual(4, len(rows))

        # Test for duplicates by id
        node.keys = ["id"]
        node.discard = True
        result = node.evaluate(self.context, sources)
        rows = list(result)
        self.assertEqual(9, len(rows))

        # Test for duplicates by id2 (should be none)
        node.keys = ["an_int"]
        node.discard = True
        result = node.evaluate(self.context, sources)
        rows = list(result)

        values = [row[1] for row in rows]
        self.assertListEqual([], values)
        self.assertEqual(0, len(values))

    def record_results(self):
        return [r for r in self.output.records()]

    def initialize_node(self, node):
        node.initialize()
        for output in node.outputs:
            output.fields = node.output_fields

    def test_aggregate_node(self):
        node = brewery.nodes.AggregateNode()
        obj = self.create_distinct_sample()
        sources = {0:obj}
        # FIXME: check for field equality

        node.keys = ["type"]
        measures = [ ("an_int", ["sum", "min", "max"]) ]
        node.measures = measures

        fields = node.output_fields(sources[0])
        fields = [str(field) for field in fields]
        a = ['type', 'an_int_sum', "an_int_min", "an_int_max", 'record_count']

        self.assertListEqual(a, fields)

        result = node.evaluate(self.context, sources)
        rows = list(result)

        self.assertEqual(3, len(rows))

        counts = {}
        sums = {}
        for result in rows:
            self.context.info("RESULT: %s" % (result, ) )
            sums[result[0]] = result[1]
            counts[result[0]] = result[-1]

        self.assertEqual({"a": 495, "b": 4500, "c": 45000}, sums)
        self.assertEqual({"a": 18, "b": 9, "c": 9}, counts)

        # Test no keys - only counts
        node.keys = None
        measures = [ ("an_int", ["sum", "min", "max"]) ]
        node.measures = measures

        result = node.evaluate(self.context, sources)
        rows = list(result)

        self.assertEqual(1, len(rows))
        first = rows[0]

        self.assertEqual(49995, first[0])
        self.assertEqual(36, first[-1])

    def assertAllRows(self, output):
        for row in output.rows():
            if not (type(row) == list or type(row) == tuple):
                self.fail('output should contain only rows (lists/tuples), found: %s' % type(row))

    def test_select(self):
        def select(value):
            return value < 5
        def select_greater_than(value, threshold):
            return value > threshold

        node = brewery.nodes.SelectNode(function=select, fields=["i"])

        # FIXME: check input > output fields
        # Passed fields should be equal
        # ifields = self.input.fields
        # ofields = node.output_fields
        # self.assertEqual(ifields, ofields)

        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(5, len(rows))

        # Try lambda
        pred = lambda value: value < 10
        node.function = pred

        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(10, len(rows))

        # Test kwargs
        node.function = select_greater_than
        node.kwargs = {"threshold" : 7}
        node.discard = True

        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(8, len(rows))

    def test_record_select(self):
        def select_dict(**record):
            return record["i"] < 5
        def select_local(i, **args):
            return i < 5

        node = brewery.nodes.SelectRecordsNode(condition=select_dict)
        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(5, len(rows))

        node.condition = select_local
        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(5, len(rows))

        node.condition = "i < 5"
        result = node.evaluate(self.context, self.sources)
        rows = list(result.rows())
        self.assertEqual(5, len(rows))

    @unittest.skip("not yet")
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

    @unittest.skip("not yet")
    def test_set_select(self):
        node = brewery.nodes.SetSelectNode(field = "type", value_set = ["a"])

        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual(18, len(self.output.buffer))

    @unittest.skip("not yet")
    def test_audit(self):
        node = brewery.nodes.AuditNode()
        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        self.assertEqual(6, len(node.output_fields))

        node.run()
        node.finalize()

        self.assertEqual(5, len(self.output.buffer))

    @unittest.skip("not yet")
    def test_strip(self):
        node = brewery.nodes.StringStripNode(fields = ["custom"])

        self.setup_node(node)
        self.create_sample(custom = "  foo  ")

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual("foo", self.output.buffer[0][3])

    @unittest.skip("not yet")
    def test_strip_auto(self):
        fields = brewery.FieldList([("str1", "string"),
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

    @unittest.skip("not yet")
    def test_consolidate_type(self):
        fields = brewery.FieldList([("s", "string"),
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

    @unittest.skip("not yet")
    def test_merge(self):
        node = brewery.nodes.MergeNode()
        self.create_distinct_sample()

        input2 = brewery.streams.SimpleDataPipe()
        input2.fields = brewery.FieldList(["type2", "name"])
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

    @unittest.skip("not yet")
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


