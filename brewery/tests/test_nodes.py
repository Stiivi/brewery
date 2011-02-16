import unittest
import brewery.pipes as pipes
import brewery
import brewery.ds as ds

class NodesTestCase(unittest.TestCase):
    def setUp(self):
        self.input = pipes.SimpleDataPipe()
        self.output = pipes.SimpleDataPipe()
            
    def setup_node(self, node):
        node.inputs = [self.input]
        node.outputs = [self.output]

    def create_sample(self, count = 100, custom = None, pipe = None):
        if not pipe:
            pipe = self.input
        pipe.empty()
        pipe.fields = brewery.ds.fieldlist(["i", "q", "str", "custom"])
        for i in range(0, count):
            pipe.put([i, float(i)/4, "item-%s" % i, custom])

    def test_sample_node(self):
        node = pipes.SampleNode()
        self.setup_node(node)
        self.create_sample()
        node.sample_size = 5
        self.initialize_node(node)
        node.run()
        node.finalize()
        
        self.assertEqual(len(self.output.buffer), 5)
        self.assertAllRows()

    def test_replace_node(self):
        node = pipes.TextSubstituteNode("str")
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
        node = pipes.AppendNode()
        self.setup_node(node)

        pipe1 = pipes.SimpleDataPipe()
        self.create_sample(4, custom = "a", pipe = pipe1)

        pipe2 = pipes.SimpleDataPipe()
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
        node = pipes.FieldMapNode()
        
        self.setup_node(node)
        self.create_sample(custom = "foo")
        
        node.rename_field("i", "index")
        node.drop_field("q")
        self.initialize_node(node)

        self.assertEqual(['index', 'str', 'custom'], node.output_field_names)
        
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
        pipe.fields = brewery.ds.fieldlist(["id", "id2", "q", "type", "class"])
        for i in range(1, 10):
            pipe.put([i, i, float(i)/4, "a", "x"])
            pipe.put([i, i*10, float(i)/4, "a", "y"])
            pipe.put([i*10, i*100, float(i)/4, "b", "x"])
            pipe.put([i*100, i*1000, float(i)/4, "c", "y"])
        
    def test_distinct(self):
        node = pipes.DistinctNode()
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
        results = []
        for record in self.output.records():
            results.append(record)
        return results

    def initialize_node(self, node):
        node.initialize()
        for output in node.outputs:
            output.fields = node.output_fields

    def test_aggregate_node(self):
        node = pipes.AggregateNode()
        self.setup_node(node)
        self.create_distinct_sample()

        node.key_fields = ["type"]
        node.add_aggregation("id", ["sum"])
        self.initialize_node(node)
        
        fields = node.output_field_names
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
        node = pipes.AggregateNode()
        self.setup_node(node)
        self.output.empty()
        self.create_distinct_sample()

        # Setup node
        node.add_aggregation("id", ["sum"])
        self.initialize_node(node)

        fields = node.output_field_names
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

    def test_select(self):
        def select(value):
            return value < 5
        def select_greater_than(value, threshold):
            return value > threshold
            
        node = pipes.SelectNode(function = select, fields = ["i"])

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
    def test_set_select(self):
        node = pipes.SetSelectNode(field = "type", value_set = ["a"])

        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual(18, len(self.output.buffer)) 
    def test_audit(self):
        node = pipes.AuditNode()
        self.setup_node(node)
        self.create_distinct_sample()

        self.initialize_node(node)

        self.assertEqual(6, len(node.output_fields)) 

        node.run()
        node.finalize()

        self.assertEqual(5, len(self.output.buffer)) 
        
    def test_strip(self):
        node = pipes.StringStripNode(fields = ["custom"])

        self.setup_node(node)
        self.create_sample(custom = "  foo  ")

        self.initialize_node(node)

        node.run()
        node.finalize()

        self.assertEqual("foo", self.output.buffer[0][3]) 

    def test_strip_auto(self):
        fields = brewery.ds.fieldlist([("str1", "string"), 
                                       ("x","unknown"), 
                                       ("str2","string"), 
                                       ("f", "unknown")])
        self.input.fields = fields
        for i in range(0, 5):
            self.input.put([" foo ", " bar ", " baz ", " moo "])

        node = pipes.StringStripNode()

        self.setup_node(node)

        self.initialize_node(node)

        node.run()
        node.finalize()

        row = self.output.buffer[0]
        self.assertEqual(["foo", " bar ", "baz", " moo "], row) 

    def test_consolidate_type(self):
        fields = brewery.ds.fieldlist([("s", "string"), 
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


        node = pipes.CoalesceValueToTypeNode()

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
        