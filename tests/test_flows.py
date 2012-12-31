import unittest
import brewery

class FlowTestCase(unittest.TestCase):

    def setUp(self):
        self.store = brewery.stores.SQLDataStore("sqlite:///")

        data = [
                    [1, "one"],
                    [2, "two"],
                    [3, "three"],
                    [3, "three2"],
                    [3, "three3"],
                    [2, "two2"],
                    [2, "two3"],
                    [4, "four"]
                ]
        fields = brewery.FieldList(["id", "label"])
        self.source = brewery.stores.IterableDataSource(data, fields)

    @unittest.skip("not yet")
    def test_unique(self):
        f = brewery.create_flow()
        f.csv_source("data.csv")
        f.unique("id")

        unique = f.fork("duplicates")
        unique.sql_target(table="duplicates", store=self.store)

        f.sql_target(table="unique", store=self.store)
    @unittest.skip("not yet")
    def test_unique_node(self):
        sources = {0: self.source}
        node = brewery.nodes.UniqueNode()
        node.fields = self.fields
        node.initialize()
        outputs = node.evaluate()
        self.assertTrue(isinstance(outputs, dict))
        self.assertEqual(2, len(outputs))
        result = node.outputs[0]

        node.best_representations()


def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(FlowTestCase))

   return suite

if __name__ == '__main__':
    unittest.main()
