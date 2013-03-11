import unittest
import brewery
from brewery.objects import *
from brewery.errors import *
import os.path
import brewery.ops as ops

class OperationsBaseTestCase(unittest.TestCase):
    def setUp(self):
        self.data = [
                    [1, "one"],
                    [2, "two"],
                    [3, "three"],
                    [3, "three2"],
                    [3, "three3"],
                    [2, "two2"],
                    [2, "two3"],
                    [4, "four"]
                ]
        self.fields = brewery.FieldList([
                                ("id", "integer"),
                                ("label", "string")
                            ])

class SQLOperationsTestCase(OperationsBaseTestCase):
    def setUp(self):
        super(SQLOperationsTestCase, self).setUp()

        self.store = brewery.objects.SQLDataStore("sqlite:///")
        obj = self.store.create("data", self.fields)

        for row in self.data:
            obj.append(row)
        obj.flush()

        self.source = obj.as_source()

    def execute(self, statement):
        """Execute `statement` in receiver's store and return result as a
        list"""
        return list(self.store.execute(statement))

    def test_size(self):
        self.assertEqual(len(self.data), len(self.source))

    def test_distinct(self):
        statement = self.source.sql_statement()
        statement = ops.sql.distinct(statement, ["id"])
        result = self.execute(statement)
        self.assertEqual(4, len(result))

    def test_field_filter(self):
        ff = brewery.FieldFilter(keep=["id"])
        statement = self.source.sql_statement()
        statement = ops.sql.field_filter(statement, self.source.fields, ff)
        out = self.execute(statement)

        # test if we get only one field
        self.assertEqual(1, len(out[0]))

        # test if the result equals to the "id" column
        col = [(r[0], ) for r in self.data]
        self.assertEqual(col, out)

class IteratorOperationsTestCase(OperationsBaseTestCase):

    def setUp(self):
        super(IteratorOperationsTestCase, self).setUp()
        self.source = brewery.objects.IterableDataSource(self.data, self.fields)

    def test_distinct(self):
        iterator = self.source.rows()
        l = list(ops.iterator.distinct(iterator, self.fields, ["id"]))
        self.assertEqual(4, len(l))

    def test_field_filter(self):
        iterator = self.source.rows()
        ff = brewery.FieldFilter(keep=["id"])
        out = list(ops.iterator.field_filter(iterator, self.fields, ff))

        # test if we get only one field
        self.assertEqual(1, len(out[0]))

        # test if the result equals to the "id" column
        col = [[r[0]] for r in self.data]
        self.assertEqual(col, out)

    def test_left_join(self):
        master = [ (1, "apple", "fruit"),
                    (2, "bannana", "fruit"),
                    (3, "orange", "fruit"),
                    (4, "carrot", "vegetable"),
                    (5, "cucumber", "vegetable")]
        detail1 = [ (1, 10), (2, 20), (3, 30) ]
        detail2 = [ ("fruit", "sweet"), ("vegetable", "meh") ]

        result = ops.iterator.left_inner_join(master, [detail1], [(0,0)])
        result = list(result)
        self.assertEqual(3, len(result))
        self.assertTrue(all(len(row) == 5 for row in result))

        result = ops.iterator.left_inner_join(master, [detail2], [(2,0)])
        result = list(result)
        self.assertEqual(5, len(result))
        self.assertTrue(all(len(row) == 5 for row in result))

        with self.assertRaisesRegexp(ArgumentError, "should be a join"):
            result = ops.iterator.left_inner_join(master, [detail2], [])
            resul = list(result)

        with self.assertRaisesRegext(ArgumentError, "No details"):
            result = ops.iterator.left_inner_join(master, [], [])
            resul = list(result)

class TransformationTestCase(unittest.TestCase):
    def setUp(self):
        self.row = [1, "janko", "ulica"]
        self.rows = [
                    [1, "janko", "ulica"],
                    [2, "ferko", "potok"],
                    [3, "anicka", "na moste"],
                ]
        self.fields = brewery.FieldList([("id", "integer"),
                                         ("name", "string"),
                                         ("address", "string")])
    def transform(self, row, trans):
        trans = ops.iterator.compile_transformation(trans, self.fields)
        return trans(row)

    def test_copy(self):
        trans = ( ("id", ), )
        self.assertEqual([1], self.transform(self.row, trans))

        trans = ( ("id", None), )
        self.assertEqual([1], self.transform(self.row, trans))

        trans = ( ("id", "id"), )
        self.assertEqual([1], self.transform(self.row, trans))

        trans = ( ("id", "name"), )
        self.assertEqual(["janko"], self.transform(self.row, trans))

        trans = ( ("id", "UNKNOWN"), )
        self.assertRaises(NoSuchFieldError, self.transform, self.row, trans)

    def test_set_value(self):
        trans = ( ("name", {"action":"set", "value":"censored"}), )
        self.assertEqual(["censored"], self.transform(self.row, trans))

    def test_map(self):
        mapping = { "janko": 10, "ferko": 20 }
        trans = ( ("name", {"action":"map", "map":mapping}), )
        self.assertEqual([10], self.transform(self.row, trans))
        trans = (
                    ("name", {  "action":"map",
                                "map":mapping,
                                "missing_value":"unknown"
                             }
                    ),
                )
        self.assertEqual(["unknown"], self.transform(self.rows[2], trans))

    def test_function(self):
        def upper(val):
            return val.upper()

        def ignore(val):
            return None

        def decorate(val, decorator):
            return decorator + val + decorator

        def concatenate(val, another, separator):
            return val + separator + another

        trans = ( ("name", {"action":"function", "function": upper}), )
        self.assertEqual(["JANKO"], self.transform(self.row, trans))

        trans = ( ("name", {"action":"function", "function": ignore}), )
        self.assertEqual([None], self.transform(self.row, trans))

        trans = ( ("name", {"action":"function", "function": ignore,
                            "missing_value":"unknown"}), )
        self.assertEqual(["unknown"], self.transform(self.row, trans))

        trans = ( ("name", {
                                "action":"function",
                                "function": decorate,
                                "args": {"decorator": "--"}
                            }), )
        self.assertEqual(["--janko--"], self.transform(self.row, trans))

        trans = ( ("name", {
                                "action":"function",
                                "function": concatenate,
                                "source": ["name", "address"],
                                "args": {"separator": ", "}
                            }), )
        self.assertEqual(["janko, ulica"], self.transform(self.row, trans))

def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(IteratorOperationsTestCase))
   suite.addTest(unittest.makeSuite(SQLOperationsTestCase))

   return suite


if __name__ == '__main__':
    unittest.main()


