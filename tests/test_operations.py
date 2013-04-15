import unittest
import brewery
from brewery.errors import *
from brewery.operations import *
from brewery import DataObject

def default(left):
    pass

def unary(left):
    pass

def binary(left, right):
    pass

class DummyDataObject(DataObject):
    def __init__(self, reps=None):
        self.reps = reps or []

    def representations(self):
        return self.reps

class OperationsBaseTestCase(unittest.TestCase):
    def test_basic_match(self):
        self.assertTrue(signature_match("*", ["foo"]))
        self.assertTrue(signature_match("foo", ["foo"]))
        self.assertTrue(signature_match("foo", ["bar", "foo"]))
        self.assertFalse(signature_match("foo", ["bar"]))
    def test_list_match(self):
        self.assertFalse(signature_match("*[]", ["foo"]))
        self.assertFalse(signature_match("foo[]", ["foo"]))
        self.assertFalse(signature_match("foo[]", ["bar", "foo"]))

        self.assertFalse(signature_match("*", ["foo"],True))
        self.assertFalse(signature_match("foo", ["foo"], True))
        self.assertFalse(signature_match("foo", ["bar", "foo"], True))

        self.assertTrue(signature_match("*[]", ["foo"], True))
        self.assertTrue(signature_match("foo[]", ["foo"], True))
        self.assertTrue(signature_match("foo[]", ["bar", "foo"], True))

    def test_common_reps(self):
        objs = [
                DummyDataObject(["a", "b", "c"]),
                DummyDataObject(["a", "b", "d"]),
                DummyDataObject(["b", "d", "e"])
            ]
        self.assertEqual(["b"], list(common_representations(*objs)))

        objs = [
                DummyDataObject(["a", "b", "c"]),
                DummyDataObject(["a", "b", "d"]),
                DummyDataObject(["d", "d", "e"])
            ]
        self.assertEqual([], list(common_representations(*objs)))

    def test_extract_representations(self):
        obj = DummyDataObject(["rows", "sql"])
        self.assertEqual( [(["rows", "sql"],False)], extract_representations(obj))

        obj = DummyDataObject(["rows", "sql"])
        extr = extract_representations([obj])
        self.assertEqual( [(["rows", "sql"],True)], extr)

    def test_match(self):
        obj_sql = DummyDataObject(["rows", "sql"])
        obj_rows = DummyDataObject(["rows"])

        operation("sql")(unary)
        operation("*", name="unary")(default)

        match = match_operation("unary", obj_sql)
        self.assertEqual(unary, match)

        with self.assertRaises(OperationError):
            match_operation("foo", obj_sql)

        with self.assertRaises(OperationError):
            match_operation("unary", obj_sql, obj_sql)

    def test_comparison(self):
        sig1 = Signature("a", "b", "c")
        sig2 = Signature("a", "b", "c")
        sig3 = Signature("a", "b")

        self.assertTrue(sig1 == sig1)
        self.assertTrue(sig1 == sig2)
        self.assertFalse(sig1 == sig3)

        self.assertTrue(sig1 == ["a", "b", "c"])
        self.assertFalse(sig1 == ["a", "b"])

    def test_delete(self):
        om = OperationMap()
        obj = DummyDataObject(["rows"])

        om.add("unary", unary, Signature("rows"))
        om.add("unary", default, Signature("*"))

        match = om.match("unary", obj)
        self.assertEqual(unary, match)

        om.remove("unary", ["rows"])
        match = om.match("unary", obj)
        self.assertEqual(default, match)

        om.remove("unary")
        with self.assertRaises(OperationError):
            match_operation("unary", obj)

if __name__ == "__main__":
    unittest.main()
