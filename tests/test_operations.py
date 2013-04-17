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
    def __init__(self, reps=None, data=None):
        self.reps = reps or []
        self.data = data

    def representations(self):
        return self.reps

class TextObject(DataObject):
    def __init__(self, string):
        self.string = string

    def representations(self):
        return ["rows", "text"]

    def rows(self):
        return iter(self.string)

    def text(self):
        return self.string

class OperationsBaseTestCase(unittest.TestCase):
    def test_match(self):
        self.assertTrue(Signature("sql").matches("sql"))
        self.assertTrue(Signature("*").matches("sql"))
        self.assertTrue(Signature("sql[]").matches("sql[]"))
        self.assertTrue(Signature("*[]").matches("sql[]"))

        self.assertFalse(Signature("sql").matches("rows"))
        self.assertFalse(Signature("sql").matches("sql[]"))

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

    def test_extract_signatures(self):
        obj = DummyDataObject(["rows", "sql"])
        self.assertEqual( [["rows", "sql"]], extract_signatures(obj))

        obj = DummyDataObject(["rows", "sql"])
        extr = extract_signatures([obj])
        self.assertEqual( [["rows[]", "sql[]"]], extr)

    def test_lookup(self):
        k = OperationKernel()

        obj_sql = DummyDataObject(["sql"])
        obj_rows = DummyDataObject(["rows"])

        k.register_operation("unary", unary, Signature("sql"))
        k.register_operation("unary", default, Signature("*"))

        match = k.lookup_operation("unary", obj_sql)
        self.assertEqual(unary, match)

        match = k.lookup_operation("unary", obj_rows)
        self.assertEqual(default, match)

        with self.assertRaises(OperationError):
            k.lookup_operation("foo", obj_sql)

        with self.assertRaises(OperationError):
            k.lookup_operation("unary", obj_sql, obj_sql)

    def test_lookup_additional_args(self):
        def func(obj, value):
            pass

        k = OperationKernel()
        k.register_operation("func", func, Signature("rows"))

        obj = DummyDataObject(["rows"])

        k.func(obj, 1)

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
        k = OperationKernel()
        obj = DummyDataObject(["rows"])

        k.register_operation("unary", unary, Signature("rows"))
        k.register_operation("unary", default, Signature("*"))

        match = k.lookup_operation("unary", obj)
        self.assertEqual(unary, match)

        k.remove_operation("unary", ["rows"])
        match = k.lookup_operation("unary", obj)
        self.assertEqual(default, match)

        k.remove_operation("unary")
        with self.assertRaises(OperationError):
            k.lookup_operation("unary", obj)

    def test_running(self):
        def func_text(obj):
            text = obj.text()
            return list(text.upper())

        def func_rows(obj):
            rows = obj.rows()
            text = "".join(rows)
            return list(text.upper())

        k = OperationKernel()
        k.register_operation("upper", func_text, Signature("text"))
        k.register_operation("upper", func_rows, Signature("rows"))

        obj = TextObject("windchimes")

        result = k.upper(obj)
        self.assertEqual(list("WINDCHIMES"), result)
        # func = om.match("upper")

    def test_retry(self):
        def join_sql(l, r):
            if l.data == r.data:
                return "SQL"
            else:
                raise RetryOperation(["sql", "rows"])

        def join_iter(l, r):
            return "ITERATOR"

        def endless(l, r):
            raise RetryOperation(["sql", "sql"])

        local = DummyDataObject(["sql", "rows"], "local")
        remote = DummyDataObject(["sql", "rows"], "remote")

        k = OperationKernel()
        k.register_operation("join", join_sql, Signature("sql", "sql"))
        k.register_operation("join", join_iter, Signature("sql", "rows"))

        result = k.join(local, local)
        self.assertEqual(result, "SQL")

        result = k.join(local, remote)
        self.assertEqual(result, "ITERATOR")

        k.register_operation("endless", endless, Signature("sql", "sql"))
        with self.assertRaises(RetryError):
            result = k.endless(local, local)

    def test_priority(self):
        objsql = DummyDataObject(["sql", "rows"])
        objrows = DummyDataObject(["rows", "sql"])

        def fsql(obj):
            pass
        def frows(obj):
            pass

        k = OperationKernel()
        k.register_operation("meditate", fsql, Signature("sql"))
        k.register_operation("meditate", frows, Signature("rows"))

        self.assertEqual(fsql, k.lookup_operation("meditate", objsql))
        self.assertEqual(frows, k.lookup_operation("meditate", objrows))

        # Reverse order of registration, expect the same result
        k = OperationKernel()
        k.register_operation("meditate", frows, Signature("rows"))
        k.register_operation("meditate", fsql, Signature("sql"))

        self.assertEqual(fsql, k.lookup_operation("meditate", objsql))
        self.assertEqual(frows, k.lookup_operation("meditate", objrows))

if __name__ == "__main__":
    unittest.main()
