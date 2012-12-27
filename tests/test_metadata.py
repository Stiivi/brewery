import unittest
import brewery
from brewery.errors import *
from copy import copy

class FieldListTestCase(unittest.TestCase):
    def test_list_creation(self):
        fields = brewery.FieldList(["foo", "bar"])

        for field in fields:
            self.assertEqual(type(field), brewery.Field)

        self.assertEqual("foo", fields[0].name, 'message')
        self.assertEqual(2, len(fields))

    def test_list_add(self):
        fields = brewery.FieldList(["foo", "bar"])
        fields.append("baz")
        self.assertEqual(3, len(fields))

    def test_indexes(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        indexes = fields.indexes( ["a", "c", "d"] )
        self.assertEqual((0,2,3), indexes)

        indexes = fields.indexes( fields.fields() )
        self.assertEqual((0,1,2,3), indexes)

    def test_deletion(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        del fields[0]

        self.assertEqual(["b", "c", "d"], fields.names())

        del fields[2]
        self.assertEqual(["b", "c"], fields.names())

        self.assertRaises(NoSuchFieldError, fields.field, "d")
        self.assertEqual(2, len(fields))

    def test_contains(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        field = brewery.Field("a")
        self.assertEqual(True, "a" in fields)
        self.assertEqual(True, field in fields._fields)

    #def test_retype(self):
    #    fields = brewery.FieldList(["a", "b", "c", "d"])
    #    self.assertEqual("unknown", fields.field("a").storage_type)
    #    retype_dict = {"a": {"storage_type":"integer"}}
    #    fields.retype(retype_dict)
    #    self.assertEqual("integer", fields.field("a").storage_type)

    #    retype_dict = {"a": {"name":"foo"}}
    #    self.assertRaises(Exception, fields.retype, retype_dict)

    def test_mask(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        mask = fields.mask(["b", "d"])
        self.assertEqual([False, True, False, True], mask)

    def test_copy(self):
        fields = brewery.FieldList(["a", "b", "c"])
        for field in fields:
            field.freeze()

        cfields = fields.clone()
        self.assertFalse(all(f.is_frozen for f in cfields))

class MetadataTestCase(unittest.TestCase):
    def test_names(self):
        field = brewery.Field("bar")
        self.assertEqual("bar", field.name)
        self.assertEqual("bar", str(field))

    def test_to_field(self):
        field = brewery.to_field("foo")
        self.assertIsInstance(field, brewery.Field)
        self.assertEqual("foo", field.name)
        # self.assertEqual("unknown", field.storage_type)
        # self.assertEqual("typeless", field.analytical_type)

        field = brewery.to_field(["bar", "string", "flag"])
        self.assertEqual("bar", field.name)
        self.assertEqual("string", field.storage_type)
        self.assertEqual("flag", field.analytical_type)

        desc = {
                "name":"baz",
                "storage_type":"integer",
                "analytical_type": "flag"
            }
        field = brewery.to_field(desc)
        self.assertEqual("baz", field.name)
        self.assertEqual("integer", field.storage_type)
        self.assertEqual("flag", field.analytical_type)

    def test_field_to_dict(self):
        desc = {
                "name":"baz",
                "storage_type":"integer",
                "analytical_type": "flag"
            }
        field = brewery.to_field(desc)
        field2 = brewery.to_field(field.to_dict())
        self.assertEqual(field, field2)

    def test_coalesce_value(self):
        self.assertEqual(1, brewery.coalesce_value("1", "integer"))
        self.assertEqual("1", brewery.coalesce_value(1, "string"))
        self.assertEqual(1.5, brewery.coalesce_value("1.5", "float"))
        self.assertEqual(1000, brewery.coalesce_value("1 000", "integer", strip=True))
        self.assertEqual(['1','2','3'], brewery.coalesce_value("1,2,3", "list", strip=True))

    def test_immutable(self):
        field = brewery.Field("bar")
        field.freeze()

        try:
            field.name = "foo"
            self.fail()
        except AttributeError:
            pass

        # This should pass
        field2 = copy(field)
        field2.name = "bar"

    def test_hash(self):
        d = {}
        field = brewery.Field("foo")
        try:
            d[field] = 10
            self.fail("Unfrozen field should not be hashable")
        except TypeError:
            pass

        field.freeze()
        d[field] = 10
        self.assertEqual(10, d[field])

    def setUp(self):
        self.fields = brewery.FieldList(["a", "b", "c", "d"])

    def test_init(self):
        self.assertRaises(MetadataError, brewery.FieldFilter, drop=["foo"], keep=["bar"])

    def test_map(self):

        m = brewery.FieldFilter(drop=["a","c"])
        self.assertListEqual(["b", "d"], m.filter(self.fields).names())

        m = brewery.FieldFilter(keep=["a","c"])
        self.assertListEqual(["a", "c"], m.filter(self.fields).names())

        m = brewery.FieldFilter(rename={"a":"x","c":"y"})
        self.assertListEqual(["x", "b", "y", "d"], m.filter(self.fields).names())

    def test_selectors(self):
        m = brewery.FieldFilter(keep=["a","c"])
        self.assertListEqual([True, False, True, False],
                                m.field_mask(self.fields))

        m = brewery.FieldFilter(drop=["b","d"])
        self.assertListEqual([True, False, True, False],
                                m.field_mask(self.fields))

        m = brewery.FieldFilter()
        self.assertListEqual([True, True, True, True],
                                m.field_mask(self.fields))
def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(FieldListTestCase))
   suite.addTest(unittest.makeSuite(MetadataTestCase))

   return suite

