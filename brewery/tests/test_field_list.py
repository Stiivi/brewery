import unittest
import brewery
import brewery.ds as ds

class FieldListCase(unittest.TestCase):
    def test_names(self):
        self.assertEqual("foo", ds.field_name("foo"))
        field = ds.Field("bar")
        self.assertEqual("bar", ds.field_name("bar"))

        self.assertEqual(["foo", "bar"], ds.field_names(["foo", "bar"]))
        fields = [ds.Field("foo"), ds.Field("bar")]
        self.assertEqual(["foo", "bar"], ds.field_names(fields))

    def test_list_creation(self):
        fields = ds.FieldList(["foo", "bar"])

        for field in fields:
            self.assertEqual(type(field), ds.Field)

        self.assertEqual("foo", fields[0].name, 'message')
        self.assertEqual(2, len(fields))

    def test_list_add(self):
        fields = ds.FieldList(["foo", "bar"])
        fields.append("baz")
        self.assertEqual(3, len(fields))
        
    def test_indexes(self):
        fields = ds.FieldList(["a", "b", "c", "d"])
        indexes = fields.indexes( ["a", "c", "d"] )
        self.assertEqual((0,2,3), indexes)

        indexes = fields.indexes( fields.fields() )
        self.assertEqual((0,1,2,3), indexes)

    def test_deletion(self):
        fields = ds.FieldList(["a", "b", "c", "d"])
        del fields[0]
        
        self.assertEqual(["b", "c", "d"], fields.names())
        
        del fields[2]
        self.assertEqual(["b", "c"], fields.names())
        
        self.assertRaises(KeyError, fields.field, "d")
        self.assertEqual(2, len(fields))
        
    def test_contains(self):
        fields = ds.FieldList(["a", "b", "c", "d"])
        field = ds.Field("a")
        
        self.assertEqual(True, "a" in fields)
        self.assertEqual(True, field in fields)
        