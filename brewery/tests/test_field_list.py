#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import brewery
import brewery.ds as ds

class FieldListCase(unittest.TestCase):
    def test_names(self):
        field = brewery.Field("bar")
        self.assertEqual("bar", str("bar"))
        self.assertEqual(field.name, str("bar"))

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
        
        self.assertRaises(KeyError, fields.field, "d")
        self.assertEqual(2, len(fields))
        
    def test_contains(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        field = brewery.Field("a")
        
        self.assertEqual(True, "a" in fields)
        self.assertEqual(True, field in fields)
        
    def test_retype(self):
        fields = brewery.FieldList(["a", "b", "c", "d"])
        self.assertEqual("unknown", fields.field("a").storage_type)
        retype_dict = {"a": {"storage_type":"integer"}}
        fields.retype(retype_dict)
        self.assertEqual("integer", fields.field("a").storage_type)

        retype_dict = {"a": {"name":"foo"}}
        self.assertRaises(Exception, fields.retype, retype_dict)
        