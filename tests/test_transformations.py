import unittest
import brewery
from brewery.objects import *
from brewery.errors import *
import os.path
import brewery.transform as transform

class TransformTestCase(unittest.TestCase):
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

    def test_transform_source(self):
        s = transform.TransformationSource(None, fields=self.fields)

        self.assertIsInstance(s.same(),transform.IdentityElement)
        self.assertIsInstance(s["id"],transform.FieldElement)
        self.assertIsInstance(s[["id", "label"]],transform.FieldListElement)
        # self.assertIsInstance(transform.LookupTransformation, s.lookup({}))


    def test_target_basics(self):
        s = transform.TransformationSource(None, fields=self.fields)
        t = transform.TransformationTarget()
        t["id"] = 10
        self.assertIsInstance(t.output["id"], transform.ValueElement)
        self.assertEqual(10, t.output["id"].value)

        t["id"] = s
        self.assertIsInstance(t.output["id"], transform.IdentityElement)

        t["id"] = s["id"]
        self.assertIsInstance(t.output["id"], transform.FieldElement)
        self.assertEqual(self.fields["id"], t.output["id"].field)
        self.assertEqual(s, t.output["id"].source)

        with self.assertRaises(BreweryError):
            s[list]

        with self.assertRaises(BreweryError):
            s[s["foo"]]

        t["id"] = s["id"].missing("unknown")
        self.assertIsInstance(t.output["id"], transform.FieldElement)
        self.assertEqual("unknown", t.output["id"].missing_value)
        self.assertIs(s, t.output["id"].source)

        t["id"] = s["id"].mapping({})
        self.assertIsInstance(t.output["id"], transform.MappingElement)

    def test_nested(self):
        s = transform.TransformationSource(None, fields=self.fields)
        t = transform.TransformationTarget()

        t["id"] = s["id"].missing(s["label"])
        self.assertIsInstance(t.output["id"], transform.FieldElement)
        self.assertIsInstance(t.output["id"].missing_value,
                                        transform.FieldElement)
        self.assertEqual("label", t.output["id"].missing_value.field.name)

    @unittest.skip("not yet")
    def test_context(self):
        trans = transform.Transformation(None, self.fields)
        with trans as (t, s):
            t["id"] = s["id"].missing(0)
            t["name"] = "unknown"

        comp = transform.TransformationCompiler(trans.target, trans.source)
        comp.compile()

def test_suite():
   suite = unittest.TestSuite()

   suite.addTest(unittest.makeSuite(TransformTestCase))

   return suite


if __name__ == '__main__':
    unittest.main()


