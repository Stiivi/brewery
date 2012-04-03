import unittest
import brewery
import brewery.nodes
import brewery.probes as probes

class DataQualityTestCase(unittest.TestCase):
    def setUp(self):
        self.records = []
        for i in range(0,101):
            record = {}
            record["i"] = i
            record["bubble"] = i % 21 + 20

            # Some duplicates on key 'i'
            record["dup_i"] = i % 90

            # Some empty values
            if (i % 2) == 0:
                record["even"] = True
            else:
                record["even"] = None

            # Some missing values
            if (i % 7) == 0:
                record["seven"] = True

            if (i < 10) == 0:
                record["small"] = True
            else:
                record["small"] = False

            # Some set for distinct
            if (i % 3) == 0:
                record["type"] = "three"
            elif (i % 5) == 0:
                record["type"] = "five"
            elif (i % 7) == 0:
                record["type"] = "seven"
            else:
                record["type"] = "unknown"

            self.records.append(record)
        
    def test_completeness_probe(self):
        probe_i = probes.MissingValuesProbe()
        probe_even = probes.MissingValuesProbe()
        for record in self.records:
            probe_i.probe(record["i"])
            probe_even.probe(record["even"])
        
        self.assertEqual(0, probe_i.count)
        self.assertEqual(50, probe_even.count)
        
    def test_statistics_probe(self):
        probe_i = probes.StatisticsProbe()
        probe_bubble = probes.StatisticsProbe()

        for record in self.records:
            probe_i.probe(record["i"])
            probe_bubble.probe(record["bubble"])   
            
        self.assertEqual(0, probe_i.min)
        self.assertEqual(100, probe_i.max)
        self.assertEqual(50, probe_i.average)
        self.assertEqual(5050, probe_i.sum)

        self.assertEqual(20, probe_bubble.min)
        self.assertEqual(40, probe_bubble.max)
        self.assertEqual(2996, probe_bubble.sum)

    def test_distinct_probe(self):
        probe = probes.DistinctProbe()
        
        for record in self.records:
            probe.probe(record["type"])
        
        distinct = list(probe.values)
        distinct.sort()
        print "DISTINCT: %s" % distinct
        self.assertEqual(4, len(distinct))
        self.assertEqual(["five", "seven", "three", "unknown"], distinct)
        