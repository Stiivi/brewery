import brewery.pipes as pipes
import brewery.ds as ds
import unittest

class ProcessingNetworkTestCase(unittest.TestCase):
    def test_connections(self):
        network = pipes.Network()
        node = pipes.Node()
        network.add(node, "source")

        node = pipes.Node()
        network.add(node, "csv_target")

        network.connect("source", "csv_target")

        node = pipes.Node()
        network.add(node, "sample")
        network.connect("source", "sample")

        node = pipes.Node()
        network.add(node, "html_target")

        network.connect("sample", "html_target")

        self.assertEqual(4, len(network.nodes))
        self.assertEqual(3, len(network.connections))

        self.assertRaises(KeyError, network.connect, "sample", "unknown")

        node = pipes.Node()
        self.assertRaises(KeyError, network.add, node, "sample")
        
        network.remove("sample")
        self.assertEqual(3, len(network.nodes))
        self.assertEqual(1, len(network.connections))
