import unittest
import brewery
import os

class DataStoreManagerTestCase(unittest.TestCase):

	def test_addition(self):
		store = {"adapter":"postgres", "host":"localhost", "database":"postgres"}
		brewery.add_datastore("foo", store)
		store2 = brewery.datastore_with_name('foo')
		self.assertEqual(store,store2,"Datastore should be the same")

	def test_removal(self):
		store = {"adapter":"postgres", "host":"localhost", "database":"postgres"}
		brewery.add_datastore("foo", store)
		brewery.remove_datastore("foo")
		self.assertRaises(KeyError, brewery.datastore_with_name, 'foo')

	def test_load(self):
		datastore_path = os.path.join(os.path.dirname(__file__), "datastores.json")
		brewery.load_datastores_file(datastore_path)
		store = brewery.datastore_with_name('test_psql')
		self.assertEqual(store["adapter"],"postgres", "Datastore should be the same")
		brewery.remove_datastore("test_psql")

	def test_load_default(self):
		datastore_path = os.path.dirname(__file__)
		brewery.set_brewery_search_paths([datastore_path])
		brewery.load_default_datastores()
		store = brewery.datastore_with_name('test_psql')
		self.assertEqual(store["adapter"],"postgres", "Datastore should be the same")

class ConnectionTestCase(unittest.TestCase):
	
	def setUp(self):
		datastore_path = os.path.dirname(__file__)
		brewery.set_brewery_search_paths([datastore_path])
		brewery.load_default_datastores()

	def test_sqlite(self):
		store = {"adapter":"sqlite3", "path":":memory:"}
		brewery.add_datastore("memory", store)
		connection = brewery.connect_datastore("memory")
		print("==> Connection: %s" % connection)

if __name__ == '__main__':
    unittest.main()

