# Brewery
import json
import os

brewery_search_paths = ['/etc/brewery', \
				 '~/.brewery/datastores.json', \
				 './config/datastores.json']
default_datastores_file_name = "datastores.json"

datastores = {}

datastore_adapters = {}

def set_brewery_search_paths(paths):
	global brewery_search_paths
	brewery_search_paths = paths
	
def load_default_datastores():
	"""Load Datastore information from default stores"""
	for path in brewery_search_paths:
		path = os.path.expanduser(path)
		if os.path.exists(path):
			load_datastores_file(os.path.join(path, default_datastores_file_name))

def load_datastores_file(path):
	"""Load datastores specified as json dictionary in file

	Example:
		{
		    "project" : {
		        "adapter": "postgres",
		        "host": "localhost",
		        "database": "project",
				"user": "me"
				"password": "secret"
		        "port": 5432
			}
		}

	Args:
		path: Path to file containing a json dictionary with datastore information
	"""
	file = open(path)
	new_stores = json.load(file)
	if not issubclass(new_stores.__class__, dict):
		raise TypeError("Datastores file '%s' sohould contain a dictionary" % path)
	# FIXME: should use add_datastore for each store to check structure/type
	datastores.update(new_stores)

def add_datastore(name, info):
	"""Add datastore 'info' into managed data stores

	Args:
		name: name of datastore
		info: dictionary containing datastore information. For example, relational database connection
			information would contain keys: adapter, database, host, port, user, password
	"""
	if not issubclass(info.__class__, dict):
		raise TypeError("Datastore info for '" + name + "' sohould be a dictionary")
	datastores[name] = info

def remove_datastore(name):
	del datastores[name]

def datastore_with_name(name):
	"""Get datastore information with given name

	Args:
		name: datastore name

	Returns:
		A dictionary with datastore information """
	if name in datastores:
		return datastores[name]
	else:
		raise KeyError("No datastore with name '%s'" % name)
		
def connect_datastore(datastore_name):
	info = datastore_with_name(datastore_name)
	adapter_name = info["adapter"]
	adapter = __datastore_adapter(adapter_name)
	return adapter.connect(info)

def __datastore_adapter(name):
	global datastore_adapters
	if adapter_name in datastore_adapters:
		adapter = datastore_adapters[adapter_name]
	else:
		module_name = "brewery.datastores." + adapter
		adapter = __import__(module_name)
		datastore_adapters[name] = adapter
	return adapter
