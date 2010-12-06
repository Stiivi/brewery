# sqlite3 module
import sqlite3

def connect(connection_info):
	path = connection_info["path"]
	return sqlite3.connect(path)