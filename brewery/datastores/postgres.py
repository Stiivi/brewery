# PostgreSQL module
import postgresql.driver.dbapi20

def connect(info):
	connection = postgresql.driver.dbapi20.connect(**info)
	return connection