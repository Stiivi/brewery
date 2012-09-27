import carray

storage_type_map = {
            "integer": "i",
            "float": "f",
            "string": "S250",
            # "text": None, # Not supported yet
            "boolean": "i"
        }

def create_array(field):
    """Creates a carray instance"""

    storage_type = field.concrete_storage_type

    if not storage_type:
        storage_type = storage_type_map[field.storage_type]

    array = carray.carray([], dtype=storage_type)
    return array

def create_table(columns, fields):
    """Create a ctable instance from carrays"""

    names = [str(field) for field in fields]
    table = CTable(columns,names=names)
    return table

class CTable(carray.ctable):
    
    # Backward compatibility, will be removed
    def rows(self):
        return self
