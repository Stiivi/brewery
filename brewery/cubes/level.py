import brewery.utils

class Level(object):
    """Hierarchy level

    Attributes:
        * name: level name
        * label: human readable label 
        * key: key field of the level (customer number for customer level, region code for region level, 
            year-month for month level). key will be used as a grouping field for aggregations. Key should be unique within level.
        * label_attribute: name of attribute containing label to be displayed (customer_name for customer level,
            region_name for region level, month_name for month level)
        * attributes: list of other additional attributes that are related to the level. The attributes are not being used for aggregations, 
            they provide additional useful information
    """
    def __init__(self, name, desc, dimension = None):
        self.name = name
        self.label = desc.get("label", "")
        self.key = desc.get("key", None)
        self.attributes = desc.get("attributes", [])
        self.label_attribute = desc.get("label_attribute", [])
        self.dimension = dimension

    def to_dict(self):
        """Convert to dictionary"""
        
        out = brewery.utils.IgnoringDictionary()
        out.setnoempty("name", self.name)
        out.setnoempty("label", self.label)
        out.setnoempty("key", self.key)
        out.setnoempty("attributes", self.attributes)
        out.setnoempty("label_attribute", self.label_attribute)

        return out
