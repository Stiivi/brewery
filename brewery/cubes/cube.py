"""OLAP Cube"""

from dimension import *
from hierarchy import *
from level import *

class Cube(object):
    """
    OLAP Cube
    
    Attributes:
    	* model: logical model
    	* name: cube name
    	* label: name that will be displayed (human readable)
    	* measures: list of fact measures
    	* dimensions: list of fact dimensions
    """
    
    def __init__(self, name, info = {}):
        """Create a new cube
        
        Args:
            name (str): dimension name
            info (dict): dict object containing keys label, description, dimensions, ...
        """
        self.name = name

        self.label = info.get("label", "")
        self.description = info.get("description", "")
        self.measures = info.get("measures", [])

        self.model = None
        self.dimensions = {}
        self.mappings = info.get("mappings", [])
        
    def validate(self):
        """Validate cube. See Model.validate() for more information. """
        results = []

        if not self.mappings:
            results.append( ('error', "No mappings for cube '%s'" % self.name) )

        # if not mappings:
        #     results.append('error', "No mappings for cube '%s'" % self.name)
        # 
        # if !fact_dataset
        #     results << [:error, "Unable to find fact dataset '#{fact_dataset_name}' for cube '#{name}'"]
        # end
        # 
        # dimensions.each { | dim |
        #     dim.levels.each { |level|
        #         level.level_fields.each { |field|
        #             ref = field_reference(field)
        #             ds = logical_model.dataset_description_with_name(ref[0])
        #             if !ds
        #                 results << [:error, "Unknown dataset '#{ref[0]}' for field '#{field}', dimension '#{dim.name}', level '#{level.name}', cube '#{name}'"]
        #             else
        #                 fd = ds.field_with_name(ref[1])
        #                 if !fd
        #                     results << [:error, "Unknown dataset field '#{ref[0]}.#{ref[1]}' specified in dimension '#{dim.name}', level '#{level.name}', cube '#{name}'"]
        #                 end
        #             end
        #         }
        #     }
        # }
        # 
        return results
