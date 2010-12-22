"""OLAP Cube"""

from dimension import *
from hierarchy import *
from level import *
import brewery

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
        self._dimensions = {}
        self.mappings = info.get("mappings", {})
    
    def add_dimension(self, dimension):
        """Add dimension to cube. Replace dimension with same name"""
        
        # FIXME: Do not allow to add dimension if one already exists
        self._dimensions[dimension.name] = dimension
        if self.model:
            self.model.add_dimension(dimension)

    def remove_dimension(self, dimension):
        """Remove a dimension from receiver"""
        del self._dimensions[dimension.name]

    @property
    def dimensions(self):
        return self._dimensions.values()
        
    def dimension(self, name):
        """Get dimension by name"""
        return self._dimensions[dimension.name]
        
    def validate(self):
        """Validate cube. See Model.validate() for more information. """
        results = []

        if not self.mappings:
            results.append( ('error', "No mappings for cube '%s'" % self.name) )

        # 1. collect all fields(attributes) and check whether there is a mapping for that
        for measure in self.measures:
            try:
                mapping = self.measure_mapping(measure)
            except KeyError:
                results.append( ('error', "No mapping for measure '%s' in cube '%s'" % (measure, self.name)) )
            else:
                split = brewery.split_field(mapping)
                if len(split) <= 1:
                    results.append( ('error', "Mapping '%s' for measure '%s' in cube '%s' " \
                                              "has no table/dataset name" % (mapping, measure, self.name)) )

        # 2. check whether dimension attributes are unique
        # 3. check whether dimension has valid keys
        

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

    def measure_mapping(self, measure):
        """Return mapping for a measure"""
        
        mapped = self.mappings.get("fact.%s" % measure)
        if not mapped:
            # FIXME: this should be depreciated
            mapped = self.mappings.get("%s" % measure)

        if not mapped:
            raise KeyError("Cube '%s' has no mapping for measure '%s'" % (self.name, measure))

        return mapped
