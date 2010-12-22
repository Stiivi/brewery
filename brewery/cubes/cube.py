"""OLAP Cube"""

from dimension import *
from hierarchy import *
from level import *
import brewery

class Cube(object):
    """
    OLAP Cube - Logical Representation
    
    Attributes:
    	* model: logical model
    	* name: cube name
    	* label: name that will be displayed (human readable)
    	* measures: list of fact measures
    	* dimensions: list of fact dimensions
    	* mappings: map logical attributes to physical dataset fields (table columns)
        * fact: dataset containing facts (fact table)
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
        self.fact = info.get("fact", None)
    
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
        
    def to_dict(self):
        """Convert to dictionary"""

        out = brewery.utils.IgnoringDictionary()
        out.setnoempty("name", self.name)
        out.setnoempty("label", self.label)
        out.setnoempty("measures", self.measures)
        
        dims = [dim.name for dim in self.dimensions]

        # Give sorted list so we can nicely compare dictionaries
        out.setnoempty("dimensions", dims.sort())

        out.setnoempty("mappings", self.mappings)
        out.setnoempty("fact", self.fact)
        
        return out

    def validate(self):
        """Validate cube. See Model.validate() for more information. """
        results = []

        if not self.mappings:
            results.append( ('error', "No mappings for cube '%s'" % self.name) )

        if not self.fact:
            results.append( ('warning', "No fact specified for cube '%s' (factless cubes are not yet supported, "
                                        "using 'fact' as default dataset/table name)" % self.name) )
            
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

        for dimension in self.dimensions:
            attributes = dimension.all_attributes()
            for attribute in attributes:
                try:
                    mapping = self.dimension_attribute_mapping(dimension, attribute)
                except KeyError:
                    results.append( ('warning', "No mapping for dimension '%s' attribute '%s' in cube '%s' " \
                                                "(using default mapping)" % (dimension.name, attribute, self.name)) )
                else:
                    split = brewery.split_field(mapping)
                    if len(split) <= 1:
                        results.append( ('error', "Mapping '%s' for dimension '%s' attribute '%s' in cube '%s' " \
                                                  "has no table/dataset name" 
                                                  % (mapping, dimension.name, attribute, self.name)) )


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

    def dimension_attribute_mapping(self, dimension, attribute):
        """Return mapping for a dimension attribute. If there is no mapping defined return default mapping where
        table/dataset name is same as dimension name and column/field name is same as dimension attribute
        
        Return: string
        """

        reference = "%s.%s" % (dimension.name, attribute)
        mapped = self.mappings.get(reference)

        # If there is no mapping, use default mapping
        if not mapped:
            mapped = reference

        return mapped

    def mapped_field(self, logical_field):
        """Return physical field name"""
        split = logical_field.split('.')
        if len(split) == 1:
            return self.measure_mapping(logical_field)
        elif split[0] == 'fact':
            return self.measure_mapping(logical_field[1])
        else:
            mapping = self.mappings.get(reference)
            if not mapping:
                mapping = logical_field
            return mapping
            