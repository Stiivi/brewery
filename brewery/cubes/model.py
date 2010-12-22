from dimension import *
from cube import *

class Model(object):
    """
    Logical Model represents mapping between human point of view on analysed facts and their physical
    database representation. Main objects of a model are datasets (physical objects) and multidimensioanl
    cubes (logical objects). For more information see Cube.
    """

    def __init__(self, name, info = {}):
    	self.name = name
    	self.label = info.get('label','')
    	self.description = info.get('description','')

    	self.dimensions = {}

    	dims = info.get('dimensions','')

    	if dims:
    	    for dim_name, dim_info in dims.items():
    	        dim = Dimension(dim_name, dim_info)
                self.dimensions[dim_name] = dim
    	        
    	self.cubes = {}

    def create_cube(self, cube_name, info ={}):
        """Create a Cube instance for the model. This is designated factory method for cubes as it
        properrely creates references to dimension objects
        
        Args:
            cube_name: name of a cube to be created
            info: dict object with cube information

        Returns:
            freshly created and initialized Cube instance
        """

        cube = Cube(cube_name, info)
        cube.model = self
        self.cubes[cube_name] = cube

    	dims = info.get('dimensions','')
    	
    	if dims and type(dims) == list:
    	    for dim_name in dims:
    	        dim = self.dimensions.get(dim_name)
    	        if not dim:
    	            raise KeyError("There is no dimension '%s' for cube '%s' in model '%s'" % (dim_name, cube_name, self.name))
                cube.dimensions[dim_name] = dim

        return cube

    def validate(self):
        """Validate the model, check for model consistency. Validation result is array of tuples in form:
        (validation_result, message) where validation_result can be 'warning' or 'error'.
        
        Returs: array of tuples
        """
        
        results = []
        
        ################################################################
        # 1. Chceck dimensions
        is_fatal = False
        for dim_name, dim in self.dimensions.items():
            if not issubclass(dim.__class__, Dimension):
                results.append(('error', "Dimension '%s' is not a subclass of Dimension class" % dim_name))
                is_fatal = True

        # We are not going to continue if there are no valid dimension objects, as more errors migh emerge
        if is_fatal:
            return results

        for dim in self.dimensions.values():
            results.extend(dim.validate())

        ################################################################
        # 2. Chceck cubes

        if len(self.cubes) == 0:
            results << ('warning', 'No cubes defined')
        else:
            for cube_name, cube in self.cubes.items():
                results.extend(cube.validate())

            return results
        