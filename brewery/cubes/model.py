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

    	self._dimensions = {}

    	dims = info.get('dimensions','')

    	if dims:
    	    for dim_name, dim_info in dims.items():
    	        dim = Dimension(dim_name, dim_info)
                self._dimensions[dim_name] = dim
    	        
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
    	        dim = self.dimension(dim_name)
    	        if not dim:
    	            raise KeyError("There is no dimension '%s' for cube '%s' in model '%s'" % (dim_name, cube_name, self.name))
                cube.add_dimension(dim)

        return cube

    def add_dimension(self, dimension):
        """Add dimension to cube. Replace dimension with same name"""

        # FIXME: Do not allow to add dimension if one already exists
        self._dimensions[dimension.name] = dimension

    def remove_dimension(self, dimension):
        """Remove a dimension from receiver"""
        del self._dimensions[dimension.name]
        # FIXME: check whether the dimension is not used in cubes

    @property
    def dimensions(self):
        return self._dimensions.values()

    def dimension(self, name):
        """Get dimension by name"""
        return self._dimensions[name]

    def validate(self):
        """Validate the model, check for model consistency. Validation result is array of tuples in form:
        (validation_result, message) where validation_result can be 'warning' or 'error'.
        
        Returs: array of tuples
        """
        
        results = []
        
        ################################################################
        # 1. Chceck dimensions
        is_fatal = False
        for dim_name, dim in self._dimensions.items():
            if not issubclass(dim.__class__, Dimension):
                results.append(('error', "Dimension '%s' is not a subclass of Dimension class" % dim_name))
                is_fatal = True

        # We are not going to continue if there are no valid dimension objects, as more errors migh emerge
        if is_fatal:
            return results

        for dim in self.dimensions:
            results.extend(dim.validate())

        ################################################################
        # 2. Chceck cubes

        if not self.cubes:
            results.append( ('warning', 'No cubes defined') )
        else:
            for cube_name, cube in self.cubes.items():
                results.extend(cube.validate())

        return results
            
    def is_valid(self, strict = False):
        """Check whether model is valid. Model is considered valid if there are no validation errors. If you want
        to be sure that there are no warnings as well, set *strict* to ``True``.
        
        Args:
            strict: If ``False`` only errors are considered fatal, if ``True`` also warnings will make model invalid.
            
        Returns:
            boolean flag whether model is valid or not.
        """
        results = self.validate()
        if not results:
            return True
            
        if strict:
            return False
            
        for result in results:
            if result[0] == 'error':
                return False
                
        return True
            
            
