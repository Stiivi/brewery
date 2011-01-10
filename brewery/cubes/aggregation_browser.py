import copy

class AggregationBrowser(object):
    """Class for browsing data cube aggregations

    :Attributes:
    * `cube` - cube for browsing
    """

    def __init__(self, cube):
        super(AggregationBrowser, self).__init__()
        self.cube = cube

    def full_cube(self):
        return Cuboid(self)
        
    def dimension_object(self, dimension):
        """Helper function to return proper dimension object as a subclass of Dimension.

        :Arguments:
            * `dimension` - a dimension object or a string, if it is a string, then dimension object
                is retrieved from cube
        """
        
        if type(dimension) == str:
            return self.cube.dimension(dimension)
        else:
            return dimension
            
class Cuboid(object):
    """Part of a cube determined by slicing dimensions. Immutable object."""
    def __init__(self, browser, cuts = []):
        self.browser = browser
        self.cube = browser.cube
        self.cuts = cuts

    def slice(self, dimension, path):
        """Create another cuboid by slicing receiving cuboid through `dimension` at `path`.
        Receiving object is not modified. If cut with dimension exists it is replaced with new one 
        
        Example::

            full_cube = browser.full_cube()
            contracts_2010 = full_cube.slice("date", [2010])
        
        Returns: new derived Cuboid object.
        """
        dimension = self.browser.dimension_object(dimension)
        cuts = self._filter_dimension_cuts(dimension, exclude = True)
        cut = PointCut(dimension, path)
        cuts.append(cut)
        return Cuboid(self.browser, cuts = cuts)
    
    def multi_slice(self, cuts):
        """Create another cuboid by slicing through multiple slices. `cuts` can be list or a dictionry.
        If it is a list, it should be a list of two item tuples where first item is a dimension, second
        item is a dimension cut path. If `cuts` is a dictionary, then keys are dimensions, values are
        cut paths.
        
        See :meth:`Cuboid.slice` for more information about slicing."""
                
        cuboid = self
                
        if type(cuts) == dict:
            for dim, path in cuts.items():
                cuboid = cuboid.slice(dim, path)
        elif type(cuts) == list or type(cuts) == tuple:
            for dim, path in cuts:
                cuboid = cuboid.slice(dim, path)
        else:
            raise TypeError("Cuts for multi_slice sohuld be a list or a dictionary, is '%s'" \
                                % cuts.__class__)
            
        return cuboid
    
    
    def _filter_dimension_cuts(self, dimension, exclude = False):
        dimension = self.browser.dimension_object(dimension)
        cuts = []
        for cut in self.cuts:
            if (exclude and cut.dimension != dimension) or (not exclude and cut.dimension == dimension):
                cuts.append(cut)
        return cuts

    def __eq__(self, other):
        """Cuboids are considered equal if:
            * they refer to the same cube within same browser
            * they have same set of cuts (regardless of their order)
        """
        
        if self.browser != other.browser:
            return False
        elif self.cube != other.cube:
            return False

        if len(self.cuts) != len(other.cuts):
            return False
            
        for cut in self.cuts:
            if cut not in other.cuts:
                return False
        
        return True

    def __ne__(self, other):
        return not self.__eq__(other)


class PointCut(object):
    """Object describing way of slicing a cube (cuboid) through point in a dimension"""
    
    def __init__(self, dimension, path):
        self.dimension = dimension
        self.path = path

    def __repr__(self):
        if type(self.dimension) == str:
            dim_name = self.dimension
        else:
            dim_name = self.dimension.name
        
        return '{"cut": "%s", "dimension":"%s", "path": "%s"}' % ("PointCut", dim_name, self.path)
        
    def __eq__(self, other):
        if self.dimension != other.dimension:
            return False
        elif self.path != other.path:
            return False
        return True
        
    def __ne__(self, other):
        return not self.__eq__(other)

