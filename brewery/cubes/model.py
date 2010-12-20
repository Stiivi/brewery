class Model(object):
    """
    Logical Model represents mapping between human point of view on analysed facts and their physical
    database representation. Main objects of a model are datasets (physical objects) and multidimensioanl
    cubes (logical objects). For more information see Cube.
    """

    def __init__(self, name):
    	self.name = name
    	self.label = ""
    	self.description = ""
    	self.dimensions = {}
    	self.cubes = {}