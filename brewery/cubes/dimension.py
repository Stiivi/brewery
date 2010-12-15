"""Cube dimension"""

from hierarchy import *
from level import *

class Dimension(object):
    """
    Cube dimension.
    
    Attributes:
    	* model: logical model
    	* name: dimension name
    	* label: dimension name that will be displayed (human readable)
    	* levels: list of dimension levels (see: :class:`brewery.cubes.Level`)
    	* hierarchies: list of dimension hierarchies
    	* default_hierarchy_name: name of a hierarchy that will be used when no hierarchy is explicitly specified
    """
    
    def __init__(self, name, info = {}):
        """Create a new dimension
        
        Args:
            name (str): dimension name
            info (dict): dict object containing keys label, description, levels, hierarchies, default_hierarchy, key_field
        """
        self.name = name

        self.label = info.get("label", "")
        self.description = info.get("description", "")

        self.__init_levels(info.get("levels", None))
        self.__init_hierarchies(info.get("hierarchies", None))

        self.default_hierarchy_name = info.get("default_hierarchy", None)
        self.key_field = info.get("key_field")

    def __init_levels(self, info):
        self.levels = {}

        if info == None:
            return

        for level_name, level_info in info.items():
            level = Level(level_name, level_info)
            level.dimension = self
            self.levels[level_name] = level

    def __init_hierarchies(self, info):
        """booo bar"""
        self.hierarchies = {}

        if info == None:
            return

        for hier_name, hier_info in info.items():
            hier = Hierarchy(hier_name, hier_info)
            hier.dimension = self
            self.hierarchies[hier_name] = hier

    @property
    def default_hierarchy(self):
        """Get default hierarchy specified by ``default_hierarchy_name``, if the variable is not set then
        get a hierarchy with name *default*"""
        if self.default_hierarchy_name:
            return self.hierarchies[self.default_hierarchy_name]
        else:
            return self.hierarchies["default"]
