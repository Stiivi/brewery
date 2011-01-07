"""Cube dimension"""

from hierarchy import *
from level import *
import brewery.utils

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
    
    def __init__(self, name, desc = {}):
        """Create a new dimension
        
        Args:
            * name (str): dimension name
            * desc (dict): dict object containing keys label, description, levels, hierarchies, default_hierarchy, key_field
        """
        self.name = name

        self.label = desc.get("label", "")
        self.description = desc.get("description", "")

        self.__init_levels(desc.get("levels", None))
        self.__init_hierarchies(desc.get("hierarchies", None))
        self._flat_hierarchy = None

        self.default_hierarchy_name = desc.get("default_hierarchy", None)
        self.key_field = desc.get("key_field")

    def __init_levels(self, desc):
        self.levels = {}

        if desc == None:
            return

        for level_name, level_info in desc.items():
            level = Level(level_name, level_info)
            level.dimension = self
            self.levels[level_name] = level

    def __init_hierarchies(self, desc):
        """booo bar"""
        self.hierarchies = {}

        if desc == None:
            return

        for hier_name, hier_info in desc.items():
            hier = Hierarchy(hier_name, hier_info)
            hier.dimension = self
            self.hierarchies[hier_name] = hier
            
    def _initialize_default_flat_hierarchy(self):
        if not self._flat_hierarchy:
            self._flat_hierarchy = self.flat_hierarchy(self.levels.values()[0])
        
    @property
    def default_hierarchy(self):
        """Get default hierarchy specified by ``default_hierarchy_name``, if the variable is not set then
        get a hierarchy with name *default*"""
        if self.default_hierarchy_name:
            hierarchy_name = self.default_hierarchy_name
        else:
            hierarchy_name = "default"

        hierarchy = self.hierarchies.get(hierarchy_name)

        if not hierarchy:
            if len(self.hierarchies) == 1:
                hierarchy = self.hierarchies.values()[0]
            else:
                if not self.hierarchies:
                    if len(self.levels) == 1:
                        self._initialize_default_flat_hierarchy()
                        return self._flat_hierarchy
                    elif len(self.levels) > 1:
                        raise KeyError("There are no hierarchies in dimenson %s "
                                       "and there are more than one level" % self.name)
                    else:
                        raise KeyError("There are no hierarchies in dimenson %s "
                                       "and there are no levels to make hierarchy from" % self.name)
                else:
                    raise KeyError("No default hierarchy specified in dimension '%s' " \
                                   "and there is more (%d) than one hierarchy defined" \
                                   % (self.name, len(self.hierarchies)))

        return hierarchy
    
    def flat_hierarchy(self, level):
        # if len(levels) > 0:
        #     raise AttributeError("Could not create default flat hierarchy in dimension '%s' if there "
        #                          "are more than one level" % self.name)
        hier = Hierarchy(level.name)
        hier.level_names = [level.name]
        hier.dimension = self
        return hier

    def all_attributes(self, hierarchy = None):
        if not hierarchy:
            hier = self.default_hierarchy
        elif type(hierarchy) == str:
            hier = self.hierarchies[hierarchy]
        else:
            hier = hierarchy
        
        attributes = []
        for level in hier.levels:
            attributes.extend(level.attributes)

        return attributes
        
    def to_dict(self):
        out = brewery.utils.IgnoringDictionary()
        out.setnoempty("name", self.name)
        out.setnoempty("label", self.label)
        out.setnoempty("default_hierarchy_name", self.default_hierarchy_name)

        levels_dict = {}
        for level in self.levels.values():
            levels_dict[level.name] = level.to_dict()
        out["levels"] = levels_dict

        hier_dict = {}
        for hier in self.hierarchies.values():
            hier_dict[hier.name] = hier.to_dict()
        out["hierarchies"] = hier_dict
            

    	# * levels: list of dimension levels (see: :class:`brewery.cubes.Level`)
    	# * hierarchies: list of dimension hierarchies

        return out
        
    def validate(self):
        """Validate dimension. See Model.validate() for more information. """
        results = []

        if not self.levels:
            results.append( ('error', "No levels in dimension '%s'" % self.name) )

        if not self.hierarchies:
            results.append( ('error', "No hierarchies in dimension '%s'" % self.name) )
        else:
            if not self.default_hierarchy_name:
                if len(self.hierarchies) > 1 and not "default" in self.hierarchies:
                    results.append( ('error', "No defaut hierarchy specified, there is "\
                                              "more than one hierarchy in dimension '%s'" % self.name) )
                else:
                    def_name = self.default_hierarchy.name
                    results.append( ('warning', "No default hierarchy name specified in dimension '%s', using "
                                                "'%s'"% (self.name, def_name)) )

        if self.default_hierarchy_name and not self.hierarchies.get(self.default_hierarchy_name):
            results.append( ('warning', "Default hierarchy '%s' does not exist in dimension '%s'" % 
                            (self.default_hierarchy_name, self.name)) )

        for level_name, level in self.levels.items():
            if not level.attributes:
                results.append( ('error', "Level '%s' in dimension '%s' has no attributes" % (level.name, self.name)) )
            else:
                if not level.key:
                    attr = level.attributes[0]
                    results.append( ('warning', "Level '%s' in dimension '%s' has no key attribute specified, "\
                                                "first attribute will be used: '%s'" 
                                                % (level.name, self.name, attr)) )

            if level.attributes and level.key and level.key not in level.attributes:
                results.append( ('error', "Key '%s' in level '%s' in dimension '%s' in not in attribute list"
                                                % (level.key, level.name, self.name)) )

        return results