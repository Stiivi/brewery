import brewery.utils

class Hierarchy(object):
    """Dimension hierarchy
    
    Attributes:
        name: hierarchy name
        label: human readable name
        levels: ordered list of levels from dimension
    """

    def __init__(self, name, info = {}, dimension = None):
        self.name = name
        self._dimension = None
        self.label = info.get("label", "")
        self.level_names = info.get("levels", [])
        self.dimension = dimension

    @property
    def dimension(self):
        return self._dimension
        
    @dimension.setter
    def dimension(self, a_dimension):
        self._dimension = a_dimension
        self.levels = []
        if a_dimension != None:
            for level_name in self.level_names:
                level = self.dimension.levels.get(level_name)

                if not level:
                    raise KeyError("No level %s in dimension %s" % (level_name, a_dimension.name))
                    
                self.levels.append(level)

    def to_dict(self):
        """Convert to dictionary"""

        out = brewery.utils.IgnoringDictionary()
        out.setnoempty("name", self.name)
        out.setnoempty("label", self.label)
        out.setnoempty("levels", self.level_names)

        return out
