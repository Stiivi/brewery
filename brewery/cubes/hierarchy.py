class Hierarchy(object):
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
                level = self.dimension.levels[level_name]
                self.levels.append(level)