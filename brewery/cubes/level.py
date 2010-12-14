class Level(object):
    """Hierarchy level

    Attributes:
        name: level name
        dimension: dimension the hierarhy belongs to
        label: human readable label
        key: key attribute for level
        attributes: list of level attributes
    """
    def __init__(self, name, info, dimension = None):
        self.name = name
        self.label = info.get("label", "")
        self.key = info.get("key", None)
        self.attributes = info.get("attributes", [])
        self.dimension = dimension
