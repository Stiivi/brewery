"""Brewery handy utilities"""

class IgnoringDictionary(dict):
    """Simple dictionary extension that will ignore any keys of which values are empty (None/False)"""
    def setnoempty(self, key, value):
        """Set value in a dictionary if value is not null"""
        if value:
            self[key] = value
