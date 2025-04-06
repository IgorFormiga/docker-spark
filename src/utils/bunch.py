class Bunch(dict):

    def __getattribute__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(key)
        
    def __setattr__(self, key, value):
        self[key] = value

    def copy(self) -> "Bunch":
        return Bunch(dict.copy(self))