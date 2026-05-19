class Context:
    def __init__(self, variables: None|dict = None, freeze=False):
        if variables is not None:
            self.dump(variables)
        self._read_only = freeze

    def __str__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def __setattr__(self, name, value):
        if getattr(self, "_read_only", False) and name != "_read_only":
            raise AttributeError("Instance is read-only")
        object.__setattr__(self, name, value)

    def get_json(self):
        vars = {}
        for k, v in self.__dict__.items():
            if not k.startswith("_"):
                vars[k] = v
        return vars

    def freeze(self):
        self._read_only = True

    def dump(self, variables: dict):
        if variables and isinstance(variables, dict):
            for k, v in variables.items():
                setattr(self, k, v)