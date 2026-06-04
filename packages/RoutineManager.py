from packages.loaders.RoutineLoader import RoutineLoader
from packages.LoaderUtils import make_file, delete_file


class RoutineManager:
    """Handles the create / delete / read / write lifecycle for JSON routines."""

    def __init__(self, working_config: dict, save_config_fn):
        self._config = working_config
        self._save = save_config_fn

    def list(self) -> dict:
        routines = {"routines": dict(self._config.get("routines", {}))}
        for key in routines["routines"]:
            routines["routines"][key]["json"] = RoutineLoader.is_json(key)
        return routines

    def create(self, name: str, alias: str, description: str) -> bool:
        if name in self._config.get("routines", {}):
            return False
        routines = self._config.setdefault("routines", {})
        routines[name] = {"name": alias, "desc": description}
        make_file(f"routines/{name}.json")
        self._save()
        RoutineLoader.write_json_content(name, [])
        return True

    def delete(self, name: str) -> bool:
        routines = self._config.get("routines", {})
        if name not in routines:
            return False
        del routines[name]
        delete_file(f"routines/{name}.json")
        self._save()
        return True

    def get_content(self, name: str):
        if name not in self._config.get("routines", {}):
            return []
        return RoutineLoader.read_json_content(name)

    def set_content(self, name: str, content) -> bool:
        if name not in self._config.get("routines", {}):
            return False
        RoutineLoader.write_json_content(name, content)
        return True