from packages.Context import Context
from packages.JsonRoutine import JsonRoutine
from packages.LoaderUtils import import_attr, regex_check_str, file_hash
from pathlib import Path
import json


class RoutineLoader:
    def __init__(self, ctx: Context, hashes: dict):
        self.ctx = ctx
        self.hashes = hashes

    def load(self, config: dict):
        rtn = Context()
        for name in config.get("routines", {}).keys():
            routine = self._load_one(name)
            if routine is not None:
                rtn.__setattr__(name, routine)
            self.hashes[f"routines/{name}"] = self._hash(name)
        self.ctx.routines = rtn

    def check_reload(self, config: dict):
        if getattr(self.ctx, "routines", None) is None:
            return
        for name in config.get("routines", {}).keys():
            try:
                if not regex_check_str(name):
                    raise ValueError(f"Invalid routine name: {name}")
                current = self._hash(name)
                key = f"routines/{name}"
                if current == self.hashes.get(key):
                    continue
                print(f"Reloading routine {name}")
                routine = self._load_one(name)
                if routine is not None:
                    self.ctx.routines.__setattr__(name, routine)
                    self.hashes[key] = current
            except Exception as e:
                print(f"Unable to reload routine {name}: {e}")

    def _hash(self, name: str) -> str:
        py = f"routines/{name}.py"
        if Path(py).is_file():
            return str(file_hash(py))
        return str(file_hash(f"routines/{name}.json"))

    def _load_one(self, name: str):
        try:
            if not regex_check_str(name):
                raise ValueError(f'Invalid routine filename for "{name}"')
            py_path = f"routines/{name}.py"
            if Path(py_path).is_file():
                return import_attr(f"routines.{name}", name, kind="routine")
            json_path = f"routines/{name}.json"
            if Path(json_path).is_file():
                routine_json = json.loads(Path(json_path).read_text())
                return lambda ctx, j=routine_json: JsonRoutine(ctx, j)
            return None
        except RuntimeError as e:
            print(f"Unable to load routine {name}: {e}")
            return None
        except Exception as e:
            print(f"Could not load routine {name}: {e}")
            return None

    # ------------------------------------------------------------------
    # JSON routine file helpers (used by RoutineManager)
    # ------------------------------------------------------------------

    @staticmethod
    def is_json(name: str) -> bool:
        if Path(f"routines/{name}.py").is_file():
            return False
        return Path(f"routines/{name}.json").is_file()

    @staticmethod
    def read_json_content(name: str):
        path = Path(f"routines/{name}.json")
        if not path.is_file():
            return []
        return json.loads(path.read_text(encoding="utf-8"))

    @staticmethod
    def write_json_content(name: str, content):
        path = Path(f"routines/{name}.json")
        try:
            path.write_text(json.dumps(content, indent=4), encoding="utf-8")
        except OSError as e:
            print(f"Routine not saved: {e}")