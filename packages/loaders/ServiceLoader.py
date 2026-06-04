from packages.Context import Context
from packages.LoaderUtils import import_attr, regex_check_str, file_hash
from pathlib import Path


class ServiceLoader:
    def __init__(self, ctx: Context, hashes: dict):
        self.ctx = ctx
        self.hashes = hashes

    def load(self, config: dict):
        srv = Context()
        for name in config.get("services", []):
            svc = self._load_one(name)
            if svc is not None:
                srv.__setattr__(name, svc)
            self.hashes[f"services/{name}"] = self._hash(name)
        self.ctx.services = srv

    def check_reload(self, config: dict):
        if getattr(self.ctx, "services", None) is None:
            return
        for name in config.get("services", []):
            try:
                if not regex_check_str(name):
                    raise ValueError(f'Invalid service name: "{name}"')
                current = self._hash(name)
                key = f"services/{name}"
                if current == self.hashes.get(key):
                    continue
                print(f"Reloading service {name}")
                svc = self._load_one(name)
                if svc is not None:
                    self.ctx.services.__setattr__(name, svc)
                    self.hashes[key] = current
            except Exception as e:
                print(f"Could not reload service {name}: {e}")

    def _hash(self, name: str) -> str:
        return str(file_hash(f"services/{name}.py"))

    def _load_one(self, name: str):
        try:
            if not regex_check_str(name):
                raise ValueError(f'Invalid service filename for "{name}"')
            path = f"services/{name}.py"
            if not Path(path).is_file():
                return None
            cls = import_attr(f"services.{name}", name, kind="service")
            return cls
        except RuntimeError as e:
            print(f"Unable to load service {name}: file does not contain a '{name}' class. ({e})")
            return None
        except Exception as e:
            print(f"Could not create service {name}: {e}")
            return None