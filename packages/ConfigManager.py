from importlib import import_module, invalidate_caches
from packages.Context import Context
from pathlib import Path
import hashlib
import json
import sys
import re

class ConfigManager:
    def __init__(self, config):
        self.ctx = Context()
        self.working_config = {}
        self.config_path = config
        self.hashes = {}



    def load_config(self):
        config = self._read_json(self.config_path)
        self.working_config = config

        self._load_gateways(config)



    def _read_json(self, src):
        src = Path(src)
        if not src.exists():
            raise FileNotFoundError(src)
        try:
            return json.loads(src.read_text(encoding="utf-8"))
        except json.JSONDecodeError as e:
            return self.working_config

    @staticmethod
    def _import_attr(module_path: str, attr: str, *, kind: str):
        if module_path in sys.modules:  # Prevents cached state. For hot reload
            del sys.modules[module_path]
        invalidate_caches()
        module = import_module(module_path)

        try:
            return getattr(module, attr)
        except AttributeError:
            raise RuntimeError(f"{kind.capitalize()} '{attr}' not found in {module_path}") from None
    @staticmethod
    def file_hash(path, algo="sha256"):
        try:
            h = hashlib.new(algo)

            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)

            return h.hexdigest()
        except FileNotFoundError:
            return 0


    def _check_reloads(self):
        for path, old_hash in self.hashes.items():
            pass


    def _get_gateway_hash(self, name):
        path = f"gateways/{name}.py"
        return str(self.file_hash(path))
    def _load_gateways(self, config):
        gtw = Context()
        for name, params in config.get("gateways", {}).items():
            gateway = self._load_gateway(name, params)
            if gateway is not None:
                gtw.__setattr__(name, gateway)
            # self.hashes[f"gateways/{name}.py"] = self.file_hash(f"gateways/{name}.py")
            self.hashes[f"gateways/{name}.py"] = self._get_gateway_hash(name)
        self.ctx.gateways = gtw
    def _load_gateway(self, name, params):
        if not isinstance(name, str):
            return None
        path = f"gateways/{name}.py"
        try:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", name):
                raise ValueError(f'Invalid gateway name: "{name}"')

            if not isinstance(params, dict):
                raise TypeError(f"Gateway {name} does not have valid params.")

            if Path(path).is_file():
                try:
                    gateway_class = self._import_attr(f"gateways.{name}", name, kind="gateway")
                    return gateway_class(**params)
                except RuntimeError as e:
                    print(f"Unable to load gateway {name} because the file does not contain a '{name}' class. ({e})")
                    return None
                except Exception as e:
                    raise e
            else:
                raise FileNotFoundError(f"Gateway file not found: {path}")
        except Exception as e:
            print(f"Could not create Gateway {name}: {e}")
            return None


    def _get_service_hash(self, name):
        path = f"services/{name}.py"
        return str(self.file_hash(path))
    def _load_services(self, config):
        srv = Context()
        for s in config.get("services", []):
            service = self._load_service(s)
            if service is not None:
                srv.__setattr__(s, service)
            self.hashes[f"services/{s}.py"] = self._get_service_hash(s)
        self.ctx.services = srv
    def _load_service(self, name):
        path = f"services/{name}.py"
        try:
            if Path(path).is_file():
                try:
                    service_class = self._import_attr(f"services.{name}", name, kind="service")
                    return service_class
                except RuntimeError as e:
                    print(f"Unable to load service {name} because the file does not contain a '{name}' class. ({e})")
                    return None
        except Exception as e:
            print(f"Could not create service {name}: {e}")
            return None





    def _get_trigger_hash(self, name, routines):
        path = f"triggers/{name}.py"
        routine_hash = ""
        for routine in routines:
            routine_path = f"routines/{routine}.py"
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", routine_path):
                raise ValueError(f'Invalid trigger name: "{routine}"')
            routine_hash += str(self.file_hash(routine_path))
        return str(self.file_hash(path)) + routine_hash
