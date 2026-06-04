from importlib import import_module, invalidate_caches
from pathlib import Path
import hashlib
import sys
import re


class HashRepository:
    def __init__(self):
        self._routine_path = "routines"
        self._gateway_path = "gateways"
        self._service_path = "services"
        self._trigger_path = "triggers"
        self._hashes = {}

    @staticmethod
    def file_hash(path: str, algo="sha256"):
        try:
            h = hashlib.new(algo)

            with open(path, "rb") as f:
                for chunk in iter(lambda: f.read(1024 * 1024), b""):
                    h.update(chunk)

            return str(h.hexdigest())
        except FileNotFoundError:
            return "0"

    def save_hash(self, key, h):
        self._hashes[key] = h

    def compute_element_hash(self, key):
        """Saves or update the hash for a specific file and return said hash."""
        path: str|None = self.get_path(key)
        if path is None:
            return "0"
        else:
            h = self.file_hash(path)
            return h

    def compute_components_hash(self, routines=None):
        """Returns the hash for a specific component"""
        routines = routines or []
        parts = [self.get_hash(self.get_path(routine)) for routine in routines]
        h = "|".join(parts)
        return h

    def compute_complete_hash(self, key, routines=None):
        """Returns the complete hash for a specific component"""
        routines = routines or []
        parts = [self.compute_element_hash(key)] + [self.compute_components_hash(routines)]
        h = "|".join(parts)
        return h

    def get_hash(self, key):
        """Returns the saved hash for a specific file."""
        if key in self._hashes:
            return self._hashes[key]
        else:
            return "0"

    def compare_file_hash(self, key):
        """Returns True if the hash is the same as the old one."""
        p: str|None = self.get_path(key)
        if p is None or key not in self._hashes:
            return False

        current_hash = self.file_hash(p)
        return current_hash == self.get_hash(key)

    def update_all_hashes(self):
        for h in self._hashes.keys():
            self._hashes[h] = self.get_hash(h)


    @staticmethod
    def get_path(key):
        py_path = f"{key}.py"
        py_exists = Path(py_path).is_file()
        if py_exists:
            return f"{key}.py"
        json_path = f"{key}.json"
        json_exists = Path(json_path).is_file()
        if json_exists:
            return f"{key}.json"
        return None

    @staticmethod
    def regex_check_str(string):
        return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", string))

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

    def check_file_exists(self, key):
        p = self.get_path(key)
        return p is not None

    def get_routine_hash_key(self, routine_name: str):
        """Returns the hash key for a routine"""
        if not routine_name.startswith(self._routine_path + "/"):
            return f"{self._routine_path}/{routine_name}"
        else:
            return routine_name

    def get_gateway_hash_key(self, gateway_name):
        """Returns the hash key for a gateway"""
        if not gateway_name.startswith(self._gateway_path + "/"):
            return f"{self._gateway_path}/{gateway_name}"
        else:
            return gateway_name

    def get_service_hash_key(self, service_name):
        """Returns the hash key for a service"""
        if not service_name.startswith(self._service_path + "/"):
            return f"{self._service_path}/{service_name}"
        else:
            return service_name

    def get_trigger_hash_key(self, trigger_name):
        if not trigger_name.startswith(self._trigger_path + "/"):
            return f"{self._trigger_path}/{trigger_name}"
        else:
            return trigger_name
