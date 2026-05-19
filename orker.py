import sys
sys.dont_write_bytecode = True


from importlib import import_module, invalidate_caches
from threading import current_thread, main_thread
from packages import APIServer, Context
from pathlib import Path
import hashlib
import json
import time


class Orker:
    def __init__(self, config="config.json", hot_reload=True):
        self.host = "127.0.0.1"
        self.port = 5000
        self.server = None
        self.config_path = config
        self.config = {}
        self.context = Context.Context()
        self.should_restart = False
        self.hashes = {}
        self.hot_reload = hot_reload

    # ---------- SERVER HANDLING ----------

    def start(self):
        print(f"Starting server on http://{self.host}:{self.port}.")
        while True:
            try:
                self._start_server()
            except KeyboardInterrupt:
                break

    def _start_server(self):
        if type(self.server) != APIServer.APIServer :
            self.server = APIServer.APIServer()
        self.load_json(self.config_path)
        self.server.start(host=self.host, port=self.port)
        if current_thread() is main_thread():
            try:
                while True:
                    if self.hot_reload:
                        self.check_hashes()
                    if self.should_restart:
                        self._stop_server()
                        self.should_restart = False
                        print("Reloading")
                        return
                    time.sleep(1)
            except KeyboardInterrupt:
                self._stop_server()
                # exit()
                raise KeyboardInterrupt

    def _stop_server(self):
        if type(self.server) == APIServer.APIServer:
            self.server.stop()
            self.server = None

    # ---------- JSON CONFIG ----------
    def load_json(self, src):
        self.hashes = {}
        config = self._read_json(src)
        self.config = config

        self._load_services(config.get("services", []))
        self._load_variables(config.get("variables", {}))
        self._load_routines(config.get("routines", []))
        self._load_endpoints(config.get("endpoints", []))

        self.hashes[src] = self.file_hash(src)

    def _read_json(self, src):
        src = Path(src)
        if not src.exists():
            raise FileNotFoundError(src)
        try:
            return json.loads(src.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return self.config

    def _load_services(self, services):
        s = Context.Context()
        for service in services:
            self.hashes[f"services/{service}.py"] = self.file_hash(f"services/{service}.py")
            svc_cls = self._import_attr(f"services.{service}", service, kind="service")
            s.__setattr__(service, svc_cls)
        self.context.__setattr__("services", s)
    def _load_variables(self, variables) -> None:
        v = Context.Context()
        for key, value in variables.items():
            v.__setattr__(key, value)
        self.context.__setattr__("variables", v)
    def _load_routines(self, routines):
        r = Context.Context()
        for routine in routines:
            self.hashes[f"routines/{routine}.py"] = self.file_hash(f"routines/{routine}.py")
            if Path(f"routines/{routine}.py").exists():
                try:
                    rtn_cls = self._import_attr(f"routines.{routine}", routine, kind="routine")
                    r.__setattr__(routine, rtn_cls)
                except RuntimeError as e:
                    print(f"Unable to load the routine '{routine}' because the file does not contain a '{routine}' class. ({e})")
        self.context.__setattr__("routines", r)
    def _load_endpoints(self, endpoints) -> None:
        for spec in endpoints:
            try:
                route = spec["route"]
                method = spec["method"]
                routine = spec["routine"]
                try:
                    if isinstance(routine, str):
                        r = self.context.routines.__getattribute__(routine)
                    else:
                        raise Exception("Invalid routine type (must be str or json).")
                except AttributeError:
                    raise Exception(f"Routine '{routine}' for endpoint {route} undefined.")


                if not isinstance(self.server, APIServer.APIServer):
                    raise Exception(f"Inernal Error: Endpoint loaded before server instance. Report this issue.")

                self.server.make_endpoint(route, method, r(self.context).run)

            except Exception as e:
                print(f"Could not create Endpoint: {e}")


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

    def check_hashes(self):
        for path, old_hash in self.hashes.items():
            if self.file_hash(path) != old_hash:
                self.should_restart = True



orker = Orker()
orker.start()
