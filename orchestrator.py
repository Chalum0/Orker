from flask import Flask, jsonify, request
from werkzeug.serving import make_server
from importlib import import_module
from dataclasses import dataclass
from threading import Thread
from pathlib import Path
import subprocess
import datetime
import argparse
import json
import time
import sys


class Orchestrator:
    def __init__(self):
        self._server = APIServer()
        self.ctx = Context()

    # ---------- INTERNAL WORK ----------
    def start_server(self):
        self._server.start(host="127.0.0.1", port=5050)
        # Loop to prevent main thread from exiting causing some libraries to crash
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_server()
    def stop_server(self):
        if self._server.server is None:
            return
        self._server.stop()

    def create_endpoints(self, endpoints):
        for endpoint in endpoints:
            self._server.make_endpoint(endpoint.route, endpoint.method, endpoint.endpoint)

    def load_json(self, src):
        data = self._read_json(src)
        self._load_services(data.get("services", []))
        self._load_variables(data.get("variables", {}))
        self._load_endpoints(data.get("endpoints", []))
        self.start_server()

    @staticmethod
    def _read_json(src):
        src = Path(src)
        if not src.exists():
            raise FileNotFoundError(src)

        return json.loads(src.read_text(encoding="utf-8"))


    def _load_services(self, services):
        for spec in services:
            name = spec["name"]
            svc_cls = self._import_attr(f"services.{name}", name, kind="service")
            setattr(self.ctx, f"service_{name}", svc_cls)

    def _load_variables(self, variables) -> None:
        for key, value in variables.items():
            setattr(self.ctx, f"v_{key}", value)


    def _load_endpoints(self, endpoints) -> None:
        blueprints = []
        for spec in endpoints:
            name = spec["name"]
            routine_name = spec["routine"]["name"]

            endpoint_cls = self._import_attr(f"endpoints.{name}", name, kind="endpoint")
            routine_cls = self._import_attr(f"routines.{routine_name}", routine_name, kind="routine")

            persistent_ctx = Context()
            make_routine_executor = lambda: RoutineExecutor(orchestrator=self, routine=routine_cls, persistent_ctx=persistent_ctx)
            blueprint = lambda **kwargs: EndpointBlueprint(executor_class=make_routine_executor, **kwargs)
            # make_executor = lambda: RoutineExecutor(self, routine_cls, persistent_ctx)  # noqa: E731
            # blueprint = EndpointBlueprint(executor_class=make_executor)
            blueprints.append(endpoint_cls(blueprint).endpoint)

        self.create_endpoints(blueprints)



    @staticmethod
    def _import_attr(module_path: str, attr: str, *, kind: str):
        module = import_module(module_path)
        try:
            return getattr(module, attr)
        except AttributeError:
            raise RuntimeError(f"{kind.capitalize()} '{attr}' not found in {module_path}") from None

class APIServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.server = None
        self.thread = None

        @self.app.route("/ping")
        def ping():
            return jsonify({"status": "ok"})

    def make_endpoint(self, route, method, handler):
        def endpoint():
            payload = request.get_json(silent=True) or {}
            result = handler(payload)
            if isinstance(result, dict):
                return jsonify(result)
            return result
        endpoint.__name__ = f"view_{handler.__name__}_{route.strip('/').replace('/', '_')}"

        self.app.add_url_rule(
            route,
            endpoint=endpoint.__name__,
            view_func=endpoint,
            methods=[method],
        )

        return endpoint

    def start(self, host="0.0.0.0", port=5000):
        """Boot the HTTP server in a background thread."""
        if self.server is not None:
            # already running
            return

        self.server = make_server(host, port, self.app)
        self.thread = Thread(target=self.server.serve_forever)  # , daemon=True)
        self.thread.start()
        print(f"API started on http://{host}:{port}")

    def stop(self):
        """Gracefully stop the HTTP server."""
        if self.server is None:
            return

        self.server.shutdown()
        self.thread.join()
        self.server = None
        self.thread = None
        print("API stopped")


class Context:
    def __init__(self):
        pass

    def __str__(self):
        return f"{self.__class__.__name__}({self.__dict__})"

    def isset(self, key):
        return hasattr(self, key)

    def create_if_not_exist(self, key, default):
        if not self.isset(key):
            self.__setattr__(key, default)
        return self.__getattribute__(key)


@dataclass
class Utils:
    pass

class RoutineExecutor:
    def __init__(self, orchestrator: Orchestrator, routine, persistent_ctx):
        self.ctx = Context()
        self.ctx.glob = orchestrator.ctx
        self.ctx.pers = persistent_ctx
        self.routine = routine(self.ctx)

    def run_routine(self):
        print(f"{datetime.datetime.now()} - Starting Routine")
        return self.routine.run(self.ctx)


class EndpointBlueprint:
    def __init__(self, executor_class, name, endpoint_type, route, method):
        self.name = name
        self.endpointType = endpoint_type
        self.route = route
        self.method = method
        self.executor = executor_class()
        self.handler = self.make_handler(self.executor)
        self.endpoint = self.make_endpoint(self.handler)

    @staticmethod
    def make_handler(executor):
        def handler(payload):
            # Create a new instance of the executor class that
            # already has a clean context plus the global context
            # thanks to the factory in the import of the endpoint.

            # A new instance is created for each time the endpoint is called.
            # Each execution in isolated
            # Dump payload in context
            for k, v in payload.items():
                setattr(executor.ctx, f"payload_{k}", v)

            print(f" {datetime.datetime.now()} - got request")

            return executor.run_routine()
        return handler

    @staticmethod
    def make_endpoint(callback):
        def endpoint(payload):
            if callback:
                response = callback(payload)
                if response:
                    return jsonify({"content": response})
                return jsonify({"content": None})
            return jsonify({"content": None})

        return endpoint


class OrchestratorUtils:

    @staticmethod
    def install_dependencies():
        try:
            OrchestratorUtils._install_folder("./endpoints")
            OrchestratorUtils._install_folder("./services")
            OrchestratorUtils._install_folder("./routines")
        except Exception as e:
            print(f"Error installing dependencies: {e}")

    @staticmethod
    def _install_folder(folder: str) -> bool:
        if not Path(folder).exists(): return True
        for file in Path(folder).iterdir():
            if file.is_file():
                with file.open("r", encoding="utf-8") as f:
                    first_line = f.readline().strip()
                    if first_line.startswith("#") and "pip install" in first_line:
                        command = first_line.replace("#", "").strip()
                        if command.startswith("pip "):
                            command = f'"{sys.executable}" -m {command}'
                            subprocess.run(command, shell=True, check=True)
        return True

# orc = Orchestrator()
# orc.load_json("./config.json")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        "-i",
        "--install",
        action="store_true",
        help="Installs dependencies for all the endpoints, services and routines",
    )
    group.add_argument(
        "-r",
        "--run",
        metavar="LINK",
        type=str,
        help="Path of the json blueprint",
    )

    args = parser.parse_args()
    if args.install:
        OrchestratorUtils.install_dependencies()
        exit()

    elif args.run:
        path = args.run
        orc = Orchestrator()
        orc.load_json(path)
        exit()

    else:
        print(f'Invalid args. Run "python orchestrator.py -h" for help.')
