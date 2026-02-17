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

    def load_json(self, j):
        if type(j) != dict:
            if type(j) == str:
                json_path = Path(j)
                if not json_path.exists():
                    raise FileNotFoundError(f"File '{json_path}' not found")
                j = json.loads(json_path.read_text(encoding="utf-8"))
            else:
                raise ValueError("Invalid JSON provided")

        services = {}
        for s in j["services"]:
            name = s["name"]
            module = import_module(f"services.{name}")
            service = getattr(module, name, None)
            if service is None:
                raise RuntimeError(f"Routine '{name}' not found")
            services[name] = service

        for k, v in j["variables"].items():
            setattr(self.ctx, f"v_{k}", v)


        # self.ctx.services = services
        for k, v in services.items():
            setattr(self.ctx, f"service_{k}", v)

        print(self.ctx)

        endpoints = []
        for e in j["endpoints"]:
            name = e["name"]
            routine_name = e["routine"]["name"]
            module = import_module(f"endpoints.{name}")
            endpoint = getattr(module, name, None)
            if endpoint is None:
                raise RuntimeError(f"Endpoint '{name}' not found")
            routine_module = import_module(f"routines.{routine_name}")
            routine = getattr(routine_module, routine_name, None)
            if routine is None:
                raise RuntimeError(f"Routine '{routine_name}' not found")
            persistent_ctx = Context()
            make_routine_executor = lambda: RoutineExecutor(orchestrator=self, routine=routine, persistent_ctx=persistent_ctx)
            blueprint = lambda **kwargs: EndpointBlueprint(executor_class=make_routine_executor, **kwargs)
            endpoint = endpoint(blueprint).endpoint
            endpoints.append(endpoint)
        self.create_endpoints(endpoints)

        self.start_server()


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
        self.handler = self.make_handler(executor_class)
        self.endpoint = self.make_endpoint(self.handler)

    @staticmethod
    def make_handler(executor_class):
        def handler(payload):
            # Create a new instance of the executor class that
            # already has a clean context plus the global context
            # thanks to the factory in the import of the endpoint.

            # A new instance is created for each time the endpoint is called.
            # Each execution in isolated
            executor = executor_class()
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
