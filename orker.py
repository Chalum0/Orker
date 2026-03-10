from threading import Thread, current_thread, main_thread, Lock
from flask import Flask, jsonify, request
from werkzeug.serving import make_server
from importlib import import_module
from croniter import croniter
from pathlib import Path
import subprocess
import datetime
import argparse
import requests
import inspect
import json
import time
import sys


class Orker:
    def __init__(self, ui=False, write_config=True):
        self._internal_server = APIServer()
        self._ui_server = APIServer()
        self.ctx = Context()
        self.ctx.trigger_routine = self.trigger_routine
        self.src = None
        self.ui = ui
        self._restarting_internal_server = False
        self.write_config = write_config
        self._restart_internal_server_lock = Lock()
        self.crons = {}

    # ---------- INTERNAL WORK ----------
    def _start_ui_server(self, ui):
        if ui:
            def services_handler(payload):
                return self.get_methods_of_services()
            self._ui_server.make_endpoint("/api/services", "GET", services_handler)

            def restart_handler(payload):
                if not self._restart_internal_server_lock.acquire(blocking=False): return {"ok": False, "error": "restart in progress"}
                self._restarting_internal_server = True
                try:
                    self._restart_internal_server()
                    return {"ok": True}
                finally:
                    self._restarting_internal_server = False
                    self._restart_internal_server_lock.release()
            self._ui_server.make_endpoint("/api/restart", "GET", restart_handler)

            def trigger_handler(payload):
                data = request.get_json()
                if not data:
                    return jsonify({"error": "invalid json"}), 400

                required = ["routine_endpoint", "method", "payload"]
                for key in required:
                    if key not in data:
                        return jsonify({"error": f"missing field: {key}"}), 400

                routine_endpoint = data["routine_endpoint"]
                method = data["method"]
                payload = data["payload"]

                return jsonify({"ok": True, "result": self.trigger_routine(routine_endpoint, method, payload)}), 200
            self._ui_server.make_endpoint("/api/trigger", "POST", trigger_handler)

            self._ui_server.start(host="0.0.0.0", port=5051)
    def _start_internal_server(self):
        self._internal_server.start(host="127.0.0.1", port=5050)
        # Loop to prevent main thread from exiting causing some libraries to crash
        # And execute cron tasks
        if current_thread() is main_thread():
            try:
                self._start_ui_server(self.ui)
                while True:
                    for task in self.crons.values():
                        if task.should_execute():
                            self.trigger_routine(task.routine_endpoint, task.method, task.payload)
                    time.sleep(1)
            except KeyboardInterrupt:
                self._stop_servers()

    def _stop_ui_server(self):
        if self._ui_server is not None:
            self._ui_server.stop()
    def _stop_internal_server(self):
        if self._internal_server is not None:
            self._internal_server.stop()

    def _stop_servers(self):
        if self._internal_server.server is None and self._ui_server.server is None:
            return
        if self._internal_server is not None:
            self._internal_server.stop()
        if self._ui_server is not None:
            self._ui_server.stop()
    def _restart_internal_server(self):
        print("restarting server")
        self._stop_internal_server()
        self._internal_server = APIServer()
        self.load_json(self.src)

    def create_endpoints(self, endpoints):
        for endpoint in endpoints:
            self._internal_server.make_endpoint(endpoint.route, endpoint.method, endpoint.endpoint)

    def load_json(self, src):
        self.src = src
        data = self._read_json(src)
        self._save_to_history(data)
        # ui = data.get("ui", False)
        self._load_services(data.get("services", []))
        self._load_variables(data.get("variables", {}))
        self._load_endpoints(data.get("endpoints", []))
        self._load_crons(data.get("crons", []))
        # if ui:
        #     def services_handler(payload):
        #         return jsonify(self.get_methods_of_services())
        #     self._ui_server.make_endpoint("/api/services", "GET", services_handler)
        self._start_internal_server()

    @staticmethod
    def _read_json(src):
        src = Path(src)
        if not src.exists():
            raise FileNotFoundError(src)

        return json.loads(src.read_text(encoding="utf-8"))
    def _load_services(self, services):
        for service in services:
            svc_cls = self._import_attr(f"services.{service}", service, kind="service")
            setattr(self.ctx, f"service_{service}", svc_cls)
    def _load_variables(self, variables) -> None:
        for key, value in variables.items():
            setattr(self.ctx, f"v_{key}", value)
    def _load_endpoints(self, endpoints) -> None:
        blueprints = []
        for spec in endpoints:
            try:
                name = spec["name"]
                route = spec["route"]
                method = spec["method"]
                routine_type = spec["routine"]["type"]
                if routine_type == "python":
                    routine_name = spec["routine"]["name"]
                    routine_cls = self._import_attr(f"routines.{routine_name}", routine_name, kind="routine")
                elif routine_type == "json":
                    routine_json = spec["routine"]["json"]
                    routine_cls = JsonRoutine(routine_json)
                else:
                    raise Exception(f"Invalid routine type: {routine_type}")

                persistent_ctx = Context()
                make_routine_executor = lambda: RoutineExecutor(orchestrator=self, routine=routine_cls, persistent_ctx=persistent_ctx)
                blueprint = Endpoint(executor_class=make_routine_executor, name=name, route=route, method=method)
                blueprints.append(blueprint)
            except Exception as e:
                print(f"Cannot load Endpoint: {e}")

        self.create_endpoints(blueprints)
    def _load_crons(self, crons):
        for task in crons:
            try:
                name = task["name"]
                endpoint = task["endpoint"]
                method = task["method"]
                payload = task["payload"]
                schedule = task["schedule"]
                self.crons[name] = CronTask(endpoint, method, payload, schedule)
            except Exception as e:
                print(f"Cannot load cron: {e}")

    def get_methods_of_services(self):
        s = self._get_all_services()
        out = {}
        for service_name in s:
            out[service_name] = self._get_methods_of_service(service_name)
        return out
    def _get_methods_of_service(self, service_name):
        service = self.ctx.__getattribute__(service_name)
        out = {}
        for name, raw in service.__dict__.items():
            if name.startswith("_"):
                continue
            if isinstance(raw, (staticmethod, classmethod)):
                func = raw.__func__
            elif inspect.isfunction(raw):
                func = raw
            else:
                continue

            try:
                sig = inspect.signature(func)
            except (TypeError, ValueError):
                continue

            args = []
            for p in sig.parameters.values():
                if p.name in ("self", "cls"):
                    continue
                args.append(p.name)
            out[name] = args
        return out
    def _get_all_services(self):
        out = []
        for name, value in self.ctx.__dict__.items():
            if name.startswith("service_"):
                out.append(name)
        return out

    @staticmethod
    def _import_attr(module_path: str, attr: str, *, kind: str):
        module = import_module(module_path)
        try:
            return getattr(module, attr)
        except AttributeError:
            raise RuntimeError(f"{kind.capitalize()} '{attr}' not found in {module_path}") from None

    @staticmethod
    def _save_to_history(config, file_path="./config.history.json"):
        history = []

        if Path(file_path).exists():
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except (json.JSONDecodeError, ValueError):
                history = []

        history.append(config)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)

    def _write_config(self, endpoint, routine, routine_type):
        if self.write_config:
            data: dict = self._read_json(self.src)
            endpoints: dict = data.get("endpoints", {})
            endpoint: dict = next(ep for ep in endpoints if ep["name"] == endpoint)
            if routine_type == "python":
                endpoint["routine"] = {"type": "python", "name": routine}
            elif routine_type == "json":
                endpoint["routine"] = {"type": "json", "json": routine}

            # TODO Save json to the config file
            with open(self.src, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)

    @staticmethod
    def trigger_routine(routine_endpoint, method, payload=None):
        if payload is None:
            payload = {}
        if method == "POST":
            r = requests.post(
                f"http://127.0.0.1/{routine_endpoint}",
                json=payload
            )
        elif method == "GET":
            r = requests.get(
                f"http://127.0.0.1/{routine_endpoint}",
            )
        else:
            r = {"ok": False, "error": f"Unknown method {method}"}
        return r


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

    def set(self, key: str, value):
        setattr(self, key, value)
        return value

    def get(self, key: str):
        return getattr(self, key, None)

    def isset(self, key):
        return hasattr(self, key)

    def create_if_not_exist(self, key, default):
        if not self.isset(key):
            self.__setattr__(key, default)
        return self.__getattribute__(key)


class RoutineExecutor:
    def __init__(self, orchestrator: Orker, routine, persistent_ctx):
        self.ctx = Context()
        self.ctx.glob = orchestrator.ctx
        self.ctx.pers = persistent_ctx
        self.routine = routine(self.ctx)

    def run_routine(self):
        print(f"{datetime.datetime.now()} - Starting Routine")
        return self.routine.run(self.ctx)

    def change_routine(self, new_routine):
        self.routine = new_routine


class JsonRoutine:
    def __init__(self, j):
        self.instructions = j
        self.operations = {"set": self._set_op}

        self.vars = {}


    def run(self, ctx):
        self.vars = {}
        for inst in self.instructions:
            op_name = inst.get("op")
            fn = self.operations.get(op_name)
            if fn is None:
                return KeyError(f"Unknown op: {op_name}")

            params = dict(inst)
            params.pop("op", None)

            try:
                fn(ctx, **params)
            except Exception as e:
                return e
        return None

    def _set_op(self, ctx, scope, name, value, **_):
        # implement the action
        if isinstance(value, dict):
            value = self._calc(value)

        if scope == "global":
            ctx.glob.set(name, value)
        elif scope == "routine":
            ctx.set(name, value)
        elif scope == "runtime":
            self.vars[name] = value


    @staticmethod
    def _calc(value):
        return 1


class Endpoint:
    def __init__(self, executor_class, name, route, method):
        self.name = name
        self.route = route
        self.method = method
        self.executor = executor_class()
        self.handler = self.make_handler(self.executor)
        self.endpoint = self.make_endpoint(self.handler)

    def change_routine(self, new_routine):
        # JSON routine
        if isinstance(new_routine, dict):
            new_r = self._create_routine_object_from_json(new_routine)
        # if given a string -> try to load the corresponding module
        else:
            if type(new_routine) != str:
                return
            module = import_module(f"routines.{new_routine}")
            try:
                new_r = getattr(module, new_routine)
            except (AttributeError, TypeError):
                raise RuntimeError(f"{"routine".capitalize()} '{new_routine}' not found in {f"routines.{new_routine}"}") from None

        # Change config Json if allowed
        self.executor.change_routine(new_r)

    @staticmethod
    def _create_routine_object_from_json(j) -> JsonRoutine:
        return JsonRoutine(j)

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


class CronTask:
    def __init__(self, routine_endpoint, method, payload, schedule):
        self.routine_endpoint = routine_endpoint
        self.method = method
        self.payload = payload
        self.schedule = schedule
        self.iter = croniter(schedule, datetime.datetime.now())
        self.next_run = self.iter.get_next(datetime.datetime)

    def should_execute(self):
        now = datetime.datetime.now()

        if now >= self.next_run:
            self.next_run = self.iter.get_next(datetime.datetime)
            return True

        return False




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
    group.add_argument(
        "-ng",
        "--nogui",
        action="store_true",
        help="Run the orchestrator with no gui",
    )

    args = parser.parse_args()
    if args.install:
        OrchestratorUtils.install_dependencies()
        exit()

    if args.run:
        path = args.run
        orc = Orker(not args.nogui)
        orc.load_json(path)
        exit()

    else:
        print(f'Invalid args. Run "python orchestrator.py -h" for help.')
