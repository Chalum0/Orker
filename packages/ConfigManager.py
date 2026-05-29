from importlib import import_module, invalidate_caches
from packages.TriggerManager import TriggerManager
from packages.Context import Context
from pathlib import Path
import hashlib
import json
import sys
import re

class ConfigManager:
    def __init__(self, config_path):
        self.ctx = Context()
        self.working_config = {}
        self.config_path = config_path
        self.hashes = {}
        self.ctx.should_restart = False



    def load_config(self, server):
        config = self._read_json(self.config_path)
        self.working_config = config

        self._load_routines(config)
        self._load_services(config)
        self._load_gateways(config)
        self._load_triggers(config)
        self._load_variables(config)
        self._load_endpoints(server, config)

    def check_hot_reload(self, server):
        config = self._read_json(self.config_path)
        self.working_config = config

        self._check_endpoint_reload(server, config)
        self._check_routine_reload(config)
        self._check_service_reload(config)
        self._check_gateway_reload(config)
        self._check_trigger_reload(config)
        self._load_variables(config)



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
    @staticmethod
    def _regex_check_str(string):
        return bool(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", string))


    def _check_reloads(self):
        for path, old_hash in self.hashes.items():
            pass


    def _get_gateway_hash(self, name):
        path = f"gateways/{name}.py"
        return str(self.file_hash(path))
    def _check_gateway_reload(self, config):
        if getattr(self.ctx, "gateways", None) is None:
            return
        for g in config.get("gateways", []):
            alias = g["alias"]
            gate = g["gateway"]
            try:
                if self._regex_check_str(alias) and self._regex_check_str(gate):
                    current_hash = self._get_gateway_hash(gate)
                    hash_key = f"gateways/{alias}"
                    if current_hash == self.hashes.get(hash_key):
                        continue
                    print(f"Reloading gateway {alias}")
                    gateway = self._load_gateway(**g)
                    if gateway is not None:
                        self.ctx.gateways.__setattr__(alias, gateway)
                        self.hashes[hash_key] = current_hash
                else:
                    raise ValueError(f'Invalid gateway alias or filename: "{alias}" / "{gate}"')
            except Exception as e:
                print(f"Unable to reload gateway {alias}: {e}")
                continue
    def _load_gateways(self, config):
        gtw = Context()
        for g in config.get("gateways", []):
            alias = g["alias"]
            gate = g["gateway"]
            try:
                gateway = self._load_gateway(**g)
                if gateway is not None:
                    gtw.__setattr__(alias, gateway)
                    self.hashes[f"gateways/{alias}"] = self._get_gateway_hash(gate)
            except Exception as e:
                print(f"Unable to load gateway {alias}: {e}")
        self.ctx.gateways = gtw
    def _load_gateway(self, alias, gateway, params):
        try:
            if not self._regex_check_str(alias) or not self._regex_check_str(gateway):
                raise ValueError(f'Invalid gateway filename or alias for "{alias}"')
            if not isinstance(params, dict):
                raise TypeError(f"Gateway {alias} does not have valid params.")
            path = f"gateways/{gateway}.py"
            if Path(path).is_file():
                try:
                    gateway_class = self._import_attr(f"gateways.{gateway}", gateway, kind="gateway")
                    return gateway_class(**params)
                except RuntimeError as e:
                    print(f"Unable to load gateway {alias} because the file does not contain a '{gateway}' class. ({e})")
                    return None
                except Exception as e:
                    raise e
            else:
                raise FileNotFoundError(f"Gateway file not found: {path}")

        except Exception as e:
            print(f"Could not load gateway {alias}: {e}")
            return None

    def _load_variables(self, config):
        v = Context()
        variables = config.get("variables", {})
        for key, value in variables.items():
            v.__setattr__(key, value)
        self.ctx.variables = v

    def _get_routine_hash(self, name: str):
        path = f"routines/{name}.py"
        return str(self.file_hash(path))
    def _check_routine_reload(self, config):
        if getattr(self.ctx, "routines", None) is None:
            return
        for r in config.get("routines", []):
            try:
                if not self._regex_check_str(r):
                    raise ValueError(f"Invalid routine name: {r}")
                current_hash = self._get_routine_hash(r)
                hash_key = f"routines/{r}"
                if current_hash == self.hashes.get(hash_key):
                    continue
                print(f"Reloading routine {r}")
                routine = self._load_routine(r)
                if routine is not None:
                    self.ctx.routines.__setattr__(r, routine)
                    self.hashes[hash_key] = current_hash
            except Exception as e:
                print(f"Unable to reload routine {r}: {e}")
                continue
    def _load_routines(self, config):
        rtn = Context()
        for s in config.get("routines", []):
            routine = self._load_routine(s)
            if routine is not None:
                rtn.__setattr__(s, routine)
            self.hashes[f"routines/{s}"] = self._get_routine_hash(s)
        self.ctx.routines = rtn
    def _load_routine(self, name):
        try:
            if not self._regex_check_str(name):
                raise ValueError(f'Invalid routine filename for "{name}"')
            path = f"routines/{name}.py"
            if Path(path).is_file():
                try:
                    routine_class = self._import_attr(f"routines.{name}", name, kind="routine")
                    return routine_class
                except RuntimeError as e:
                    print(f"Unable to load routine {name}: {e}")
                    return None
        except Exception as e:
            print(f"Could not load routine {name}: {e}")
            return None

    def _get_service_hash(self, name):
        path = f"services/{name}.py"
        return str(self.file_hash(path))
    def _check_service_reload(self, config):
        if getattr(self.ctx, "services", None) is None:
            return
        for s in config.get("services", []):
            try:
                if not self._regex_check_str(s):
                    raise ValueError(f'Invalid service name: "{s}"')
                current_hash = self._get_service_hash(s)
                hash_key = f"services/{s}"
                if current_hash == self.hashes.get(hash_key):
                    continue
                print(f"Reloading service {s}")
                service = self._load_service(s)
                if service is not None:
                    self.ctx.services.__setattr__(s, service)
                    self.hashes[hash_key] = current_hash
            except Exception as e:
                print(f"Could not reload service {s}: {e}")
                continue
    def _load_services(self, config):
        srv = Context()
        for s in config.get("services", []):
            service = self._load_service(s)
            if service is not None:
                srv.__setattr__(s, service)
            self.hashes[f"services/{s}"] = self._get_service_hash(s)
        self.ctx.services = srv
    def _load_service(self, name):
        try:
            if not self._regex_check_str(name):
                raise ValueError(f'Invalid service filename for "{name}"')
            path = f"services/{name}.py"
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
        if not self._regex_check_str(name):
            raise ValueError(f'Invalid trigger name: "{name}"')

        trigger_path = f"triggers/{name}.py"
        parts = [str(self.file_hash(trigger_path))]

        for routine in routines:
            if not self._regex_check_str(routine):
                raise ValueError(f'Invalid routine name: "{routine}"')

            routine_path = f"routines/{routine}.py"
            parts.append(str(self.file_hash(routine_path)))

        return "|".join(parts)
    def _check_trigger_reload(self, config):
        if getattr(self.ctx, "triggers", None) is None:
            return
        for t in config.get("triggers", []):
            alias = t["alias"]
            trig = t["trigger"]
            routines = t["routines"]
            try:
                if not self._regex_check_str(alias) or not self._regex_check_str(trig):
                    raise ValueError(f'Invalid trigger alias or filename: "{alias}" / "{trig}"')

                current_hash = self._get_trigger_hash(trig, routines)
                hash_key = f"triggers/{alias}"
                if current_hash == self.hashes.get(hash_key):
                    continue
                print(f"Reloading trigger {alias}")
                trigger = self._load_trigger(**t)
                if trigger is not None:
                    self.ctx.triggers.start(alias, trigger)
                    self.hashes[hash_key] = current_hash
            except Exception as e:
                print(f"Unable to reload trigger {alias}: {e}")
                continue
    def _load_triggers(self, config):
        trg = TriggerManager()
        for t in config.get("triggers", []):
            alias = t["alias"]
            trig = t["trigger"]
            routines = t["routines"]
            try:
                trigger = self._load_trigger(**t)
                if trigger is not None:
                    trg.start(alias, trigger)
                    self.hashes[f"triggers/{alias}"] = self._get_trigger_hash(trig, routines)
            except Exception as e:
                print(f"Unable to load trigger {alias}: {e}")
        self.ctx.triggers = trg
    def _load_trigger(self, alias, trigger, params, routines):
        try:
            if not self._regex_check_str(alias) or not self._regex_check_str(trigger):
                raise ValueError(f'Invalid trigger filename or alias for "{alias}"')

            if not isinstance(params, dict):
                raise TypeError(f"Trigger {alias} does not have valid params.")

            path = f"triggers/{trigger}.py"
            if Path(path).is_file():
                try:
                    trigger_class = self._import_attr(f"triggers.{trigger}", trigger, kind="trigger")
                    r = [getattr(self.ctx.routines, r, None) for r in routines if getattr(self.ctx.routines, r, None) is not None]
                    trg = trigger_class(r, self.ctx, **params)
                    return trg
                except RuntimeError as e:
                    print(f"Unable to load trigger {alias} because the file does not contain a '{trigger}' class. ({e})")
                    return None
                except Exception as e:
                    raise e
            else:
                raise FileNotFoundError(f"Trigger file not found: {path}")

        except Exception as e:
            print(f"Could not load trigger {alias}: {e}")
            return None

    def _get_endpoint_hash(self, routines):
        h = ""
        for routine in routines:
            if not self._regex_check_str(routine):
                raise ValueError(f'Invalid routine name: "{routine}"')

            routine_path = f"routines/{routine}.py"
            h += str(self.file_hash(routine_path))
        return h
    def _check_endpoint_reload(self, server, config):
        if server is None:
            return
        if config.get("server_secret", None) is not None:
            server.change_secret(config["server_secret"])
        for e in config.get("endpoints", []):
            route = e["route"]
            routines = e["routines"]
            try:
                current_hash = self._get_endpoint_hash(routines)
                hash_key = f"endpoints{route}"
                if current_hash == self.hashes.get(hash_key):
                    continue
                self.ctx.should_restart = True
            except Exception as e:
                print(f"Unable to reload endpoints: {e}")
    def _load_endpoints(self, server, config):
        try:
            if config.get("server_secret", None) is not None:
                server.change_secret(config["server_secret"])
            for endpoint in config.get("endpoints", []):
                route = endpoint["route"]
                routines = endpoint["routines"]
                self._load_endpoint(server, **endpoint)
                self.hashes[f"endpoints{route}"] = self._get_endpoint_hash(routines)
        except Exception as e:
            print(f"Unable to load endpoint: {e}")
    def _load_endpoint(self, server, route, protocol, routines, method="POST"):
        try:
            if protocol not in ["HTTP", "WS"]:
                raise Exception(f'Endpoint "{route} cannot be of protocol "{protocol}". Must be "HTTP" or "WS".')
            r = [getattr(self.ctx.routines, r, None)(self.ctx) for r in routines if getattr(self.ctx.routines, r, None) is not None]
            if protocol == 'HTTP':
                server.make_http_endpoint(route=route, method=method, routines=r)
            else:
                server.make_ws_endpoint(route=route, routines=r)
        except Exception as e:
            print(f"Could not load endpoint {route}: {e}")

