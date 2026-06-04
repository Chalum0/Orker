from packages.CronJob import CronAsyncLoop
from packages.Context import Context
from packages.LoaderUtils import read_json, file_hash
from packages.loaders.GatewayLoader import GatewayLoader
from packages.loaders.ServiceLoader import ServiceLoader
from packages.loaders.RoutineLoader import RoutineLoader
from packages.loaders.TriggerLoader import TriggerLoader
from packages.loaders.EndpointLoader import EndpointLoader
from packages.loaders.CronLoader import CronLoader
from packages.RoutineManager import RoutineManager
from pathlib import Path
import json


class ConfigManager:
    def __init__(self, config_path: str):
        self.ctx = Context()
        self.ctx.should_restart = False
        self.working_config: dict = {}
        self.config_path = config_path
        self.hashes: dict = {}

        self._async_loop = CronAsyncLoop()
        self.loop = self._async_loop.loop

        # Loaders share ctx and hashes by reference
        self._routines = RoutineLoader(self.ctx, self.hashes)
        self._services = ServiceLoader(self.ctx, self.hashes)
        self._gateways = GatewayLoader(self.ctx, self.hashes)
        self._triggers = TriggerLoader(self.ctx, self.hashes, self._routines)
        self._endpoints = EndpointLoader(self.ctx, self.hashes)
        self._crons = CronLoader(self.ctx, self.hashes, self.loop, self._routines)
        self._routine_mgr = RoutineManager(self.working_config, self._save_working_config)

    # ------------------------------------------------------------------
    # Public API — introspection
    # ------------------------------------------------------------------

    def get_routine_help(self) -> dict:
        if getattr(self.ctx, "services", None) is None:
            return {}
        h = {}
        for key, value in self.ctx.services.get_json().items():
            # Alias
            c: dict = value().help()
            for k, v in c.items():
                v["service"] = key
            h.update(c)
        return h

    def get_gateway_help(self) -> dict:
        if getattr(self.ctx, "gateways", None) is None:
            return {}
        h = {}
        for key, value in self.ctx.gateways.get_json().items():
            c: dict = value.help()
            for k, v in c.items():
                v["service"] = key
            h.update(c)
        return h

    def get_help(self) -> dict:
        h = {}
        h.update(self.get_routine_help())
        h.update(self.get_gateway_help())
        # print(h["get_random_number"])
        # return {}
        print(h)
        return h

    def list_triggers(self) -> dict:
        if getattr(self.ctx, "triggers", None) is None:
            return {"triggers": []}
        return {"triggers": list(self.ctx.triggers.get_triggers())}

    def list_routines(self) -> dict:
        return self._routine_mgr.list()

    # ------------------------------------------------------------------
    # Public API — JSON routine CRUD
    # ------------------------------------------------------------------

    def create_routine(self, name: str, alias: str, description: str) -> bool:
        return self._routine_mgr.create(name, alias, description)

    def delete_routine(self, name: str) -> bool:
        return self._routine_mgr.delete(name)

    def get_json_routine_content(self, name: str):
        return self._routine_mgr.get_content(name)

    def set_json_routine_content(self, name: str, content) -> bool:
        return self._routine_mgr.set_content(name, content)

    # ------------------------------------------------------------------
    # Public API — full load
    # ------------------------------------------------------------------

    def load_config(self, server):
        config = self._refresh_config()
        self._routines.load(config)
        self._services.load(config)
        self._gateways.load(config)
        self._triggers.load(config)
        self._load_variables(config)
        self._endpoints.load(server, config)
        self._crons.load(config)
        self.hashes[self.config_path] = file_hash(self.config_path)

    # ------------------------------------------------------------------
    # Public API — partial / selective loads
    # ------------------------------------------------------------------

    def load_gateways(self):
        self._gateways.load(self._refresh_config())

    def load_variables(self):
        self._load_variables(self._refresh_config())

    def load_routines(self):
        self._routines.load(self._refresh_config())

    def load_services(self):
        self._services.load(self._refresh_config())

    def load_triggers(self):
        config = self._refresh_config()
        self._routines.load(config)
        self._triggers.load(config)

    def load_endpoints(self, server):
        config = self._refresh_config()
        self._routines.load(config)
        self._endpoints.load(server, config)

    def load_crons(self):
        config = self._refresh_config()
        self._routines.load(config)
        self._crons.load(config)

    # ------------------------------------------------------------------
    # Public API — hot reload
    # ------------------------------------------------------------------

    def check_hot_reload(self, server):
        config = self._refresh_config()
        # Routines must be reloaded first — triggers, crons, and endpoints all
        # depend on ctx.routines being up to date before their own check runs.
        self._routines.check_reload(config)
        self._services.check_reload(config)
        self._gateways.check_reload(config)
        self._triggers.check_reload(config)
        self._crons.check_reload(config)
        self._endpoints.check_reload(server, config)
        self._load_variables(config)

    def tick_cron_jobs(self):
        self._crons.tick()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _refresh_config(self) -> dict:
        self.working_config = read_json(self.config_path, self.working_config)
        self._routine_mgr._config = self.working_config
        return self.working_config

    def _load_variables(self, config: dict):
        v = Context()
        for key, value in config.get("variables", {}).items():
            v.__setattr__(key, value)
        self.ctx.variables = v

    def _save_working_config(self):
        Path(self.config_path).write_text(
            json.dumps(self.working_config, indent=4),
            encoding="utf-8",
        )