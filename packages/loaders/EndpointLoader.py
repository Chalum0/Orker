from packages.Context import Context
from packages.LoaderUtils import regex_check_str, file_hash


class EndpointLoader:
    def __init__(self, ctx: Context, hashes: dict):
        self.ctx = ctx
        self.hashes = hashes

    def load(self, server, config: dict):
        if server is None:
            return
        try:
            self._apply_secret(server, config)
            for endpoint in config.get("endpoints", []):
                route = endpoint["route"]
                self._load_one(server, **endpoint)
                self.hashes[f"endpoints{route}"] = self._hash(endpoint["routines"])
        except Exception as e:
            print(f"Unable to load endpoint: {e}")

    def check_reload(self, server, config: dict):
        if server is None:
            return
        self._apply_secret(server, config)
        for e in config.get("endpoints", []):
            route, routines = e["route"], e["routines"]
            try:
                current = self._hash(routines)
                key = f"endpoints{route}"
                if current != self.hashes.get(key):
                    self.ctx.should_restart = True
            except Exception as exc:
                print(f"Unable to reload endpoints: {exc}")

    def _hash(self, routines: list) -> str:
        h = ""
        for r in routines:
            if not regex_check_str(r):
                raise ValueError(f'Invalid routine name: "{r}"')
            h += str(file_hash(f"routines/{r}.py"))
        return h

    @staticmethod
    def _apply_secret(server, config: dict):
        if config.get("server_secret") is not None:
            server.change_secret(config["server_secret"])

    def _load_one(self, server, route, protocol, routines, method="POST"):
        try:
            if protocol not in ("HTTP", "WS"):
                raise Exception(
                    f'Endpoint "{route}" cannot use protocol "{protocol}". Must be "HTTP" or "WS".'
                )
            resolved = [
                getattr(self.ctx.routines, r, None)(self.ctx)
                for r in routines
                if getattr(self.ctx.routines, r, None) is not None
            ]
            if protocol == "HTTP":
                server.make_http_endpoint(route=route, method=method, routines=resolved)
            else:
                server.make_ws_endpoint(route=route, routines=resolved)
        except Exception as e:
            print(f"Could not load endpoint {route}: {e}")