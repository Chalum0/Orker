from packages.Context import Context
from packages.LoaderUtils import import_attr, regex_check_str, file_hash
from pathlib import Path


class GatewayLoader:
    def __init__(self, ctx: Context, hashes: dict):
        self.ctx = ctx
        self.hashes = hashes

    def load(self, config: dict):
        gtw = Context()
        for g in config.get("gateways", []):
            alias = g["alias"]
            gateway = self._load_one(**g)
            if gateway is not None:
                gtw.__setattr__(alias, gateway)
                self.hashes[f"gateways/{alias}"] = self._hash(g["gateway"])
        self.ctx.gateways = gtw

    def check_reload(self, config: dict):
        if getattr(self.ctx, "gateways", None) is None:
            return
        for g in config.get("gateways", []):
            alias, gate = g["alias"], g["gateway"]
            try:
                if not (regex_check_str(alias) and regex_check_str(gate)):
                    raise ValueError(f'Invalid gateway alias or filename: "{alias}" / "{gate}"')
                current = self._hash(gate)
                key = f"gateways/{alias}"
                if current == self.hashes.get(key):
                    continue
                print(f"Reloading gateway {alias}")
                gw = self._load_one(**g)
                if gw is not None:
                    self.ctx.gateways.__setattr__(alias, gw)
                    self.hashes[key] = current
            except Exception as e:
                print(f"Unable to reload gateway {alias}: {e}")

    def _hash(self, name: str) -> str:
        return str(file_hash(f"gateways/{name}.py"))

    def _load_one(self, alias, gateway, params):
        try:
            if not regex_check_str(alias) or not regex_check_str(gateway):
                raise ValueError(f'Invalid gateway filename or alias for "{alias}"')
            if not isinstance(params, dict):
                raise TypeError(f"Gateway {alias} does not have valid params.")
            path = f"gateways/{gateway}.py"
            if not Path(path).is_file():
                raise FileNotFoundError(f"Gateway file not found: {path}")
            cls = import_attr(f"gateways.{gateway}", gateway, kind="gateway")
            return cls(**params)
        except RuntimeError as e:
            print(f"Unable to load gateway {alias}: file does not contain a '{gateway}' class. ({e})")
            return None
        except Exception as e:
            print(f"Could not load gateway {alias}: {e}")
            return None