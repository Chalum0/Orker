from packages.TriggerManager import TriggerManager
from packages.Context import Context
from packages.loaders.RoutineLoader import RoutineLoader
from packages.LoaderUtils import import_attr, regex_check_str, file_hash
from pathlib import Path


class TriggerLoader:
    def __init__(self, ctx: Context, hashes: dict, routine_loader: RoutineLoader):
        self.ctx = ctx
        self.hashes = hashes
        self._routine_loader = routine_loader

    def load(self, config: dict):
        trg = TriggerManager()
        for t in config.get("triggers", []):
            alias, trig, routines = t["alias"], t["trigger"], t["routines"]
            try:
                trigger = self._load_one(**t)
                if trigger is not None:
                    trg.start(alias, trigger)
                    self.hashes[f"triggers/{alias}/file"] = self._file_hash(trig)
                    self.hashes[f"triggers/{alias}/routines"] = self._routines_hash(routines)
            except Exception as e:
                print(f"Unable to load trigger {alias}: {e}")
        self.ctx.triggers = trg

    def check_reload(self, config: dict):
        if getattr(self.ctx, "triggers", None) is None:
            return
        for t in config.get("triggers", []):
            alias, trig, routines = t["alias"], t["trigger"], t["routines"]
            try:
                if not (regex_check_str(alias) and regex_check_str(trig)):
                    raise ValueError(f'Invalid trigger alias or filename: "{alias}" / "{trig}"')

                current_file = self._file_hash(trig)
                current_routines = self._routines_hash(routines)

                file_changed = current_file != self.hashes.get(f"triggers/{alias}/file")
                routines_changed = current_routines != self.hashes.get(f"triggers/{alias}/routines")

                if not file_changed and not routines_changed:
                    continue

                if file_changed:
                    print(f"Reloading trigger {alias} (file changed)")
                    trigger = self._load_one(**t)
                    if trigger is not None:
                        self.ctx.triggers.start(alias, trigger)
                else:
                    print(f"Updating routines for trigger {alias}")
                    resolved = self._resolve_routines(routines)
                    self.ctx.triggers.update_routines(alias, resolved)

                self.hashes[f"triggers/{alias}/file"] = current_file
                self.hashes[f"triggers/{alias}/routines"] = current_routines

            except Exception as e:
                print(f"Unable to reload trigger {alias}: {e}")

    def _file_hash(self, trig: str) -> str:
        if not regex_check_str(trig):
            raise ValueError(f'Invalid trigger name: "{trig}"')
        return str(file_hash(f"triggers/{trig}.py"))

    def _routines_hash(self, routines: list) -> str:
        parts = []
        for r in routines:
            if not regex_check_str(r):
                raise ValueError(f'Invalid routine name: "{r}"')
            parts.append(self._routine_loader._hash(r))
        return "|".join(parts)

    def _resolve_routines(self, routines: list) -> list:
        return [
            getattr(self.ctx.routines, r, None)
            for r in routines
            if getattr(self.ctx.routines, r, None) is not None
        ]

    def _load_one(self, alias, trigger, params, routines):
        try:
            if not regex_check_str(alias) or not regex_check_str(trigger):
                raise ValueError(f'Invalid trigger filename or alias for "{alias}"')
            if not isinstance(params, dict):
                raise TypeError(f"Trigger {alias} does not have valid params.")
            path = f"triggers/{trigger}.py"
            if not Path(path).is_file():
                raise FileNotFoundError(f"Trigger file not found: {path}")
            cls = import_attr(f"triggers.{trigger}", trigger, kind="trigger")
            return cls(self._resolve_routines(routines), self.ctx, **params)
        except RuntimeError as e:
            print(f"Unable to load trigger {alias}: file does not contain a '{trigger}' class. ({e})")
            return None
        except Exception as e:
            print(f"Could not load trigger {alias}: {e}")
            return None