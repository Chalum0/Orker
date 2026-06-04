from packages.CronJob import CronJob
from packages.Context import Context
from packages.loaders.RoutineLoader import RoutineLoader
from croniter import CroniterBadCronError


class CronLoader:
    def __init__(self, ctx: Context, hashes: dict, loop, routine_loader: RoutineLoader):
        self.ctx = ctx
        self.hashes = hashes
        self.loop = loop
        self._routine_loader = routine_loader

    def load(self, config: dict):
        jobs = getattr(self.ctx, "jobs", Context())
        for job in config.get("cron_jobs", []):
            name, routine, params, expr, tz = self._unpack(job)
            if not all([name, routine, expr]):
                print(f"Invalid cron job: {job}")
                continue
            c = self._load_one(routine, params, expr, tz)
            if c is not None:
                setattr(jobs, name, c)
            self.hashes[f"cron/{name}"] = self._hash(routine, params, expr, tz)
        self.ctx.jobs = jobs

    def check_reload(self, config: dict):
        if getattr(self.ctx, "jobs", None) is None:
            return
        config_names = {
            job.get("name")
            for job in config.get("cron_jobs", [])
            if job.get("name")
        }
        # Remove deleted jobs
        for name in list(vars(self.ctx.jobs).keys()):
            if name not in config_names:
                old = getattr(self.ctx.jobs, name, None)
                if old is not None and hasattr(old, "stop"):
                    old.stop()
                delattr(self.ctx.jobs, name)
                self.hashes.pop(f"cron/{name}", None)
        # Reload changed / new jobs
        for job in config.get("cron_jobs", []):
            name, routine, params, expr, tz = self._unpack(job)
            if not all([name, routine, expr]):
                print(f"Invalid cron job: {job}")
                continue
            key = f"cron/{name}"
            current = self._hash(routine, params, expr, tz)
            if current == self.hashes.get(key):
                continue
            c = self._load_one(routine, params, expr, tz)
            if c is not None:
                setattr(self.ctx.jobs, name, c)
            self.hashes[key] = current

    def tick(self):
        jobs = getattr(self.ctx, "jobs", None)
        if jobs is None:
            return
        for name, job in jobs.get_json().items():
            if not hasattr(job, "tick"):
                continue
            try:
                job.tick()
            except Exception as e:
                print(f"Error while ticking cron job {name}: {e}")

    def _hash(self, routine, params, expr, tz) -> str | None:
        if routine is None:
            return None
        return "|".join([
            str(self._routine_loader._hash(routine)),
            str(params),
            str(expr),
            str(tz),
        ])

    def _load_one(self, routine: str, params: dict, expr: str, tz: str = "Europe/Paris"):
        r = getattr(self.ctx.routines, routine, None)
        if r is None:
            print(f"Cron routine not found: {routine}")
            return None
        try:
            return CronJob(expr, r(self.ctx).run, params, tz=tz, loop=self.loop)
        except CroniterBadCronError as e:
            print(f"Invalid cron expression for routine {routine}: {expr} ({e})")
            return None
        except Exception as e:
            print(f"Unable to load cron job for routine {routine}: {e}")
            return None

    @staticmethod
    def _unpack(job: dict):
        return (
            job.get("name"),
            job.get("routine"),
            job.get("params", {}),
            job.get("expr"),
            job.get("tz", "Europe/Paris"),
        )