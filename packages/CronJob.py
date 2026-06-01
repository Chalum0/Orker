from packages.Context import Context
from datetime import datetime
from zoneinfo import ZoneInfo
from croniter import croniter
import threading
import inspect
import asyncio


class CronAsyncLoop:
    def __init__(self):
        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(
            target=self._run,
            daemon=True,
        )
        self.thread.start()

    def _run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def stop(self):
        self.loop.call_soon_threadsafe(self.loop.stop)


class CronJob:
    def __init__(
        self,
        expr,
        func,
        params=None,
        tz="Europe/Paris",
        loop=None,
        allow_overlap=False,
    ):
        self.expr = expr
        self.func = func
        self.tz = ZoneInfo(tz)
        self.params = params if isinstance(params, dict) else {}

        self.loop = loop
        self.allow_overlap = allow_overlap
        self.running = False
        self.task = None

        now = datetime.now(self.tz)
        self.it = croniter(self.expr, now)
        self.next_run = self.it.get_next(datetime)

    def tick(self):
        now = datetime.now(self.tz)

        if now < self.next_run:
            return

        # schedule next run immediately
        # prevents same cron from starting every tick
        self.it = croniter(self.expr, now)
        self.next_run = self.it.get_next(datetime)

        if self.running and not self.allow_overlap:
            return

        self._schedule()

    def _schedule(self):
        loop = self.loop

        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                raise RuntimeError("No asyncio loop available for CronJob")

        if loop.is_running():
            self.task = asyncio.run_coroutine_threadsafe(
                self._runner(),
                loop,
            )
        else:
            raise RuntimeError("CronJob loop is not running")

    async def _runner(self):
        self.running = True

        try:
            params = Context(self.params)
            if inspect.iscoroutinefunction(self.func):
                await self.func(params)
            else:
                await asyncio.to_thread(self.func, params)

        except Exception as e:
            print(f"Error in cron job: {e}")

        finally:
            self.running = False