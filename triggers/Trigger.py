from packages.Context import Context
from routines import TestRoutine
import asyncio
import inspect

class Trigger:
    def __init__(self, routines, ctx):
        # routines are of form [class]
        self.routines = routines
        # self.routines = routines
        self.running = False
        self.task = None
        self.ctx = ctx

    def set_routines(self, routines):
        self.routines = tuple(routines)

    def trigger(self, payload: dict):
        payload = Context(payload)

        # snapshot
        routines = self.routines

        for routine in routines:
            routine(self.ctx).run(payload)

    async def _runner(self):
        if inspect.iscoroutinefunction(self.run):
            await self.run()
        else:
            await asyncio.to_thread(self.run)

    def start(self):
        if self.task and not self.task.done():
            return

        self.running = True
        self.task = asyncio.create_task(self._runner())

    def stop(self):
        self.running = False

        if self.task and not self.task.done():
            self.task.cancel()

    async def wait_stopped(self):
        if not self.task:
            return

        try:
            await self.task
        except asyncio.CancelledError:
            pass

    async def restart(self):
        self.stop()
        await self.wait_stopped()
        self.start()

    def run(self):
        raise NotImplementedError
