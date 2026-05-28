from packages.Context import Context
from routines import TestRoutine
import asyncio
import inspect

class Trigger:
    def __init__(self, routines, ctx):
        # routines are of form [(class, (str, str))]
        self.routines = []
        self.hashes = {}
        for routine, h in routines:
            self.routines.append(routine)
            self.hashes[h[0]] = h[1]
        # self.routines = routines
        self.running = False
        self.task = None
        self.ctx = ctx

    def trigger(self, payload: dict):
        for routine in self.routines:
            payload = Context(payload)
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

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()

            try:
                await self.task
            except asyncio.CancelledError:
                pass

    async def restart(self):
        await self.stop()
        self.start()

    def run(self):
        raise NotImplementedError
