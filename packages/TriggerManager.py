import asyncio
import threading

class TriggerManager:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.new_event_loop()
        self.triggers = {}
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def update_routines(self, name, routines):
        return asyncio.run_coroutine_threadsafe(
            self._update_routines_async(name, routines),
            self.loop,
        )

    async def _update_routines_async(self, name, routines):
        trigger = self.triggers.get(name)

        if not trigger:
            raise KeyError(f"Trigger not found: {name}")

        trigger.set_routines(routines)

    def get_triggers(self):
        return self.triggers.keys()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _add(self, name, trigger):
        self.triggers[name] = trigger

    def start(self, name, trigger):
        if name not in self.triggers.keys():
            self._add(name, trigger)
            self.loop.call_soon_threadsafe(
                self.triggers[name].start
            )
        else:
            self._reload(name, trigger)

    def _reload(self, name, new_trigger):
        return asyncio.run_coroutine_threadsafe(
            self._reload_async(name, new_trigger),
            self.loop,
        )

    async def _reload_async(self, name, new_trigger):
        old = self.triggers.get(name)

        if old:
            old.stop()
            await old.wait_stopped()

        self.triggers[name] = new_trigger
        new_trigger.start()

    def stop(self, name):
        async def _stop():
            trigger = self.triggers.get(name)
            if trigger:
                trigger.stop()
                await trigger.wait_stopped()

        return asyncio.run_coroutine_threadsafe(_stop(), self.loop)

    def shutdown(self, timeout=5):
        async def _shutdown():
            for trigger in list(self.triggers.values()):
                trigger.stop()

            for trigger in list(self.triggers.values()):
                await trigger.wait_stopped()

            self.triggers.clear()

        future = asyncio.run_coroutine_threadsafe(_shutdown(), self.loop)
        future.result(timeout=timeout)

        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=timeout)
        self.loop.close()
