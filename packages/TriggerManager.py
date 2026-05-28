import asyncio
import threading

class TriggerManager:
    def __init__(self, loop=None):
        self.loop = loop or asyncio.new_event_loop()
        self.triggers = {}
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

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
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                self._reload_async(name, new_trigger)
            )
        )

    async def _reload_async(self, name, new_trigger):
        old = self.triggers.get(name)

        if old:
            await old.stop()

        self.triggers[name] = new_trigger
        new_trigger.start()

    def stop(self, name):
        self.loop.call_soon_threadsafe(
            lambda: asyncio.create_task(
                self.triggers[name].stop()
            )
        )
