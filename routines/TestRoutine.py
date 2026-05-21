import time
import random

class TestRoutine:
    def __init__(self, ctx):
        self.ctx = ctx

    def run(self, payload):
        time.sleep(int(random.randint(1, 2) / 10))
        return {"status": "ok", "message": "Hello World!", "payload": payload.get_json()}
