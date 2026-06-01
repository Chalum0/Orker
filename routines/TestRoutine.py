import random
import time

class TestRoutine:
    def __init__(self, ctx):
        self.ctx = ctx

    def run(self, payload):
        # print(payload.content)
        print(f"Payload: {payload}")
        return {"status": "ok", "message": "Hello World!", "payload": payload.get_json()}

    def __str__(self):
        return "Hello World!"
