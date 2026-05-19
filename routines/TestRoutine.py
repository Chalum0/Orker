class TestRoutine:
    def __init__(self, ctx):
        self.ctx = ctx

    def run(self, payload):
        return {"status": "ok", "message": "Hello World!", "payload": payload.get_json()}
