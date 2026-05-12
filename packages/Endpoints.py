class Endpoints:
    def __init__(self, restart_function):
        self.endpoints: list[dict] = self._create_endpoints(restart_function=restart_function)

    def get_endpoints(self):
        return self.endpoints

    def _create_endpoints(self, restart_function):
        return []
        # def restart(payload):
        #     restart_function()
        #     return {"status": "ok"}
        # return [{"path": "restart", "handler": restart, "method": "POST"}]