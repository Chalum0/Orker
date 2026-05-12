from packages import APIServer


class Orker:
    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 5000
        self.server = None

    def _start_server(self):
        if type(self.server) == APIServer.APIServer :
            self.server = APIServer.APIServer()
        self.server.start(host=self.host, port=self.port)

    def _stop_server(self):
        if type(self.server) == APIServer.APIServer:
            self.server.stop()
            self.server = None

