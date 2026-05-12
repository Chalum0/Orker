from packages import APIServer, Endpoints


class Orker:
    def __init__(self):
        self.host = "127.0.0.1"
        self.port = 5000
        self.server = None

    def _start_server(self):
        if type(self.server) != APIServer.APIServer :
            self.server = APIServer.APIServer()
        self._make_endpoints()
        self.server.start(host=self.host, port=self.port)

    def _stop_server(self):
        if type(self.server) == APIServer.APIServer:
            print("stopping server")
            self.server.stop()
            self.server = None

    def _restart_server(self):
        if type(self.server) == APIServer.APIServer:
            self._stop_server()
            self.server = APIServer.APIServer()
            self._start_server()

    def _make_endpoints(self):
        print(type(self.server) == APIServer.APIServer)
        if type(self.server) == APIServer.APIServer:
            endpoints = Endpoints.Endpoints(restart_function=self._restart_server).get_endpoints()
            for endpoint in endpoints:
                self.server.make_endpoint(f"/api/"+endpoint["path"], endpoint["method"], endpoint["handler"])


orker = Orker()
orker._start_server()
