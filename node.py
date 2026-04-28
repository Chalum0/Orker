from threading import Thread, current_thread, main_thread, Lock
from packages.core.HashManager import *
from packages.core import APIServer
import requests
import time


class Node:
    def __init__(self):
        self._internal_server = None
        self.host = "http://127.0.0.1:5000"
        self.id = None
        self.secret = None

        create_package_folders()


    def _start_internal_server(self):
        if self._internal_server is None:
            self._internal_server = APIServer.APIServer()

        self._internal_server.start("127.0.0.1", 5001)
        if current_thread() is main_thread():
            try:
                while True:
                    time.sleep(1)
            except KeyboardInterrupt:
                self._stop_internal_server()

    def _stop_internal_server(self):
        if self._internal_server is not None:
            self._disconnect_from_server()
            self._internal_server.stop()
            self._internal_server = None

    def _define_internal_endpoints(self):
        self._connect_to_server()
        self._start_internal_server()
        self._stop_internal_server()

    def _connect_to_server(self):
        try:
            response = requests.get(f"{self.host}/connect", timeout=0.5)
            data = response.json()
            self.id = data["id"]
            self.secret = data["secret"]
        except Exception as e:
            print(f"unable to connect to server: {e}")

    def _disconnect_from_server(self):
        try:
            response = requests.post(f"{self.host}/disconnect", json={"id": self.id, "secret": self.secret}, timeout=0.5,)
            print(response.status_code)
        except Exception as e:
            print(f"unable to disconnect from server: {e}")


node = Node()
node._define_internal_endpoints()
