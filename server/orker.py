from packages.core import APIServer, RoutineManager
from threading import current_thread, main_thread
from packages.core.DBManager import DBManager
from packages.core.FileManager import send_package_to_node
from packages.core.HashManager import *
from flask import request
from uuid import uuid4
import requests
import time


class Orker:
    def __init__(self):
        create_package_folders()
        self._internal_server = None
        self._db_manager = DBManager()
        self._db_manager.remove_all_nodes()
        self._routine_manager = RoutineManager.RoutineManager()
        self.node_max_load = 100

        create_package_folders()


    # ----- INTERNAL WORK -----
    def _start_internal_server(self):
        if self._internal_server is None:
            self._internal_server = APIServer.APIServer()

        self._internal_server.start("127.0.0.1", 5000)
        if current_thread() is main_thread():
            try:
                previous_hashes = hash_services_and_routines()
                while True:
                    time.sleep(1)
                    self._check_nodes()
                    if not previous_hashes == hash_services_and_routines():
                        self._update_nodes()
                    # self._update_node_files(self._db_manager.get_best_node_for_routine().ip)
                    self._run_routines()
            except KeyboardInterrupt:
                self._stop_internal_server()

    def _stop_internal_server(self):
        if self._internal_server is not None:
            self._db_manager.remove_all_nodes()
            self._internal_server.stop()
            self._internal_server = None

    def _restart_server(self):
        self._stop_internal_server()
        self.start()

    def _define_internal_endpoints(self):
        self._internal_server = APIServer.APIServer()

        def whoami(payload):
            return {
                "status": "ok",
                "remote_addr": request.remote_addr,
                "headers": dict(request.headers)
            }
        self._internal_server.make_endpoint("/whoami", "GET", whoami)

        def connect(payload):
            id, secret = self._connect_node(request.remote_addr)
            print(id, secret)
            return {"status": "ok", "id": id, "secret": secret}
        self._internal_server.make_endpoint("/connect", "GET", connect)

        def disconnect(payload):
            print(payload)
            if self._db_manager.check_secret(request.remote_addr, payload["id"], payload["secret"]):
                self._db_manager.disconnect_node_with_id(payload["id"])
                return {"status": "ok"}
            return {"status": "error", "error": "invalid secret"}
        self._internal_server.make_endpoint("/disconnect", "POST", disconnect)

        def restart(payload):
            self._restart_server()
            yield {"status": "ok"}

    def _connect_node(self, ip):
        ip = ip
        id = uuid4()
        secret = uuid4()
        self._db_manager.connect_node(id, ip, secret)
        return id, secret

    def _ping_nodes(self):
        for ip in self._db_manager.get_nodes_ips():
            if not self._ping_node(ip):
                print(f"ping {ip} unsuccessful")
                self._db_manager.disconnect_node_with_ip(ip)

    @staticmethod
    def _ping_node(ip):
        url = f"http://{ip}:5001/ping"
        print(url)
        try:
            response = requests.get(url, timeout=0.5)
            return response.status_code == 200
        except requests.RequestException:
            return False

    def start(self):
        self._define_internal_endpoints()
        self._start_internal_server()





    # ----- HELPERS -----
    def _find_best_node_for_routine(self):
        while True:
            node = self._db_manager.get_best_node_for_routine()
            if node is None:
                break
            if self._ping_node(node.ip) and node.current_load < self.node_max_load:
                break
            else:
                self._db_manager.disconnect_node_with_ip(node.ip)
        return node

    def _run_routines(self):
        for i in range(self._routine_manager.get_queued_routine_amount()):
            # if available:
            best_node = self._find_best_node_for_routine()
            if best_node is None:
                break
            self._routine_manager.run_routine(best_node, self._db_manager)

    def _check_node_files(self, ip):
        """returns whether the node is up to date"""
        response = requests.get(f"http://{ip}:5001/hash", timeout=0.5)
        data = response.json()
        hashes = hash_services_and_routines()
        return hashes["routines"] == data["routines"] and hashes["services"] == data["services"]

    def _update_node_files(self, node):
        while self._db_manager.get_node_current_load(node.id) > 0:
            time.sleep(0.5)

        ip = node.ip
        secret = node.secret
        url = f"http://{ip}:5001"
        response = send_package_to_node(
            node_secret=secret,
            node_url=url,
            package_type="services",
            folder_path="packages/services"
        )
        response2 = send_package_to_node(
            node_secret=secret,
            node_url=url,
            package_type="services",
            folder_path="packages/services"
        )
        response3 = requests.post(f"{url}/restart", json={"secret": secret}).json()
        return response["status"] == "ok" and response2["status"] == "ok" and response3["status"] == "ok"

    def _update_nodes(self):
        self._db_manager.set_nodes_as_unavailable()
        for node in self._db_manager.get_nodes():
            if self._update_node_files(node):
                self._db_manager.set_node_as_available(node.id)

    def _check_nodes(self):
        for node in self._db_manager.get_nodes():
            if not self._check_node_files(node.ip):
                self._update_node_files(node)














orker = Orker()
orker.start()

