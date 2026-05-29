import sys
sys.dont_write_bytecode = True


from packages import APIServer, Context, TriggerManager, ConfigManager
from importlib import import_module, invalidate_caches
from threading import current_thread, main_thread
from pathlib import Path
import hashlib
import json
import time


class Orker:
    def __init__(self, config="config.json", hot_reload=True):
        self.host = "127.0.0.1"
        self.port = 5000
        self.server = None
        self.config_path = config
        self.config = {}
        self.hashes = {}
        self.hot_reload = hot_reload
        self.server_secret = "change_me"
        self.config_manager = ConfigManager.ConfigManager(self.config_path)

    # ---------- SERVER HANDLING ----------
    def start(self):
        print(f"Starting server on http://{self.host}:{self.port}.")
        while True:
            try:
                self._start_server()
            except KeyboardInterrupt:
                break

    def _start_server(self):
        if type(self.server) != APIServer.APIServer :
            self.server = APIServer.APIServer(self.server_secret)
        self.config_manager.load_config(self.server)
        self.context = self.config_manager.ctx
        self.server.start(host=self.host, port=self.port)
        if current_thread() is main_thread():
            try:
                while True:
                    if self.hot_reload:
                        # self.check_hashes()
                        self.config_manager.check_hot_reload(self.server)
                    if self.context.should_restart:
                        self._stop_server()
                        self.context.should_restart = False
                        print("Reloading")
                        return
                    time.sleep(1)
            except KeyboardInterrupt:
                self._stop_server()
                raise KeyboardInterrupt

    def _stop_server(self):
        if getattr(self, "context", None) is not None:
            triggers = getattr(self.context, "triggers", None)
            if triggers is not None:
                triggers.shutdown()
        if type(self.server) == APIServer.APIServer:
            self.server.stop()
            self.server = None


orker = Orker()
orker.start()
