import sys
sys.dont_write_bytecode = True
# Avoid __pycache__


from threading import current_thread, main_thread
from packages import APIServer, ConfigManager
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
        self._make_internal_api()

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
                    self.config_manager.tick_cron_jobs()
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

    def _make_internal_api(self):
        if self.server is None:
            return
        def handle_get_help(p):
            return self.config_manager.get_help()
        self.server.make_api_endpoint("/nodes", "GET", handle_get_help)

        def handle_get_triggers(p):
            return self.config_manager.list_triggers()
        self.server.make_api_endpoint("/triggers", "GET", handle_get_triggers)

        def handle_get_routines(p):
            return self.config_manager.list_routines()
        self.server.make_api_endpoint("/routines", "GET", handle_get_routines)

        def handle_create_routine(p):
            if not p.has_attributes(["name", "alias", "desc"]):
                return {"status": "Error", "message": "Invalid shape"}
            created = self.config_manager.create_routine(p.name, p.alias, p.desc)
            if created:
                return {"status": "Success", "message": "Created routine successfully"}
            return {"status": "Error", "message": f"Could not create routine {p.name}. Already exists or permission denied."}
        self.server.make_api_endpoint("/routines/create", "POST", handle_create_routine)

        def handle_delete_routine(p):
            if not p.has_attributes(["name"]):
                return {"status": "Error", "message": "Invalid shape"}
            deleted = self.config_manager.delete_routine(p.name)
            if deleted:
                return {"status": "Success", "message": "Deleted routine successfully"}
            return {"status": "Error", "message": f"Could not delete routine {p.name}. Does not exist or permission denied."}
        self.server.make_api_endpoint("/routines/delete", "POST", handle_delete_routine)

        def handle_get_routine_content(p):
            if not p.has_attributes(["name"]):
                return {"status": "Error", "message": "Invalid shape"}
            content = self.config_manager.get_json_routine_content(p.name)
            if content is not None:
                return {"routine": content}
            return {"status": "Error", "message": "Unknown error"}
        self.server.make_api_endpoint("/routines/get_content", "POST", handle_get_routine_content)

        def handle_set_routine_content(p):
            if not p.has_attributes(["name", "routine"]):
                return {"status": "Error", "message": "Invalid shape"}
            saved = self.config_manager.set_json_routine_content(p.name, p.routine)
            if saved:
                return {"status": "Success", "message": "Updated routine successfully"}
            return {"status": "Error", "message": "Unknown error"}
        self.server.make_api_endpoint("/routines/update", "POST", handle_set_routine_content)

orker = Orker()
orker.start()
