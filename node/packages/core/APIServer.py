from flask import Flask, jsonify, request
from werkzeug.serving import make_server

from threading import Thread


class APIServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.server = None
        self.thread = None

        @self.app.route('/ping')
        def ping():
            return jsonify({'status': "ok"}), 200

    def make_endpoint(self, route, method, handler):
        def endpoint():
            payload = request.get_json(silent=True) or {}
            return jsonify(handler(payload))

        endpoint.__name__ = f"view_{handler.__name__}_{route.strip('/').replace('/', '_')}"

        self.app.add_url_rule(
            route,
            endpoint=endpoint.__name__,
            view_func=endpoint,
            methods=[method],
        )
        return endpoint

    def start(self, host="0.0.0.0", port=5000):
        if self.server is not None:
            return

        self.server = make_server(host, port, self.app)
        self.thread = Thread(target=self.server.serve_forever)
        self.thread.start()
        print(f"API started on http://{host}:{port}")

    def stop(self):
        if self.server is None:
            return

        self.server.shutdown()
        self.thread.join()
        self.server = None
        self.thread = None
        print("API stopped")
