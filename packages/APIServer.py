from flask import Flask, jsonify, request
from packages.Context import Context
from threading import Thread
import logging

try:
    from waitress.server import create_server
except ImportError:
    create_server = None


class No200Filter(logging.Filter):
    def filter(self, record):
        return '" 200 ' not in record.getMessage()


class APIServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.server = None
        self.thread = None
        logging.getLogger("werkzeug").addFilter(No200Filter())

    def make_endpoint(self, route, method, handler):
        def endpoint():
            payload = request.get_json(silent=True) or {}
            payload = Context(variables=payload)
            if method == "GET":
                result = handler()
            else:
                result = handler(payload)
            if isinstance(result, dict):
                return jsonify(result)
            return result

        endpoint.__name__ = f"view_{handler.__name__}_{route.strip('/').replace('/', '_')}"
        self.app.add_url_rule(
            route,
            endpoint=endpoint.__name__,
            view_func=endpoint,
            methods=[method],
        )
        return endpoint

    def start(self, host="0.0.0.0", port=5000, threads=8):
        """Boot the HTTP server in a background thread using Waitress."""
        if self.server is not None:
            return

        if create_server is None:
            raise RuntimeError(
                "waitress is not installed. Install it with: pip install waitress"
            )

        self.server = create_server(
            self.app,
            host=host,
            port=port,
            threads=threads,
        )
        self.thread = Thread(target=self.server.run, daemon=True)
        self.thread.start()

    def stop(self):
        """Gracefully stop the HTTP server."""
        if self.server is None:
            return

        self.server.close()
        self.thread.join(timeout=5)
        self.server = None
        self.thread = None
