from websocket import create_connection, WebSocketTimeoutException
from requests.adapters import HTTPAdapter
import websockets
import threading
import websocket
import requests
import asyncio
import queue
import json
import time
import uuid


class TriggerParent:
    def __init__(self, address, secret, custom_headers=None, idle_timeout=2):
        self.address = address
        self.secret = secret
        self.custom_headers = custom_headers or {}
        self.idle_timeout = idle_timeout

    def send(self, body, timeout=None):
        return self._s(body, timeout=timeout)

    def send_all(self, bodies, timeout=None):
        for body in bodies:
            self._s(body, timeout=timeout)

class HTTPTrigger(TriggerParent):
    def __init__(self, address, secret, custom_headers=None, idle_timeout=None):
        TriggerParent.__init__(self, address=address, secret=secret,custom_headers=custom_headers, idle_timeout=idle_timeout)
        self.session = requests.Session()
        self.session.mount(
            "http://",
            HTTPAdapter(pool_connections=1, pool_maxsize=1),
        )
        self.session.mount(
            "https://",
            HTTPAdapter(pool_connections=1, pool_maxsize=1),
        )

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

    def _s(self, body, timeout=None):
        response = self.session.post(self.address, json=body, headers={ **self.custom_headers, "Authorization": f"Bearer {self.secret}"})
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError:
            raise Exception(f"Malformed response for {self.address}: \n {response.text} {'\n'*2} body: {body}")

class WebSocketSyncTrigger(TriggerParent):
    def __init__(self, address, secret, custom_headers=None, idle_timeout=2):
        TriggerParent.__init__(self, address=address, secret=secret,custom_headers=custom_headers, idle_timeout=idle_timeout)

        self.jobs = queue.Queue()
        self.worker = None
        self.closed = False

    def _connect(self):
        # Create custom headers and open connection
        headers = [f"Authorization: Bearer {self.secret}"]
        for key, value in self.custom_headers.items():
            headers.append(f"{key}: {value}")

        ws = websocket.create_connection(
            self.address,
            header=headers,
            timeout=5
        )

        ws.settimeout(None)

        return ws

    def _worker_loop(self):
        print("Worker loop created")
        ws = None

        # If incorrect secret: set fail for job and close the worker
        try:
            print("creating ws")
            ws = self._connect()
            print("created ws")
        except Exception as e:
            print(f"failed to create ws: {e}")
            try:
                body, result_queue = self.jobs.get_nowait()
                result_queue.put((False, e))
            except queue.Empty:
                pass
            self.worker = None
            return


        last_activity = time.monotonic()

        while not self.closed:
            # Get next job or check if the last activity was less than x seconds ago. If nto, then we close the connection
            try:
                job = self.jobs.get(timeout=0.1)
            except queue.Empty:
                if self.idle_timeout is None:
                    continue
                elif time.monotonic() - last_activity >= self.idle_timeout:
                    break
                continue

            body, result_queue = job

            try:
                ws.send(json.dumps(body))
                raw = ws.recv()
                last_activity = time.monotonic()

                try:
                    result = json.loads(raw)
                except json.JSONDecodeError:
                    raise Exception(f"Malformed response: {raw}")

                result_queue.put((True, result))

            except Exception as e:
                result_queue.put((False, e))
                break
        print("worker loop closed")
        ws.close()
        self.worker = None

    def _ensure_worker(self):
        if self.worker is not None and self.worker.is_alive():
            return

        self.worker = threading.Thread(
            target=self._worker_loop,
            daemon=True
        )
        self.worker.start()

    def _s(self, body, timeout=None):
        if self.closed:
            raise RuntimeError("Trigger Closed")

        print("a")
        result_queue = queue.Queue(maxsize=1)
        print("b")
        self._ensure_worker()
        print("c")
        self.jobs.put((body, result_queue))
        print("d")

        ok, value = result_queue.get(timeout=timeout)
        print("e")

        if ok:
            return value
        print("f")

        raise value

    def close(self):
        self.closed = True

        if self.worker is not None:
            self.worker.join(timeout=5)
            self.worker = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class WebSocketAsyncTrigger(TriggerParent):
    def __init__(self, address, secret, custom_headers=None, idle_timeout=2):
        TriggerParent.__init__(self, address=f"{address}/async", secret=secret,custom_headers=custom_headers, idle_timeout=idle_timeout)

        self.ws = None
        self.lock = threading.Lock()
        self.running = False
        self.reader_thread = None

        self.pending = {}
        self.done = []
        self.order = []

    def _headers(self):
        headers = {
            **self.custom_headers,
            "Authorization": f"Bearer {self.secret}",
        }
        return [f"{k}: {v}" for k, v in headers.items()]

    def connect(self):
        if self.ws:
            return

        self.ws = create_connection(
            self.address.replace("http://", "ws://").replace("https://", "wss://"),
            header=self._headers(),
            timeout=self.idle_timeout,
        )

        self.running = True
        self.reader_thread = threading.Thread(target=self._reader, daemon=True)
        self.reader_thread.start()

    def _reader(self):
        while self.running:
            try:
                msg = self.ws.recv()
                data = json.loads(msg)

                msg_type = data.get("type")
                request_id = data.get("id")

                if msg_type == "ack":
                    self.pending[request_id] = "running"

                elif msg_type in ("result", "error"):
                    self.pending.pop(request_id, None)
                    self.done.append(data)

            except WebSocketTimeoutException:
                continue
            except Exception:
                self.running = False
                break

    def _s(self, body, timeout=None):
        """
        Sync function.
        Sends job.
        Returns request id.
        Result later in self.done.
        """
        self.connect()

        request_id = str(uuid.uuid4())
        self.order.append(request_id)

        payload = {
            "id": request_id,
            "body": body,
        }

        with self.lock:
            self.pending[request_id] = "sent"
            self.ws.send(json.dumps(payload))

        return request_id

    def get_done(self, ordered=False):
        items = self.done[:]
        self.done.clear()
        if ordered:
            return sort_done(items, self.order)
        return items

    def close(self):
        self.running = False
        if self.ws:
            self.ws.close()
            self.ws = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()

def sort_done(done, order):
    by_id = {item["id"]: item for item in done}

    return [
        by_id[id_]
        for id_ in order
        if id_ in by_id
    ]
