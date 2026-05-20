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


class Trigger:
    def __init__(self, address, secret, custom_headers=None):
        self.custom_headers = custom_headers or {}
        self.secret = secret
        self.address = address
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

    def send_post(self, body):
        response = self.session.post(self.address, json=body, headers={ **self.custom_headers, "Authorization": f"Bearer {self.secret}"})
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError:
            raise Exception(f"Malformed response for {self.address}: \n {response.text} {'\n'*2} body: {body}")

class SockTrigger:
    def __init__(self, address, secret, custom_headers=None, idle_timeout=2):
        self.address = address
        self.secret = secret
        self.custom_headers = custom_headers or {}
        self.idle_timeout = idle_timeout

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
        ws = None

        # If incorrect secret: set fail for job and close the worker
        try:
            ws = self._connect()
        except Exception as e:
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
                if time.monotonic() - last_activity >= self.idle_timeout:
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

    def send(self, body, timeout=None):
        if self.closed:
            raise RuntimeError("Trigger Closed")

        result_queue = queue.Queue(maxsize=1)

        self._ensure_worker()
        self.jobs.put((body, result_queue))

        ok, value = result_queue.get(timeout=timeout)

        if ok:
            return value

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


class AsyncTrigger:
    def __init__(self, address, secret, custom_headers=None, idle_timeout=2):
        self.address = f"{address}/async"
        self.secret = secret
        self.custom_headers = custom_headers or {}
        self.idle_timeout = idle_timeout

        self.ws = None
        self.pending = {}
        self.futures = []

        self.receiver_task = None
        self.idle_task = None

        self.send_lock = asyncio.Lock()
        self.send_lock = None
        self.last_activity = 0

        self.loop = asyncio.new_event_loop()
        self.thread = threading.Thread(target=self._run_loop, daemon=True)
        self.thread.start()

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    async def connect(self):
        """Connects to the server and defines a receiver and a watcher"""
        if self.send_lock is None:
            self.send_lock = asyncio.Lock()
        if self.ws is not None:
            return

        headers = {
            **self.custom_headers,
            "Authorization": f"Bearer {self.secret}",
        }

        self.ws = await websockets.connect(
            self.address,
            additional_headers=headers
        )

        self.last_activity = time.monotonic()
        self.receiver_task = asyncio.create_task(self._receiver())
        self.idle_task = asyncio.create_task(self._idle_watcher())

    def send(self, body):
        future = asyncio.run_coroutine_threadsafe(
            self._send(body),
            self.loop,
        )

        # waits until request sent, not until server finished
        return future.result()

    async def _send(self, body):
        await self.connect()

        request_id = str(uuid.uuid4())
        future = self.loop.create_future()

        self.pending[request_id] = future
        self.futures.append(future)

        async with self.send_lock:
            await self.ws.send(json.dumps({
                "id": request_id,
                "body": body
            }))

        self.last_activity = time.monotonic()

    async def _get_results(self):
        return await asyncio.gather(*self.futures)

    def get_results(self):
        future = asyncio.run_coroutine_threadsafe(
            self._get_results(),
            self.loop,
        )

        return future.result()

    async def _receiver(self):
        try:
            async for raw in self.ws:
                message = json.loads(raw)
                msg_type = message.get("type")

                if msg_type == "ack":
                    self.last_activity = time.monotonic()
                    continue

                request_id = message.get("id")
                future = self.pending.pop(request_id, None)

                if future is None or future.done():
                    continue

                if msg_type == "result":
                    future.set_result(message.get("result"))

                elif msg_type == "error":
                    future.set_exception(message.get("error"))

                self.last_activity = time.monotonic()


        except Exception as e:
            for future in self.pending.values():
                if not future.done():
                    future.set_exception(e)
            self.pending.clear()

    async def _idle_watcher(self):
        while True:
            await asyncio.sleep(0.2)

            if self.ws is None:
                break

            if self.pending:
                continue

            if time.monotonic() - self.last_activity >= self.idle_timeout:
                await self._close_socket()
                break

    async def _close_socket(self):
        if self.ws is not None:
            await self.ws.close()
            self.ws = None

        self.receiver_task = None
        self.idle_task = None

    async def _close(self):
        await self.close()

    def stop(self):
        future = asyncio.run_coroutine_threadsafe(
            self.close(),
            self.loop,
        )
        future.result()

        self.loop.call_soon_threadsafe(self.loop.stop)
        self.thread.join(timeout=5)

    async def close(self):
        await self._close_socket()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
