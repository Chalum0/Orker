from fastapi import FastAPI, WebSocket, WebSocketDisconnect, status, WebSocketException, Request, HTTPException
from concurrent.futures import ThreadPoolExecutor
from fastapi.responses import JSONResponse
from packages.Context import Context
from threading import Thread
from queue import Queue
import uvicorn
import inspect
import secrets
import asyncio


class APIServer:
    def __init__(self, secret):
        self.secret = secret
        self.app = FastAPI()
        self.server = None
        self.thread = None
        self.jobs = Queue()
        self.executor = ThreadPoolExecutor(max_workers=16)

        @self.app.middleware("http")
        async def check_bearer_token(request: Request, call_next):
            auth = request.headers.get("Authorization", "")

            if not auth.startswith("Bearer "):
                return JSONResponse(
                    status_code=401,
                    content={"detail": "Unauthorized"},
                )

            token = auth.removeprefix("Bearer ").strip()

            if not secrets.compare_digest(token, self.secret):
                return JSONResponse(
                    status_code=403,
                    content={"detail": "Forbidden"},
                )

            return await call_next(request)

    def change_secret(self, secret):
        self.secret = secret

    def make_ws_endpoint(self, route, routines):
        def handler(payload):
            results = []
            for routine in routines:
                results.append(routine.run(payload))
            return results

        async def run_handler(data):
            payload = Context(variables=data)

            if inspect.iscoroutinefunction(handler):
                return await handler(payload)

            return await asyncio.to_thread(handler, payload)


        # Synchronous endpoints returns the result of the routine directly
        async def endpoint(ws: WebSocket):
            self.verify_ws_token(ws)
            await ws.accept()

            try:
                while True:
                    data = await ws.receive_json()
                    result = await run_handler(data)

                    if result is not None:
                        await ws.send_json(result)
            except WebSocketDisconnect:
                pass
            except Exception:
                await ws.close(code=1011)


        # Asynchronous endpoint returns ok directly then sends the result of the routine once done with it's id
        async def async_endpoint(ws: WebSocket):
            self.verify_ws_token(ws)
            await ws.accept()

            loop = asyncio.get_running_loop()
            send_lock = asyncio.Lock()

            async def safe_send(payload):
                async with send_lock:
                    await ws.send_json(payload)

            def run_job(body):
                payload = Context(variables=body)
                return handler(payload)

            def submit_job(request_id, body):
                future = self.executor.submit(run_job, body)

                def on_done(f):
                    try:
                        result = f.result()
                        payload = {
                            "type": "result",
                            "id": request_id,
                            "result": result,
                        }
                    except Exception as e:
                        payload = {
                            "type": "error",
                            "id": request_id,
                            "error": str(e)
                        }

                    asyncio.run_coroutine_threadsafe(
                        safe_send(payload),
                        loop
                    )
                future.add_done_callback(on_done)

            try:
                while True:
                    data = await ws.receive_json()
                    request_id = data.get("id")
                    body = data.get("body", {})

                    await safe_send({
                        "type":  "ack",
                        "id": request_id
                    })
                    submit_job(request_id, body)
            except WebSocketDisconnect:
                pass

        clean_route = route.strip("/").replace("/", "_")
        endpoint.__name__ = f"view_{handler.__name__}_{clean_route}"
        async_endpoint.__name__ = f"view_{handler.__name__}_{clean_route}_async"

        # Create both endpoints (/route and /route/async)
        self.app.add_api_websocket_route(route, endpoint)
        self.app.add_api_websocket_route(f"{route.rstrip('/')}/async", async_endpoint)

    def make_http_endpoint(self, route, method, routines):
        def handler(payload):
            results = []
            for routine in routines:
                results.append(routine.run(payload))
            return results

        async def endpoint(request: Request):
            payload_data = await request.json() if method != "GET" else {}
            payload = Context(variables=payload_data)
            result = handler(payload)

            if inspect.isawaitable(result):
                result = await result

            if isinstance(result, dict):
                return JSONResponse(result)

            return result

        self.app.add_api_route(
            route,
            endpoint,
            methods=[method],
            name=f"view_{handler.__name__}_{route.strip('/').replace('/', '_')}",
        )


    def verify_ws_token(self, ws: WebSocket):
        auth = ws.headers.get("authorization", "")


        if not auth.startswith("Bearer "):
            raise WebSocketException(
                code=status.WS_1008_POLICY_VIOLATION
            )

        token = auth.removeprefix("Bearer ").strip()

        if not secrets.compare_digest(token, self.secret):
            raise WebSocketException(
                code=status.WS_1008_POLICY_VIOLATION
            )

    def start(self, host="0.0.0.0", port=5000):
        """Boot ASGI server in background thread using Uvicorn."""
        if self.server is not None:
            return

        config = uvicorn.Config(
            self.app,
            host=host,
            port=port,
            log_level="warning",
        )

        self.server = uvicorn.Server(config)
        self.thread = Thread(target=self.server.run, daemon=True)
        self.thread.start()

    def stop(self):
        """Gracefully stop the HTTP server."""
        if self.server is not None:
            self.server.should_exit = True
            self.thread.join(timeout=5)
            self.server = None
            self.thread = None
            