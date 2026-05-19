import json
import requests
from requests.adapters import HTTPAdapter


class Trigger:
    def __init__(self, address):
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
        response = self.session.post(self.address, json=body)
        response.raise_for_status()
        try:
            return response.json()
        except json.JSONDecodeError:
            raise Exception(f"Malformed response for {self.address}: \n {response.text} {'\n'*2} body: {body}")
