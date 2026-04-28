from packages.core.DBManager import DBManager
from uuid import uuid4


class RoutineManager:
    def __init__(self):
        self._routine_queue = []

    def handler(self, payload, endpoint):
        routine = {
            'endpoint': endpoint,
            'payload': payload,
            'id': uuid4(),
            'status': "waiting"
        }
        self._routine_queue.append(routine)
        return {"status": "ok", "routine": "Added to queue"}

    def run_routine(self, node, db_manager: DBManager):
        routine = self._routine_queue.pop(0)
        db_manager.create_routine(routine["id"], node.id)
        db_manager.increase_load(node.id)

    def get_queued_routine_amount(self):
        return len(self._routine_queue)
