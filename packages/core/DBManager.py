from peewee import SqliteDatabase, Model, CharField, BooleanField, BigIntegerField, TextField, DateTimeField
from datetime import datetime
from pathlib import Path

class DBManager:
    def __init__(self):
        self._db_path = "./packages/databases/orker.db"
        self._check_db_exists()
        self._db = SqliteDatabase(
            self._db_path,
            pragmas={
                "journal_mode": "wal",
                "foreign_keys": 1,
                "synchronous": "normal",
            }
        )

        class BaseModel(Model):
            class Meta:
                database = self._db

        class Node(BaseModel):
            id = CharField(primary_key=True)
            ip = CharField(unique=True)
            running = BooleanField(default=False)
            available = BooleanField(default=False)
            secret = CharField()
            current_load = BigIntegerField(default=0)
            total_load = BigIntegerField(default=0)
        self.Node = Node

        class Routine(BaseModel):
            id = CharField(primary_key=True)
            node_id = CharField()
            status = TextField()
            started_at = DateTimeField(default=datetime.now)
            finished_at = DateTimeField(null=True)
            result = TextField(null=True)
        self.Routine = Routine


        self._db.connect()
        self._db.create_tables([self.Node])






    def _check_db_exists(self):
        if not Path(self._db_path).exists():
            with Path(self._db_path).open("w") as f:
                f.write("")


    # ----- NODE TABLE HELPERS -----
    def get_nodes_ips(self):
        return [node.ip for node in self.Node.select(self.Node.ip)]

    def get_node_by_id(self, id):
        return self.Node.get_or_none(self.Node.id == id)

    def get_node_by_ip(self, ip):
        return self.Node.get_or_none(self.Node.ip == ip)

    def check_secret(self, ip, id, secret):
        node = self.get_node_by_id(id)

        if node is None:
            return False

        return node.secret == secret and node.ip == ip

    def get_best_node_for_routine(self):
        return (
            self.Node
            .select()
            .where(self.Node.available)
            .order_by(self.Node.current_load.asc())
            .first()
        )

    def set_nodes_as_unavailable(self):
        self.Node.update(available=False).execute()

    def set_node_as_unavailable(self, id):
        self.Node.update(available=False).where(self.Node.id == id).execute()

    def set_node_as_available(self, id):
        self.Node.update(available=True).where(self.Node.id == id).execute()

    def connect_node(self, id, ip, secret):
        node, created = self.Node.get_or_create(
            id=id,
            defaults={
                "ip": ip,
                "running": True,
                "available": False,
                "secret": secret,
                "current_load": 0,
                "total_load": 0,
            }
        )

        if not created:
            node.ip = ip
            node.secret = secret
            node.running = True
            node.available = False
            node.save()

        return node

    def disconnect_node_with_id(self, id):
        self.Node.delete().where(self.Node.id == id).execute()

    def disconnect_node_with_ip(self, ip):
        self.Node.delete().where(self.Node.ip == ip).execute()

    def increase_load(self, id):
        self.Node.update(
            current_load=self.Node.current_load + 1,
            total_load=self.Node.total_load + 1,
        ).where(
            self.Node.id == id
        ).execute()

    def decrease_load(self, id):
        self.Node.update(
            current_load=self.Node.current_load - 1,
        ).where(
            self.Node.id == id
        ).execute()

    def remove_all_nodes(self):
        self.Node.delete().execute()



    # ----- ROUTINE TABLE HELPER -----
    def create_routine(self, id, node_id):
        return self.Routine.create(
            id=id,
            node_id=node_id,
            status="running"
        )

    def finish_routine(self, id, result):
        return (
            self.Routine
            .update(
                status="finished",
                finished_at=datetime.now(),
                result=result
            )
            .where(self.Routine.id == id)
            .execute()
        )
