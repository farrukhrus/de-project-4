from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from datetime import datetime
from psycopg.rows import class_row
from pydantic import BaseModel


class dm_courierObj(BaseModel):
    id: int
    courier_id: str
    courier_name: str
    
class dm_couriersOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_couriers(self) -> List[dm_courierObj]:
        with self._db.client().cursor(row_factory=class_row(dm_courierObj)) as cur:
            cur.execute(
                """
                select  dc.id, 
                        dc.object_value::json->>'_id' as courier_id,
                        dc.object_value::json->>'name' as courier_name
                from stg.deliverysystem_couriers dc
                order by id asc;
                """
            )
            objs = cur.fetchall()
        #print(objs)
        return objs


class dm_courierDestRepository:
    def insert_dm_courier(self, conn: Connection, dm_courier: dm_courierObj) -> None:
        with conn.cursor() as cur:
            # типа MERGE
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers (courier_id, courier_name)
                    VALUES (%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (courier_id) DO UPDATE
                    SET
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "courier_id": dm_courier.courier_id,
                    "courier_name": dm_courier.courier_name
                },
            )
            
class dm_courierLoader:
    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_couriersOriginRepository(pg_dest)
        self.dds = dm_courierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log


    def load_dm_couriers(self):
        with self.pg_dest.connection() as conn:
            load_queue = self.origin.list_dm_couriers()
            self.log.info(f"Found {len(load_queue)} dm_couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_courier in load_queue:
                self.dds.insert_dm_courier(conn, dm_courier)

            self.log.info("done")
