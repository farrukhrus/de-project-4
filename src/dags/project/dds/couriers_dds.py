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

    def list_dm_couriers(self, dm_courier_threshold: int, limit: int) -> List[dm_courierObj]:
        with self._db.client().cursor(row_factory=class_row(dm_courierObj)) as cur:
            cur.execute(
                """
                select  dc.id, 
                        dc.object_value::json->>'_id' as courier_id,
                        dc.object_value::json->>'name' as courier_name
                from stg.deliverysystem_couriers dc 
                order by id asc;
                """, {
                    "threshold": dm_courier_threshold
                }
            )
            objs = cur.fetchall()
        print(objs)
        return objs


class dm_courierDestRepository:
    def insert_dm_courier(self, conn: Connection, dm_courier: dm_courierObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_couriers (id, courier_id, courier_name)
                    VALUES (%(id)s,%(courier_id)s, %(courier_name)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        courier_id = EXCLUDED.courier_id,
                        courier_name = EXCLUDED.courier_name;
                """,
                {
                    "id": dm_courier.id,
                    "courier_id": dm_courier.courier_id,
                    "courier_name": dm_courier.courier_name
                },
            )
            
class dm_courierLoader:
    WF_KEY = "example_dm_couriers_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000 

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_couriersOriginRepository(pg_dest)
        self.dds = dm_courierDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_couriers(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_couriers(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_couriers to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_courier in load_queue:
                self.dds.insert_dm_courier(conn, dm_courier)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
