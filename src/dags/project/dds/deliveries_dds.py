from logging import Logger
from typing import List

from examples.dds.dds_settings_repository import EtlSetting, DdsEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str, str2json
from psycopg import Connection
from datetime import datetime
from psycopg.rows import class_row
from pydantic import BaseModel


class dm_deliverieObj(BaseModel):
    id: int
    order_id: str
    order_ts: str
    delivery_id: str
    courier_id: str
    address: str
    delivery_ts: datetime
    rate: float
    sum: float
    tip_sum: float
     
class dm_deliveriesOriginRepository:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def list_dm_deliveries(self, dm_deliverie_threshold: int, limit: int) -> List[dm_deliverieObj]:
        with self._db.client().cursor(row_factory=class_row(dm_deliverieObj)) as cur:
            cur.execute(
                """
                select  dc.id, 
                        dc.object_value::json->>'order_id' as order_id,
                        dc.object_value::json->>'order_ts' as order_ts,
                        dc.object_value::json->>'delivery_id' as delivery_id,
                        dc.object_value::json->>'courier_id' as courier_id,
                        dc.object_value::json->>'address' as address,
                        dc.object_value::json->>'delivery_ts' as delivery_ts,
                        dc.object_value::json->>'rate' as rate,
                        dc.object_value::json->>'sum' as "sum",
                        dc.object_value::json->>'tip_sum' as tip_sum
                from stg.deliverysystem_deliveries dc
                order by id asc;
                """, {
                    "threshold": dm_deliverie_threshold
                }
            )
            objs = cur.fetchall()
        print(objs)
        return objs


class dm_deliverieDestRepository:

    def insert_dm_deliverie(self, conn: Connection, dm_deliverie: dm_deliverieObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.dm_deliveries (id, order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum)
                    VALUES (%(id)s,%(order_id)s, %(order_ts)s, %(delivery_id)s, %(courier_id)s, %(address)s, %(delivery_ts)s, %(rate)s, %(sum)s, %(tip_sum)s)
                    ON CONFLICT (id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        order_ts = EXCLUDED.order_ts,
                        delivery_id = EXCLUDED.delivery_id,
                        courier_id = EXCLUDED.courier_id,
                        address = EXCLUDED.address,
                        delivery_ts = EXCLUDED.delivery_ts,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        sum = EXCLUDED.sum;
                """,
                {
                    "id": dm_deliverie.id,
                    "order_id": dm_deliverie.order_id,
                    "order_ts": dm_deliverie.order_ts,
                    "delivery_id": dm_deliverie.delivery_id,
                    "courier_id": dm_deliverie.courier_id,
                    "address": dm_deliverie.address,
                    "delivery_ts": dm_deliverie.delivery_ts,
                    "rate": dm_deliverie.rate,
                    "sum": dm_deliverie.sum,
                    "tip_sum": dm_deliverie.tip_sum
                },
            )
            


class dm_deliverieLoader:
    WF_KEY = "example_dm_deliveries_origin_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"
    BATCH_LIMIT = 1000000 

    def __init__(self, pg_origin: PgConnect, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.origin = dm_deliveriesOriginRepository(pg_dest)
        self.dds = dm_deliverieDestRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def load_dm_deliveries(self):
        with self.pg_dest.connection() as conn:

            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]
            load_queue = self.origin.list_dm_deliveries(last_loaded, self.BATCH_LIMIT)
            self.log.info(f"Found {len(load_queue)} dm_deliveries to load.")
            if not load_queue:
                self.log.info("Quitting.")
                return

            for dm_deliverie in load_queue:
                self.dds.insert_dm_deliverie(conn, dm_deliverie)

            wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = max([t.id for t in load_queue])
            wf_setting_json = json2str(wf_setting.workflow_settings)
            self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

            self.log.info(f"Load finished on {wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]}")
