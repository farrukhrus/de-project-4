import logging
from logging import Logger
from lib import PgConnect
from lib.dict_util import json2str
from datetime import datetime
import requests

log = logging.getLogger(__name__)
class courierLoader:
    WF_KEY = "prj_couriers_to_stg_workflow"
    LAST_LOADED_ID = "last_loaded_ID"

    def __init__(self, pg_dest: PgConnect, log: Logger) -> None:
        self.pg_dest = pg_dest
        self.log = log

    def load_couriers(self, **api):
        
        cohort = api["cohort"]
        x_api_key = api["x_api_key"]
        nickname = api["nickname"]
        
        with self.pg_dest.connection() as conn:
            limit = 50; offset = 0; items = []; initial = True
            request = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=_id&sort_direction=asc& \
                            limit={limit}&offset={offset}"
            
            while(len(items) > 0 or initial == True):
                items = requests.get (
                    request.format(limit=limit, offset=offset),
                    headers= { "X-API-KEY": x_api_key, "X-Nickname": nickname, "X-Cohort": cohort }
                ).json()
                
                if(len(items) > 0):
                    
                    if(initial == True):
                        with conn.cursor() as cur:
                                cur.execute("truncate table stg.deliverysystem_couriers;")
                        initial = False

                    offset = offset + 50
                    for item in items:
                        with conn.cursor() as cur:
                            cur.execute(
                                """
                                    INSERT INTO stg.deliverysystem_couriers(courier_id, object_value)
                                    VALUES (%(courier_id)s,%(object_value)s)
                                    ON CONFLICT (id) DO UPDATE
                                    SET
                                        courier_id = EXCLUDED.courier_id,
                                        object_value = EXCLUDED.object_value;
                                """,
                                {
                                    "courier_id": item["_id"],
                                    "object_value": str(item).replace('\'','"')
                                },
                            )
                                
        self.log.info("Done")

            