import logging
from logging import Logger
from examples.stg import EtlSetting, StgEtlSettingsRepository
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
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log

    def load_couriers(self, **api):
        
        cohort = api["cohort"]
        x_api_key = api["x_api_key"]
        nickname = api["nickname"]
        
        with self.pg_dest.connection() as conn:
            limit = 50; offset = 0; items = []; initial = True; firstTime = False
            request = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=_id&sort_direction=asc& \
                            limit={limit}&offset={offset}"
            
            while(len(items) > 0 or initial == True):
                items = requests.get (
                    request.format(limit=limit, offset=offset),
                    headers= { "X-API-KEY": x_api_key, "X-Nickname": nickname, "X-Cohort": cohort }
                ).json()
                
                if(len(items) > 0):
                    wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
                    
                    if(not wf_setting and initial == True):
                        self.log.info("no wf_setting")
                        firstTime = True
                        wf_setting = EtlSetting(
                            id=0,
                            workflow_key=self.WF_KEY,
                            workflow_settings={
                                self.LAST_LOADED_ID: min([item["_id"] for item in items]) # каст строки в дату
                            }
                        )
                    last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID]
                    #print(f"last_loaded_id: {last_loaded_id}")

                    offset = offset + 50
                    for item in items:
                        if( (item["_id"] > last_loaded_id ) or (initial == True and firstTime == True)) :
                            self.log.info(f"firstTime {firstTime}")
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
                            initial = False
                            firstTime = False
                                
                    # Сохраняем прогресс.
                    last_loaded_id= max([item["_id"] for item in items])
                    wf_setting.workflow_settings[self.LAST_LOADED_ID] = last_loaded_id
                    wf_setting_json = json2str(wf_setting.workflow_settings) # Преобразуем к строке, чтобы положить в БД.
                    self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
                
            self.log.info(f"Last loaded ID: {wf_setting.workflow_settings[self.LAST_LOADED_ID]}")
        self.log.info("Done")

            