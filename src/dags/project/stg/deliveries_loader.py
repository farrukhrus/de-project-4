import logging
from logging import Logger
from examples.stg import EtlSetting, StgEtlSettingsRepository
from lib import PgConnect
from lib.dict_util import json2str
from datetime import datetime
import requests

log = logging.getLogger(__name__)
class deliveryLoader:
    WF_KEY = "prj_deliveries_to_stg_workflow"
    LAST_LOADED_TS_KEY = "last_loaded_ts"

    def __init__(self, pg_dest: PgConnect, log: Logger, date_from: str, date_to: str) -> None:
        self.pg_dest = pg_dest
        self.settings_repository = StgEtlSettingsRepository()
        self.log = log
        self.date_from = date_from
        self.date_to = date_to

    def load_deliveries(self, **api):
        
        cohort = api["cohort"]
        x_api_key = api["x_api_key"]
        nickname = api["nickname"]
        
        with self.pg_dest.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(
                    id=0,
                    workflow_key=self.WF_KEY,
                    workflow_settings={
                        self.LAST_LOADED_TS_KEY: datetime(2022, 1, 1).isoformat() # каст строки в дату
                    }
                )

            last_loaded_str = wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]
            last_loaded_ts = datetime.fromisoformat(last_loaded_str)
            limit = 50; offset = 0; items = []; initial = True

            request = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=date&sort_direction=asc& \
                            limit={limit}&offset={offset}&from={date_from}&to={date_to}"
            while(len(items) > 0 or initial == True):
                items = requests.get (
                    request.format(limit=limit, offset=offset, date_from=self.date_from, date_to=self.date_to),
                    headers= { "X-API-KEY": x_api_key, "X-Nickname": nickname, "X-Cohort": cohort }
                ).json()
                
                if(len(items) > 0):
                    initial = False
                    offset = offset + 50
                    minorderts = datetime.fromisoformat(min([item["order_ts"] for item in items]) )

                    if( minorderts > last_loaded_ts):
                        for item in items:
                            with conn.cursor() as cur:
                                cur.execute(
                                    """
                                        INSERT INTO stg.deliverysystem_deliveries(order_ts, object_value)
                                        VALUES (%(order_ts)s, %(object_value)s)
                                        ON CONFLICT (id) DO UPDATE
                                        SET
                                            order_ts = EXCLUDED.order_ts,
                                            object_value = EXCLUDED.object_value;
                                    """,
                                    {
                                        "order_ts": item["order_ts"],
                                        "object_value": str(item).replace('\'','"')
                                    },
                                )
                                
                        # Сохраняем прогресс.
                        last_loaded_ts_new = max([item["order_ts"] for item in items])
                        wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY] = last_loaded_ts_new
                        wf_setting_json = json2str(wf_setting.workflow_settings) # Преобразуем к строке, чтобы положить в БД.
                        self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)
                        last_loaded_ts = datetime.fromisoformat(last_loaded_ts_new)
                
            self.log.info(f"Last loaded date: {wf_setting.workflow_settings[self.LAST_LOADED_TS_KEY]}")
        self.log.info("Done")

            