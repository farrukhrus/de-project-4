from airflow.decorators import dag, task
import pendulum
import logging
from lib import ConnectionBuilder

from examples.project.stg.deliveries_loader import deliveryLoader
from examples.project.stg.couriers_loader import courierLoader

from examples.project.dds.couriers_dds import dm_courierLoader
from examples.project.dds.deliveries_dds import dm_deliverieLoader
from examples.project.cdm.courier_ledger_cdm import dm_courier_ledger
log = logging.getLogger(__name__)

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

@dag(
    # every day at 2 am
    schedule_interval='0 2 * * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False, 
    tags=['sprint5', 'daily', 'origin', 'project'],
    is_paused_upon_creation=True
)

def delivery_system_daily_dag(p_date_from: str, p_date_to: str):
    api = { 'cohort' : "11", 'x_api_key': "25c27781-8fde-4b30-a22e-524044a7580f", 'nickname': "frustamov"}

    @task(task_id="deliveries_load")
    def deliveries_load(p_date_from: str, p_date_to: str):
        devl = deliveryLoader(dwh_pg_connect, log, date_from=p_date_from, date_to=p_date_to)
        devl.load_deliveries(**api)
    
    ## предполагаю, что таблица должна перезагружаться полностью, т.к. в api не за что зацепиться для инкрементальной загрузки
    ## stg/couriers_loader_nowf.py - тут пробовал зацепиться за _id, но похоже, что вариант не рабочий
    @task(task_id="couriers_load")
    def couriers_load():
        devl = courierLoader(dwh_pg_connect, log)
        devl.load_couriers(**api)

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        rest_loader = dm_courierLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_couriers()
    
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        rest_loader = dm_deliverieLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_deliveries()
    
    deliveries_load = deliveries_load(p_date_to=p_date_to,p_date_from=p_date_from)
    couriers_load = couriers_load()
    load_dm_couriers = load_dm_couriers()
    load_dm_deliveries = load_dm_deliveries()
    
    [couriers_load, deliveries_load] >> load_dm_couriers >> load_dm_deliveries

# per day
v_date_from = '2023-05-03%2000:00:00'
v_date_to='2023-05-04%2000:00:00'

delivery_system_dag = delivery_system_daily_dag(p_date_to=v_date_to, p_date_from=v_date_from)
delivery_system_dag
print('Done')