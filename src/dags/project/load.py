from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.empty import EmptyOperator
import pendulum
import logging
from lib import ConnectionBuilder
import requests
import psycopg2

from examples.project.dds.couriers_dds import dm_courierLoader
from examples.project.dds.deliveries_dds import dm_deliverieLoader
from examples.project.cdm.courier_ledger_cdm import dm_courier_ledger
log = logging.getLogger(__name__)

cohort = '11'
x_api_key = '25c27781-8fde-4b30-a22e-524044a7580f'
nickname = 'frustamov'
dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

def get_data(insert: str, request: str):
    conn = psycopg2.connect("host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
    cur = conn.cursor()
    limit = 50; offset = 0; items = []; initial = True
    date_from = '2022-04-16%2000:00:00'; 
    date_to='2023-04-22%2000:00:00'

    while(len(items) > 0 or initial == True):
        items = requests.get (
            request.format(limit=limit, offset=offset, date_from=date_from, date_to=date_to),
            headers= { "X-API-KEY": x_api_key, "X-Nickname": nickname, "X-Cohort": cohort }
        ).json()
        
        initial = False
        offset = offset + 50 

        for item in items:
            cur.execute( insert.replace('{s_val}',(str(item)).replace('\'','"'))  )
        conn.commit()
    print('done')
    cur.close()

@dag(
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False, 
    tags=['sprint5', 'stg', 'origin', 'project'],
    is_paused_upon_creation=True
)

def stg_delivery_system_dag():
    @task(task_id="couriers_load")
    def couriers():
        insert_couriers = "insert into stg.deliverysystem_couriers (object_value) VALUES (\'{s_val}\');"
        request_couriers = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field=_id&sort_direction=desc&limit={limit}&offset={offset}"
        get_data(insert=insert_couriers, request=request_couriers)

    @task(task_id="deliveries_load")
    def deliveries():
        insert_deliveries = "insert into stg.deliverysystem_deliveries (object_value) VALUES (\'{s_val}\');"
        request_deliveries = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field=_id&sort_direction=desc&limit={limit}&offset={offset}&from={date_from}&to={date_to}"
        get_data(insert=insert_deliveries, request=request_deliveries)

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        rest_loader = dm_courierLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_couriers()
    
    @task(task_id="dm_deliveries_load")
    def load_dm_deliveries():
        rest_loader = dm_deliverieLoader(origin_pg_connect, dwh_pg_connect, log)
        rest_loader.load_dm_deliveries()
    
    @task(task_id="courier_ledger_load")
    def load_courier_ledger():
        rest_loader = dm_courier_ledger
        rest_loader.load_courier_ledger()
    
    couriers = couriers()
    deliveries = deliveries()
    load_dm_couriers = load_dm_couriers()
    load_dm_deliveries = load_dm_deliveries()
    load_courier_ledger=load_courier_ledger()
    
    [couriers, deliveries] >> load_dm_couriers >> load_dm_deliveries >> load_courier_ledger

stg_delivery_system_dag = stg_delivery_system_dag()
stg_delivery_system_dag
print('Done')