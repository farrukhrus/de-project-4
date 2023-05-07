from airflow.decorators import dag, task
import pendulum
import logging
from lib import ConnectionBuilder
from examples.project.cdm.courier_ledger_cdm import dm_courier_ledger
log = logging.getLogger(__name__)

dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

@dag(
    # every 2nd day of month at 3 am
    schedule_interval='0 3 2 * *',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False, 
    tags=['sprint5', 'monthly', 'origin', 'project'],
    is_paused_upon_creation=True
)

def delivery_system_monthly_dag(p_date_from: str, p_date_to: str):
    @task(task_id="courier_ledger_load")
    def load_courier_ledger(date_from: str, date_to: str):
        rest_loader = dm_courier_ledger(pg=dwh_pg_connect)
        rest_loader.load_courier_ledger(date_from=date_from, date_to=date_to)
    
    load_courier_ledger=load_courier_ledger(date_from=p_date_from, date_to=p_date_to)
    
# per month
v_date_from = '2023-05-01 00:00:00'
v_date_to='2023-06-01 00:00:00'

delivery_system_dag = delivery_system_monthly_dag(p_date_to=v_date_to, p_date_from=v_date_from)
delivery_system_dag
print('Done')