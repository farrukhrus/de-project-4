import psycopg2, psycopg2.extras
from logging import Logger
from lib import PgConnect
from datetime import datetime


class dm_courier_ledger:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg

    def load_courier_ledger(self, date_from: str, date_to: str):
        with self._db.client() as cur:
            load_date = datetime.fromisoformat(date_from)
            
            # есть что грузить?
            check = cur.execute(""" select count(*) from dds.dm_deliveries dd
                            where dd.order_ts > %(start_date)s and dd.order_ts < %(end_date)s"""
                        ,
                        {
                        "start_date": date_from,
                        "end_date": date_to
                    }).fetchall()
            
            # такой проверки явно не достаточно, но для учебного проект норм :) 
            if(len(check)==1):
                # вычищаем витрину от старых данных за период (если были)
                cur.execute("""
                    delete from cdm.dm_courier_ledger dd where dd.settlement_year=%(year)s and dd.settlement_month=%(month)s
                """, 
                    {
                        "year": load_date.year,
                        "month": load_date.month
                    },
                )
                
                cur.execute("""
                    insert into cdm.dm_courier_ledger (
                        courier_id, courier_name, settlement_year, settlement_month, orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
                    select t1.*,
                        t1.courier_order_sum + courier_tips_sum * 0.95 as courier_reward_sum
                    from
                        (select
                            courier_id,
                            courier_name,
                            settlement_year,
                            settlement_month,
                            orders_count,
                            orders_total_sum,
                            rate_avg,
                            order_processing_fee,
                            case
                                when (rate_cat = 1 and courier_order_sum_fact < 100) then 100
                                when (rate_cat = 2 and courier_order_sum_fact < 150) then 150
                                when (rate_cat = 3 and courier_order_sum_fact < 175) then 175
                                when (rate_cat = 4 and courier_order_sum_fact < 200) then 200
                            else 
                                courier_order_sum_fact
                            end as courier_order_sum,
                            courier_tips_sum
                        from (
                            select 
                                dc.courier_id, dc.courier_name, 
                                extract(year from to_date(dd.order_ts,'YYYY-MM-DD')) as settlement_year,
                                extract(month from to_date(dd.order_ts,'YYYY-MM-DD')) as settlement_month,
                                count(distinct dd.order_id) as orders_count,
                                sum(dd.sum) as orders_total_sum,
                                avg(dd.rate) as rate_avg,
                                sum(dd.sum)*0.25 as order_processing_fee,
                                case 
                                    when avg(dd.rate) < 4 then 1
                                    when avg(dd.rate) between 4 and 4.4 then 2
                                    when avg(dd.rate) between 4.5 and 4.8 then 3
                                    when avg(dd.rate) >= 4.9 then 4
                                end as rate_cat,
                                case 
                                    when avg(dd.rate) < 4 then sum(dd.sum)*0.05
                                    when avg(dd.rate) between 4 and 4.4 then sum(dd.sum)*0.07
                                    when avg(dd.rate) between 4.5 and 4.8 then sum(dd.sum)*0.08
                                    when avg(dd.rate) >= 4.9 then sum(dd.sum)*0.1
                                end as courier_order_sum_fact,
                                sum(dd.tip_sum) as courier_tips_sum
                            from dds.dm_deliveries dd inner join dds.dm_couriers dc on dd.courier_id = dc.courier_id
                            and dd.order_ts > %(start_date)s and dd.order_ts < %(end_date)s
                            group by dc.courier_id, dc.courier_name, settlement_year, settlement_month
                        ) t0 
                    ) t1""",
                        {
                            "start_date": date_from,
                            "end_date": date_to
                        },
                    )