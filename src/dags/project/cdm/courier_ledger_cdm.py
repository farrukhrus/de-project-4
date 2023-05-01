import psycopg2, psycopg2.extras

class dm_courier_ledger:
    def load_courier_ledger():
        conn = psycopg2.connect("host='localhost' port='5432' dbname='de' user='jovyan' password='jovyan'")
        cur = conn.cursor()
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
                    group by dc.courier_id, dc.courier_name, settlement_year, settlement_month
                ) t0 
            ) t1""")
        conn.commit()
        cur.close()
        conn.close()
