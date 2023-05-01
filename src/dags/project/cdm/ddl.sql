drop table if exists cdm.dm_courier_ledger;
CREATE TABLE cdm.dm_courier_ledger (
	id serial4 NOT NULL,
	courier_id varchar NOT NULL,
	courier_name varchar NOT NULL,
	settlement_year int NOT NULL,
	settlement_month int4 NOT NULL,
	orders_count float8 NOT NULL,
	orders_total_sum float8 NOT NULL,
	rate_avg float8 NOT NULL,
	order_processing_fee float8 NOT NULL,
	courier_order_sum float8 NOT NULL,
	courier_tips_sum float8 NOT NULL,
	courier_reward_sum float8 NOT NULL,
	CONSTRAINT dm_courier_ledger_check_month CHECK (((settlement_month >= 1) AND (settlement_month <= 12))),
	CONSTRAINT dm_courier_ledger_pkey PRIMARY KEY (id)
);