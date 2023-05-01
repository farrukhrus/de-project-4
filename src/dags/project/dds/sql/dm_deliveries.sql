CREATE TABLE dds.dm_deliveries (
	id serial4 NOT NULL,
	order_id varchar NOT NULL,
	order_ts varchar NOT NULL,
	delivery_id varchar NOT NULL,
	courier_id varchar NOT NULL,
	address varchar NOT NULL,
	delivery_ts timestamp NOT NULL,
	rate int4 NOT NULL,
	sum float8 NOT NULL,
	tip_sum float8 NOT NULL,
	CONSTRAINT dm_deliveries_check_rate CHECK (((rate >= 1) AND (rate <= 5))),
	CONSTRAINT dm_deliveries_check_sum CHECK ((sum > (0)::double precision)),
	CONSTRAINT dm_deliveries_check_tip_sum CHECK ((sum >= (0)::double precision)),
	CONSTRAINT dm_deliveries_pkey PRIMARY KEY (id)
);