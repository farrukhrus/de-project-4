drop table if exists stg.deliverysystem_couriers ;
create table stg.deliverysystem_couriers (
	id serial primary key,
	courier_id varchar,
	object_value text NOT NULL
);

CREATE TABLE stg.deliverysystem_deliveries (
	id serial4 NOT NULL,
	order_ts varchar NULL,
	object_value text NOT NULL,
	CONSTRAINT deliverysystem_deliveries_pkey PRIMARY KEY (id)
);