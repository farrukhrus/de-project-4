drop table if exists stg.deliverysystem_couriers ;
create table stg.deliverysystem_couriers (
	id serial primary key,
	object_value text NOT NULL
);

drop table if exists stg.deliverysystem_deliveries ;
create table stg.deliverysystem_deliveries (
	id serial primary key,
	object_value text NOT NULL
);