create table trips
(
	id serial not null,
	uuid uuid not null,
	total float not null,
	request_at timestamptz not null,
	dropoff_at timestamptz not null,
	surge bool not null,
	distance float not null,
	duration float not null,
	pickup_lat float,
	pickup_long float,
	dropoff_lat float,
	dropoff_long float,
	status text not null,
	vehicle_type text not null,
	pickup_address text not null,
	dropoff_address text,
	custom_route_map text not null
);

create unique index trips_dropoff_at_uindex
	on trips (dropoff_at);

create unique index trips_id_uindex
	on trips (id);

create unique index trips_request_at_uindex
	on trips (request_at);

create unique index trips_uuid_uindex
	on trips (uuid);

alter table trips
	add constraint trips_pk
		primary key (id);

