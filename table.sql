create table uber_trips
(
	id serial not null,
	uuid uuid not null,
	request_at timestamptz not null,
	dropoff_at timestamptz not null,
	surge bool not null,
	distance float not null,
	duration float not null,
	status text not null,
	vehicle_type text not null,
	pickup_address text not null,
	dropoff_address text,
	custom_route_map text not null
);

create unique index uber_trips_dropoff_at_uindex
	on uber_trips (dropoff_at);

create unique index uber_trips_id_uindex
	on uber_trips (id);

create unique index uber_trips_request_at_uindex
	on uber_trips (request_at);

create unique index uber_trips_uuid_uindex
	on uber_trips (uuid);

alter table uber_trips
	add constraint uber_trips_pk
		primary key (id);

