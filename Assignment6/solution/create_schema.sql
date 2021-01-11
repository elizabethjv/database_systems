-- create_schema.sql
-- Elizabeth Vellethara - 08/09/2020
-- CSC 370 - Summer 2020
-- Assignment 6
-- Name: Elizabeth Vellethara
-- Student ID: V00883616

-- Issue a pre-emptive rollback (to discard the effect of any active transaction) --
rollback;

drop table if exists reservations;
drop table if exists passengers;
drop table if exists flights;
drop table if exists aircrafts;
drop table if exists airports;

drop function if exists insert_flight_constraint_trigger();
drop function if exists multiple_reservations_constraint_trigger();
drop function if exists reservations_name_constraint_trigger();
drop function if exists reservations_constraint_trigger();
drop function if exists aircraft2_constraint_trigger();
drop function if exists aircraft1_constraint_trigger();
drop function if exists airline_constraint_trigger();
drop function if exists international_constraint_trigger();
drop function if exists iata_code_constraint_trigger();

create table airports(
	iata_code varchar(3) primary key,
	airport_name varchar(255),
	country varchar(255),
	international_airport_status varchar(10) not null,
	check( length(iata_code) > 0 ),
	check( length(airport_name) > 0 ),
	check( length(country) > 0 )
	);

create table aircrafts(
	aircraft_id varchar(64) primary key,
	airline varchar(255),
	model varchar(255),
	passenger_capacity integer,
	check( length(aircraft_id) > 0 ),
	check( length(airline) > 0 ),
	check( length(model) > 0 ),
	check( passenger_capacity >=0 )
	);
	
create table flights(
	flight_id integer primary key,
	source_iata_code varchar(255),
	destination_iata_code varchar(255),
	aircraft_id varchar(64),
	airline varchar(255),
	departure timestamp(6),
	arrival timestamp(6),
	check (flight_id > 0),
	check (source_iata_code <> destination_iata_code),
	check (departure < arrival),
	foreign key (source_iata_code) references airports(iata_code) 
		on delete cascade 
		on update cascade 
		deferrable,
	foreign key (destination_iata_code) references airports(iata_code) 
		on delete cascade
		on update cascade 
		deferrable,
	foreign key (aircraft_id) references aircrafts(aircraft_id) 
		on delete cascade
		on update cascade 
		deferrable
	);

create table passengers(
	passenger_id integer,
	name varchar(1000),
	primary key (passenger_id),
	check (passenger_id > 0),
	check(length(name) > 0)
	);

create table reservations(
	flight_id integer,
	passenger_id integer,
	primary key (passenger_id, flight_id),
	foreign key (passenger_id) references passengers(passenger_id)
		on delete cascade
		on update cascade 
		deferrable,
	foreign key (flight_id) references flights(flight_id)
		on delete restrict 
		on update cascade 
		deferrable,
	check (passenger_id > 0),
	check(flight_id > 0)
	);

create function iata_code_constraint_trigger()
returns trigger as
$BODY$
begin
if new.iata_code <> upper(new.iata_code) or length(new.iata_code) <> 3
then 
	raise exception 'IATA code must be exactly three characters long and contain only contain uppercase letters';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger iata_code_constraint
	after insert or update on airports
	deferrable
	for each row 
	execute procedure iata_code_constraint_trigger();

create function international_constraint_trigger()
returns trigger as
$BODY$
begin
if (select country from airports where iata_code = new.source_iata_code) <> (select country from airports where iata_code = new.destination_iata_code) and ((select international_airport_status from airports where iata_code = new.source_iata_code) = 'no' or (select international_airport_status from airports where iata_code = new.destination_iata_code) = 'no' ) 
then 
	raise exception 'Either the source or destination flight or both is/are not an international airport';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger international_constraint
	after insert or update on flights
	deferrable	
	for each row 
	execute procedure international_constraint_trigger();

create function airline_constraint_trigger()
returns trigger as
$BODY$
begin
if new.airline <> (select airline from aircrafts where aircraft_id = new.aircraft_id) 
then 
	raise exception 'This airline does not this use this model of aircraft';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger airline_constraint
	after insert or update on flights 
	deferrable
	for each row 
	execute procedure airline_constraint_trigger();

create function aircraft1_constraint_trigger()
returns trigger as
$BODY$
begin
if (select count(*)  as sixty_min_gap from  flights where aircraft_id= new.aircraft_id and flight_id <> new.flight_id and arrival < new.departure and ((extract(epoch from new.departure) - extract(epoch from arrival))/60) <60) > 0
then 
	raise exception 'The aircraft is not on the ground for 60 minutes for it to be used again';
elsif (select count(*)  as sixty_min_gap from  flights where aircraft_id= new.aircraft_id and flight_id <> new.flight_id and arrival < new.departure and ((extract(epoch from new.departure) - extract(epoch from arrival))/60) <60) > 0
	and
	(select destination_iata_code from flights natural join (select max(arrival) as arrival, aircraft_id from flights where aircraft_id = new.aircraft_id and flight_id <> new.flight_id and arrival < new.departure and (extract(epoch from new.departure) - extract(epoch from arrival))/60>=60 group by aircraft_id) as T1) <> new.source_iata_code
	then
	raise exception 'The aircraft is not in the desired airport';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger aircraft1_constraint
	after  insert or update on flights
       deferrable	
	for each row 
	execute procedure aircraft1_constraint_trigger();

create function aircraft2_constraint_trigger()
returns trigger as
$BODY$
begin
if  (select count(*)  as sixty_min_gap from flights where aircraft_id = new.aircraft_id and flight_id <> new.flight_id and departure > new.arrival and ((extract(epoch from departure) - extract(epoch from new.arrival))/60) <60) > 0
then 
	raise exception 'The aircraft is not on the ground for 60 minutes only after which it can be used again';
elsif (select count(*)  as sixty_min_gap from flights where aircraft_id = new.aircraft_id and flight_id <> new.flight_id and departure > new.arrival and ((extract(epoch from departure) - extract(epoch from new.arrival))/60) <60) > 0
	and
	(select source_iata_code from flights natural join (select min(departure) as departure, aircraft_id  from flights where aircraft_id = new.aircraft_id and flight_id <> new.flight_id and departure > new.arrival  and (extract(epoch from departure) - extract(epoch from new.arrival))/60>=60 group by aircraft_id) as T1) <> new.destination_iata_code
then
	raise exception 'The aircraft is not in the desired airport';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger aircraft2_constraint
	after insert or update on flights  
	deferrable
	for each row 
	execute procedure aircraft2_constraint_trigger();

create function reservations_constraint_trigger()
returns trigger as
$BODY$
begin
if  (select count from (select flight_id, count(*) from reservations group by flight_id) as T1 where flight_id = new.flight_id) > (select passenger_capacity from aircrafts natural join (select aircraft_id, flight_id from flights where flight_id=new.flight_id)as T1)
then 
	raise exception 'Reservation is full';
end if;
return new;
end
$BODY$
language plpgsql;

create constraint trigger reservations_constraint
	after insert or update on reservations
	for each row 
	execute procedure reservations_constraint_trigger();

create function reservations_name_constraint_trigger()
returns trigger as
$BODY$
begin
if (select name from reservations natural join passengers where passenger_id=new.passenger_id) <> new.name or (select passenger_id from reservations natural join passengers where name=new.name) <> new.passenger_id
then 
	raise exception 'The name or passenger_id does not match with the name in the system';
	return NULL;
end if;
return new;
end
$BODY$
language plpgsql;

create trigger reservations_name_constraint
	before insert or update on passengers 
	for each row 
	execute procedure reservations_name_constraint_trigger();

create function multiple_reservations_constraint_trigger()
returns trigger as 
$BODY$
begin
if  (select count(*) from  passengers where passenger_id = new.passenger_id and name = new.name) > 0
then
	return NULL;
end if;
return new;
end
$BODY$
language plpgsql;

create trigger multiple_reservations_constraint
	before insert on passengers
	for each row
	execute procedure multiple_reservations_constraint_trigger();

create function insert_flight_constraint_trigger()
returns trigger as
$BODY$
begin
if (select count(*) from flights where flight_id = new.flight_id group by flight_id) <> 0
then 
	raise exception 'A flight with given flight_id already exists';
	return NULL;
end if;
return new;
end
$BODY$
language plpgsql;

create trigger insert_flight_constraint
	before insert on flights
	for each row 
	execute procedure insert_flight_constraint_trigger();
