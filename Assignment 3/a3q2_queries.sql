-- CSC 370 - Summer 2020
-- Assignment 3: Queries for Question 2 (ferries)
-- Name: Elizabeth Vellethara
-- Student ID: V00883616

-- Place your query for each sub-question in the appropriate position
-- below. Do not modify or remove the '-- Question 2x --' header before
-- each question.


-- Question 2a --
select route_number, count(source_port) as num_sailings from sailings group by route_number
union
select route_number, 0 as num_sailings from (select distinct route_number from routes except select distinct route_number from sailings) as T1;
-- Question 2b --
SELECT vessel_name, count(source_port) as count from sailings group by vessel_name;
-- Question 2c --
select route_number, count(distinct vessel_name) as num_vessels from sailings group by route_number having count(distinct vessel_name)>=2;
-- Question 2d --
select distinct route_number, vessel_name, year_built 
from 
	(select distinct route_number, vessel_name, year_built 
	from sailings natural join (select vessel_name, min(year_built) as year_built from fleet group by vessel_name) as T1) as T6 
	natural join 
	(select distinct route_number, min(year_built) as year_built 
	from (select distinct route_number, vessel_name, year_built from sailings natural join (select vessel_name, min(year_built) as year_built from fleet group by vessel_name) as T1) as T2 group by route_number) as T3;
-- Question 2e --
select distinct vessel_name from sailings natural join (select source_port from sailings where vessel_name = 'Coastal Renaissance') as T1;
-- Question 2f --
select route_number, num_vessels from (select max(num_vessels) as num_vessels from (select route_number, count (distinct vessel_name) as num_vessels from sailings group by route_number) as T1) as T2 natural join (select route_number, count (distinct vessel_name) as num_vessels from sailings group by route_number) as T3; 
-- Question 2g --
select distinct source_port as port, route_number, sailings from sailings natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1 union 
select distinct destination_port as port, route_number, sailings from sailings natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1; 
-- Question 2h --
select port, route_number, sailings 
from 
	(select distinct source_port as port, route_number, sailings 
	from 
		sailings 
		natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1 
	union 
	select distinct destination_port as port, route_number, sailings 
	from 
		sailings 
		natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1) as T2 
	natural join 
	(select port, max(sailings) as sailings 
	from 
		(select distinct source_port as port, route_number, sailings
		from 
		sailings 
		natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1 
		union 
		select distinct destination_port as port, route_number, sailings from sailings natural join (select route_number, count(vessel_name) as sailings from sailings group by route_number) as T1
	) as T2 group by port
) as T3;