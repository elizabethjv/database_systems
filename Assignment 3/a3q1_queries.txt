-- CSC 370 - Summer 2020
-- Assignment 3: Queries for Question 1 (imdb)
-- Name: Elizabeth Vellethara
-- Student ID: V00883616

-- Place your query for each sub-question in the appropriate position
-- below. Do not modify or remove the '-- Question 1x --' header before
-- each question.


-- Question 1a --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select primary_name, year, title_id
from
	titles
	natural join
	primary_names
	where year = 1989 and length_minutes = 180 and title_type = 'tvSpecial';
-- Question 1b --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select primary_name, year, length_minutes
from
	titles
	natural join
	primary_names
	where length_minutes >= 4320 and title_type = 'movie';
-- Question 1c --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select primary_name, year, length_minutes
from
	titles
	natural join
	primary_names
	natural join
	(select title_id from cast_crew natural join people where name = 'Meryl Streep') as T1
	where title_type = 'movie' and year <= 1985;
-- Question 1d --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select primary_name, year, length_minutes
from
	title_genres
	natural join
	primary_names 
	natural join
	(select title_id, year, length_minutes from titles natural join title_genres where title_type = 'movie' and genre = 'Film-Noir') as T1
	where genre = 'Action';
-- Question 1e --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select name from people natural join (select * from writers natural join (select title_id
from
	titles
	natural join
	primary_names 
	where title_type = 'movie' and primary_name = 'Die Hard') as T1
union
select * from directors natural join (select title_id
from
	titles
	natural join
	primary_names 
	where title_type = 'movie' and primary_name = 'Die Hard') as T1) as T2;
-- Question 1f --
with
primary_names as (select title_id, name as primary_name
				from title_names where is_primary = true)
select primary_name, year, length_minutes
from
	primary_names 
	natural join 
	titles 
	natural join 
	(select title_id from cast_crew natural join (select person_id from people where name = 'Meryl Streep') as T1
		intersect 
		select title_id from cast_crew natural join (select person_id from people where name = 'Tom Hanks') as T1) as T2 where title_type ='movie';