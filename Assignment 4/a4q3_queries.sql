-- CSC 370 - Summer 2020
-- Assignment 4: Queries for Question 3 (vwsn_1year)
-- Name: Elizabeth Vellethara
-- Student ID: V00883616

-- Place your query for each sub-question in the appropriate position
-- below. Do not modify or remove the '-- Question 3x --' header before
-- each question.


-- Question 3a --
with 
	highest_temp as (
		select station_id as id, observation_time, temperature from observations where temperature = (select max(temperature) from observations))
select id as station_id, name, temperature, observation_time from highest_temp natural join stations;
-- Question 3b --
with
	max_temps as (
		select station_id , max(temperature) as temperature from observations where station_id >= 1 and station_id <=10 group by station_id),
	max_temps_with_obs_times as (
		select station_id, temperature , observation_time from max_temps natural join observations),
	station_id_1_10 as (
		select id as station_id, name from stations where id >= 1 and id <=10)
select station_id, name, temperature as max_temperature, observation_time from max_temps_with_obs_times natural join station_id_1_10;
-- Question 3c --
select id as station_id, name from stations natural join (select station_id as id from observations
except
select station_id as id from observations where extract(year from observation_time)=2020 and extract(month from observation_time)=1) as T1 order by station_id;
-- Question 3d --
with
	daily_average_temperature as (
		select year, month, day, avg(temperature) as temperature from (select station_id, temperature, extract(year from observation_time) as year, extract(month from observation_time) as month, extract(day from observation_time) as day from observations) as T1 group by year, month, day),
	ten_hottest_days as(
		select * from (select year, month, day, temperature, rank() over(partition by year, month order by temperature desc) as rankings from daily_average_temperature) as T1 where rankings<=10),
	ten_coldest_days as (
		select * from (select year, month, day, temperature, rank() over(partition by year, month order by temperature asc) as rankings from daily_average_temperature) as T1 where rankings<=10),
	hottest_10average as (
		select year, month, avg(temperature) as hottest_10average from ten_hottest_days group by year, month order by year, month),
	coldest_10average as (
		select year, month, avg(temperature) as coldest_10average from ten_coldest_days group by year, month order by year, month)	
select * from hottest_10average natural join coldest_10average;
-- Question 3e --
with
	daily_average_temperature as (
		select year, month, day, avg(temperature) as temperature from (select station_id, temperature, extract(year from observation_time) as year, extract(month from observation_time) as month, extract(day from observation_time) as day from observations) as T1 group by year, month, day),
	avg_with_min_temps as (
		select year, month, day, temperature, min(temperature) over(order by year, month rows between 28 preceding and 1 preceding) as mintemp from daily_average_temperature where (year=2019 and month=5 and day>=29)or (year=2019 and month <> 5 and month <> 6) or (year=2020))
select year, month, day from avg_with_min_temps where temperature<mintemp;