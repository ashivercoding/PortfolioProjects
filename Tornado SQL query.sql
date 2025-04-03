-- Use bigquery wildcard feature to combined all storm data sets into one. ***Note: after looking through the data more, I noticed that the storms datasets contain multiple storm types, not just tornadoes. This initial table should have been called something like 'storm_data_combined' but will be left as is. 

CREATE TABLE `helpful-cipher-450917-h6.noaa.tornado_data_combined` AS
SELECT *
FROM `bigquery-public-data.noaa_historic_severe_storms.storms_*` 
ORDER BY event_begin_time;

-- Create a copy of the table to perform exploratory data analysis on tornado data only

CREATE TABLE `helpful-cipher-450917-h6.noaa.tornado_data_condensed` AS
SELECT episode_id, event_id, state, event_begin_time, event_end_time, injuries_direct, injuries_indirect, deaths_direct, deaths_indirect, damage_property, damage_crops, tor_f_scale, tor_length, tor_width
FROM `helpful-cipher-450917-h6.noaa.tornado_data_combined`
WHERE event_type = 'tornado'
ORDER BY event_begin_time;

-- Observe total tornado impact in the US

SELECT COUNT(event_id) AS total_tornadoes, (SUM(injuries_direct) + SUM(injuries_indirect)) AS total_injuries, (SUM(deaths_direct) + SUM(deaths_indirect)) AS total_deaths, SUM(damage_property)  AS total_property_damage, SUM(damage_crops) AS total_crops_damage
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed`;

-- Create new table. Use join to match state fips code with state full name. The MERGE function would be better suited for accomplishing this task but is not available with the free version of BigQuery. MERGE would not require creating a new table. 

CREATE TABLE `helpful-cipher-450917-h6.noaa.tornado_data_condensed2` AS
SELECT fip.state_name, tor.event_id, tor.event_begin_time, tor.event_end_time, (tor.injuries_direct + tor.injuries_indirect) AS total_injuries, (tor.deaths_direct + tor.deaths_indirect) AS total_deaths, damage_property, damage_crops, tor_f_scale, tor_length, tor_width
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed` as tor
JOIN `bigquery-public-data.census_utility.fips_codes_states` as fip
ON tor.state_fips_code = fip.state_fips_code;

-- Observe tornado impact per state

SELECT state_name, COUNT(state_name) AS total_tornadoes, SUM(total_injuries) AS injuries_per_state, SUM(total_deaths) AS deaths_per_state, SUM(damage_property) AS dmg_property_per_state, SUM(damage_crops) AS dmg_crops_per_state
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed2`
GROUP BY state_name 
ORDER BY total_tornadoes DESC;

-- Observe tornado event data for Georgia
SELECT state_name, event_begin_time, total_injuries, total_deaths, damage_property, damage_crops, tor_f_scale, tor_length, tor_width
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed2`
WHERE state_name = 'Georgia'
ORDER BY event_begin_time;

-- Observe overall tornado data for Georgia
SELECT SUM(total_injuries) AS total_injuries, SUM(total_deaths) AS total_deaths, SUM(damage_property) AS total_property_damage, SUM(damage_crops) AS total_crop_damage, MAX(tor_length) AS max_tor_length, MIN(tor_length) AS min_tor_length, MAX(tor_width) AS max_tor_width, MIN(tor_width) AS min_tor_width
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed2`
WHERE state_name = 'Georgia';

-- Which tornado episode was the deadliest?
SELECT episode_id, event_begin_time, COUNT(state) AS number_of_states_affected, SUM(deaths_direct + deaths_indirect) as tot_deaths, COUNT(event_begin_time) as tor_count
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed`
WHERE episode_id IS NOT NULL
GROUP BY episode_id, event_begin_time
ORDER BY tot_deaths DESC;

-- Which day was the deadliest?
SELECT EXTRACT(DATE FROM event_begin_time) AS day, COUNT(state) AS number_of_states_affected, SUM(deaths_direct + deaths_indirect) as tot_deaths, COUNT(event_begin_time) as tor_count, ROUND(AVG(CAST(tor_length AS FLOAT64)),2) AS avg_tor_length, ROUND(AVG(CAST(tor_width AS FLOAT64)),2) AS avg_tor_width
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed`
WHERE episode_id IS NOT NULL
GROUP BY day
ORDER BY tot_deaths DESC;


-- Which 5 states had the most tornadoes every 10 years?
-- First, assign a number to each row based on the 10-year range it falls into and create new table
CREATE TABLE `helpful-cipher-450917-h6.noaa.10_year_interval_table` AS 
SELECT state_name, EXTRACT(YEAR FROM event_begin_time) as event_year,
CASE 
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 1950 AND 1959 THEN 1
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 1960 AND 1969 THEN 2
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 1970 AND 1979 THEN 3
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 1980 AND 1989 THEN 4
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 1990 AND 1999 THEN 5
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 2000 AND 2009 THEN 6
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 2010 AND 2019 THEN 7
WHEN CAST(EXTRACT(YEAR FROM event_begin_time) AS int64) BETWEEN 2020 AND 2029 THEN 8
ELSE 0
END AS year_interval
FROM `helpful-cipher-450917-h6.noaa.tornado_data_condensed2`
ORDER BY year_interval
;

-- Then, partition the data to rank the states by number of tornadoes over each 10 year period. The partioned data is then subqueried to determine the top 5 states in each period. 

SELECT year_interval, state_name, number_of_tornadoes, tornado_rank
FROM 
(
  SELECT year_interval
, state_name
, COUNT(state_name) AS number_of_tornadoes
, ROW_NUMBER() OVER (PARTITION BY year_interval ORDER BY count(state_name) DESC)  AS tornado_rank
FROM `helpful-cipher-450917-h6.noaa.10_year_interval_table`
GROUP BY year_interval, state_name
ORDER BY year_interval, tornado_rank
)
WHERE tornado_rank <= 5
ORDER BY year_interval, tornado_rank;
