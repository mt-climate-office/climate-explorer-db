create schema future AUTHORIZATION mco;
create table future.county();
alter table future.county
	add column "name" varchar(64),
	add column id varchar(64),
	add column model varchar(64),
	add column scenario varchar(64),
	add column variable varchar(64),
	add column "date" date,
	add column value numeric;

CREATE UNIQUE INDEX county_future_unique_index ON future.county USING btree (id, variable, date DESC, scenario, model);

set datestyle to ISO, YMD;
copy future.county (model, scenario, variable, "name", "date", value, id) from '/data/county.csv' delimiter ',' csv header;

create schema historical AUTHORIZATION mco;
create table historical.county();
alter table historical.county
	add column "name" varchar(64),
	add column id varchar(64),
	add column variable varchar(64),
	add column "date" date,
	add column value numeric;

CREATE UNIQUE INDEX county_historical_unique_index ON historical.county USING btree (id, variable, date DESC);

set datestyle to ISO, YMD;
copy historical.county ("name", id, variable, "date", value) from '/data/county_historical.csv' delimiter ',' csv header;

-- create table future.huc();
-- alter table future.huc
-- 	add column "name" varchar(64),
-- 	add column id varchar(64),
-- 	add column model varchar(64),
-- 	add column scenario varchar(64),
-- 	add column variable varchar(64),
-- 	add column "date" date,
-- 	add column value numeric;

-- CREATE UNIQUE INDEX huc_unique_index ON future.huc USING btree (id, variable, date DESC, scenario, model);

-- set datestyle to ISO, YMD;
-- copy future.huc (model, scenario, variable, "name", "date", value, id) from '/data/huc.csv' delimiter ',' csv header;
