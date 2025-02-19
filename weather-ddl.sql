-- Databricks notebook source
create schema if not exists weather
location 'abfss://silver@masteraaa001sta.dfs.core.windows.net/weather/'

-- COMMAND ----------

CREATE TABLE if not exists weather.report (
    base STRING,
    clouds BIGINT,
    cod BIGINT,
    dt BIGINT,
    dt_timestamp TIMESTAMP,
    id BIGINT,
    name STRING,
    timezone BIGINT,
    visibility BIGINT,
    ingestion_timestamp TIMESTAMP,
    coor_lat DOUBLE,
    coor_lon DOUBLE,
    feels_like DOUBLE,
    grnd_level BIGINT,
    humidity BIGINT,
    pressure BIGINT,
    sea_level BIGINT,
    temp DOUBLE,
    temp_max DOUBLE,
    temp_min DOUBLE,
    country STRING,
    sys_id BIGINT,
    sunrise BIGINT,
    sunset BIGINT,
    type BIGINT,
    wind_deg BIGINT,
    wind_gust DOUBLE,
    wind_speed DOUBLE
)
USING DELTA
LOCATION 'abfss://silver@masteraaa001sta.dfs.core.windows.net/weather/report'


-- COMMAND ----------

CREATE TABLE if not exists weather.forecast (
    clouds BIGINT,
    name STRING,
    pop DOUBLE,
    visibility BIGINT,
    ingestion_timestamp TIMESTAMP,
    dt_timestamp TIMESTAMP,
    feels_like DOUBLE,
    grnd_level BIGINT,
    humidity BIGINT,
    pressure BIGINT,
    sea_level BIGINT,
    temp DOUBLE,
    temp_max DOUBLE,
    temp_min DOUBLE,
    wind_deg BIGINT,
    wind_gust DOUBLE,
    wind_speed DOUBLE
)
USING DELTA
LOCATION 'abfss://silver@masteraaa001sta.dfs.core.windows.net/weather/forecast'


-- COMMAND ----------

create schema if not exists weather_analysis
location 'abfss://gold@masteraaa001sta.dfs.core.windows.net/weather_analysis/'

-- COMMAND ----------

CREATE TABLE if not exists weather_analysis.temperature (
    name STRING,
    dt_timestamp TIMESTAMP,
    forecast_temp DOUBLE,
    report_temp DOUBLE,
    temp_diff DOUBLE
)
USING DELTA
LOCATION 'abfss://gold@masteraaa001sta.dfs.core.windows.net/weather_analysis/temperature'