# Databricks notebook source
import requests
from pyspark.sql.functions import current_timestamp, to_timestamp, col,lit
import datetime

api_key = dbutils.secrets.get(scope="secrets", key="weather_api_key")
cities = ["Madrid", "Barcelona", "Paris", "London", "Berlin"]

dbutils.widgets.text("execution_date", "2025-02-12T09:00:00", "Execution date")

iso_datetime = dbutils.widgets.get("execution_date")
date_start = datetime.datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%S")
date_end = date_start + datetime.timedelta(hours=3)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Get weather report data

# COMMAND ----------

# Get data
weather_data = []
for city in cities:
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        weather_data.append(response.json())
    else: 
        print(f"Error obteniendo datos de {city}: {response.status_code}")

weather_df = spark.read.json(spark.sparkContext.parallelize(weather_data)) \
                .withColumn("ingestion_timestamp",lit(date_start))

display(weather_df)

# COMMAND ----------

# Write data
weather_df.write     \
    .format("delta") \
    .mode("append")  \
    .option("path", "abfss://bronze@masteraaa001sta.dfs.core.windows.net/weather/report") \
    .saveAsTable("raw_weather.report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get forecast data

# COMMAND ----------

forecast_data = []

for city in cities:
    url = f"https://api.openweathermap.org/data/2.5/forecast?q={city}&exclude=alerts,hourly,minutely,current&appid={api_key}"
    response = requests.get(url)
    if response.status_code == 200:
        city_forecast = response.json()['list']
        for entry in city_forecast:
            entry['name'] = city 
        forecast_data.extend(city_forecast)
    else:
        print(f"Error obteniendo pronóstico de {city}: {response.status_code}")

forecast_df = spark.read.json(spark.sparkContext.parallelize(forecast_data)) \
                .withColumn("ingestion_timestamp",current_timestamp()) \
                .withColumn("dt_timestamp", to_timestamp("dt_txt", "yyyy-MM-dd HH:mm:ss")) \
                .drop("dt_txt","dt") \
                .filter((col("dt_timestamp") <= date_end) & (col("dt_timestamp") > date_start))


display(forecast_df.limit(100))

# COMMAND ----------

# Write data
forecast_df.write     \
    .format("delta") \
    .mode("append")  \
    .option("path", "abfss://bronze@masteraaa001sta.dfs.core.windows.net/weather/forecast") \
    .saveAsTable("raw_weather.forecast")