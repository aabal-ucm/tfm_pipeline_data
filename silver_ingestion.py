# Databricks notebook source
import datetime
from pyspark.sql.functions import col, lit, from_unixtime, to_timestamp

dbutils.widgets.text("execution_date", "2025-02-12T09:00:00", "Execution date")

iso_datetime = dbutils.widgets.get("execution_date")
date_start = datetime.datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%S")
date_end = date_start + datetime.timedelta(hours=3)

# COMMAND ----------

weather_df = spark.sql(f"""
          select * from raw_weather.report 
          where ingestion_timestamp >= '{date_start}' and ingestion_timestamp < '{date_end}'
""")

weather_df = (weather_df
                .withColumn("clouds", col("clouds.all"))
                .withColumn("coor_lat", col("coord.lat"))
                .withColumn("coor_lon", col("coord.lon"))
                .withColumn("feels_like", col("main.feels_like"))
                .withColumn("grnd_level", col("main.grnd_level"))
                .withColumn("humidity", col("main.humidity"))
                .withColumn("pressure", col("main.pressure"))
                .withColumn("sea_level", col("main.sea_level"))
                .withColumn("temp", col("main.temp"))
                .withColumn("temp_max", col("main.temp_max"))
                .withColumn("temp_min", col("main.temp_min"))
                .withColumn("country", col("sys.country"))
                .withColumn("sys_id", col("sys.id"))
                .withColumn("sunrise", col("sys.sunrise"))
                .withColumn("sunset", col("sys.sunset"))
                .withColumn("type", col("sys.type"))
                .withColumn("wind_deg", col("wind.deg"))
                .withColumn("wind_gust", col("wind.gust"))
                .withColumn("wind_speed", col("wind.speed"))
                .withColumn("dt_timestamp", to_timestamp(from_unixtime(col("dt"))))
                .drop("coord","main","sys","wind","weather","rain","snow")
            )

weather_df.createOrReplaceTempView("report")

display(weather_df)

# COMMAND ----------

display(spark.sql(f"""delete from weather.report where ingestion_timestamp >= '{date_start}' and ingestion_timestamp < '{date_end}'"""))

# COMMAND ----------

weather_df.write     \
    .format("delta") \
    .mode("append")  \
    .saveAsTable("weather.report")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load weather.forecast

# COMMAND ----------

weather_df = spark.sql(f"""
          select * from raw_weather.forecast 
          where ingestion_timestamp >= '{date_start}' and ingestion_timestamp < '{date_end}'
""")

weather_df = (weather_df
                .withColumn("clouds", col("clouds.all"))
                .withColumn("feels_like", col("main.feels_like"))
                .withColumn("grnd_level", col("main.grnd_level"))
                .withColumn("humidity", col("main.humidity"))
                .withColumn("pressure", col("main.pressure"))
                .withColumn("sea_level", col("main.sea_level"))
                .withColumn("temp", col("main.temp"))
                .withColumn("temp_max", col("main.temp_max"))
                .withColumn("temp_min", col("main.temp_min"))
                .withColumn("wind_deg", col("wind.deg"))
                .withColumn("wind_gust", col("wind.gust"))
                .withColumn("wind_speed", col("wind.speed"))
                .drop("coord","main","sys","wind","weather","rain","snow")
            )

weather_df.createOrReplaceTempView("forecast")

display(weather_df)

# COMMAND ----------

display(spark.sql(f"""delete from weather.forecast where ingestion_timestamp >= '{date_start}' and ingestion_timestamp < '{date_end}'"""))

# COMMAND ----------

weather_df.write     \
    .format("delta") \
    .mode("append")  \
    .saveAsTable("weather.forecast")