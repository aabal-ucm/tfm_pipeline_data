# Databricks notebook source
import datetime
from pyspark.sql.functions import col, lit, date_trunc, to_timestamp

dbutils.widgets.text("execution_date", "2025-02-12T09:00:00", "Execution date")

iso_datetime = dbutils.widgets.get("execution_date")
date_start = datetime.datetime.strptime(iso_datetime, "%Y-%m-%dT%H:%M:%S")
date_end = date_start + datetime.timedelta(hours=3)

# COMMAND ----------

report_df = spark.sql(f"""
          select * from weather.report 
          where dt_timestamp >= '{date_start}' and dt_timestamp <= '{date_end}'
""").withColumn("dt_timestamp_hour", date_trunc("hour", col("dt_timestamp")))

forecast_df = spark.sql(f"""
          select * from weather.forecast 
          where dt_timestamp >= '{date_start}' and dt_timestamp <= '{date_end}'
""")

# COMMAND ----------

joined_df = forecast_df.join(
    report_df,
    (forecast_df.name == report_df.name) &
    (forecast_df.dt_timestamp == report_df.dt_timestamp_hour),
    "inner"
).withColumn("temp_diff", col("forecast.temp") - col("report.temp")) \
.withColumnRenamed("forecast.temp", "forecast_temp") \
.withColumnRenamed("report.temp", "report_temp") \
.select(
      col("forecast.name"),
      col("forecast.dt_timestamp"),
      col("forecast.temp").alias("forecast_temp"),
      col("report.temp").alias("report_temp"),
      col("temp_diff")
  ) \
.dropDuplicates()


display(joined_df)

# COMMAND ----------

display(spark.sql(f"""delete from weather_analysis.temperature  where dt_timestamp >= '{date_start}' and dt_timestamp <= '{date_end}'"""))

# COMMAND ----------

joined_df.write     \
    .format("delta") \
    .mode("append")  \
    .saveAsTable("weather_analysis.temperature")