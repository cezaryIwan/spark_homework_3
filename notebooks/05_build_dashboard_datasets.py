# Databricks notebook source
from pyspark.sql.functions import col, sum

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Setup and Configuration

# COMMAND ----------

gold_db = "gold"
source_table_name = f"{gold_db}.hotel_weather_metrics"
dashboard_table_prefix = f"{gold_db}.dashboard_metrics"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Find the Top 5 Cities

# COMMAND ----------

top_5_cities_df = (spark.read.table(source_table_name)
    .groupBy("country", "city")
    .agg(sum("number_of_distinct_hotels").alias("total_hotel_reports"))
    .orderBy(col("total_hotel_reports").desc())
    .limit(5)
)

top_5_cities_list = [(row['country'], row['city']) for row in top_5_cities_df.collect()]

for country, city in top_5_cities_list:
    print(f"- {city}, {country}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Create a Separate Dataset for Each Top City

# COMMAND ----------

gold_df = spark.read.table(source_table_name)

for country, city in top_5_cities_list:
    city_name_safe = city.replace(' ', '_').lower()
    target_table_name = f"{dashboard_table_prefix}_{city_name_safe}"
    
    city_df = (gold_df
        .filter((col("city") == city) & (col("country") == country))
        .select(
            "wthr_date",
            col("number_of_distinct_hotels").alias("number_of_reported_hotels"),
            col("avg_temperature").alias("avg_tmpr_c"),
            col("max_temperature").alias("max_tmpr_c"),
            col("min_temperature").alias("min_tmpr_c")
        )
        .orderBy("wthr_date")
    )
    
    (city_df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_name)
    )
