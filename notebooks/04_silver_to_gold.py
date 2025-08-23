# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup and Configuration

# COMMAND ----------

from pyspark.sql.functions import col, countDistinct, avg, max, min
from delta.tables import DeltaTable

# COMMAND ----------

gold_table_name = f"gold.hotel_weather_metrics"
secret_scope_name = 'secret-scope'
storage_account_name = 'sasparkhm3'
adls_container_name = 'contsparkhm3'

checkpoint_path = f'abfs://{adls_container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/hotel_weather_gold'
storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key='storage_account_key')
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

# MAGIC %run "../utils/configuration"

# COMMAND ----------

# MAGIC %run "../utils/encryption_helper"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream

# COMMAND ----------

silver_df = (spark.readStream
    .table('silver.hotel_weather_processed')
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Decrypt PII Fields

# COMMAND ----------

encryption_helper = EncryptionHelper(dbutils)

decrypted_df = encryption_helper.decrypt_dataframe(silver_df, common_pii_fields) # variable from configuration notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Define Aggregation and Upsert Logic

# COMMAND ----------

def calculate_and_upsert_metrics(micro_batch_df, batch_id):
    metrics_df = (micro_batch_df
        .groupBy("country", "city", "wthr_date")
        .agg(
            countDistinct("id").alias("number_of_distinct_hotels"),
            avg("avg_tmpr_c").alias("avg_temperature"),
            max("avg_tmpr_c").alias("max_temperature"),
            min("avg_tmpr_c").alias("min_temperature")
        )
        .withColumn("temperature_difference", col("max_temperature") - col("min_temperature"))
    )


    if not spark.catalog.tableExists(gold_table_name): 
        (spark.createDataFrame([], metrics_df.schema)
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(gold_table_name)
        )

    gold_target_table = DeltaTable.forName(spark, gold_table_name)
    
    merge_condition = """
        target.country = source.country AND
        target.city = source.city AND
        target.wthr_date = source.wthr_date
    """

    (gold_target_table.alias("target")
        .merge(
            source=metrics_df.alias("source"),
            condition=merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream

# COMMAND ----------

query = (decrypted_df.writeStream
    .foreachBatch(calculate_and_upsert_metrics)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
