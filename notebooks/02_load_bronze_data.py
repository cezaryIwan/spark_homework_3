# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup and Configuration

# COMMAND ----------

storage_account_name = 'sasparkhm3'
storage_account_key = dbutils.secrets.get(scope='secret-scope', key='storage_account_key')
adls_container_name = 'contsparkhm3'

spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

hotel_weather_input_path = f'abfs://{adls_container_name}@{storage_account_name}.dfs.core.windows.net/m13sparkstreaming/hotel-weather'
checkpoint_path = f'abfs://{adls_container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/hotel_weather_raw'

# COMMAND ----------

# MAGIC %run "../utils/configuration"

# COMMAND ----------

# MAGIC %run "../utils/encryption_helper"

# COMMAND ----------

encryption_helper = EncryptionHelper(dbutils)
encrypted_df = encryption_helper.encrypt_dataframe(raw_df, common_pii_fields) # variable from configuration notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verify paths and data existence 

# COMMAND ----------

checkpoint_path = f'abfs://{adls_container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/hotel_weather_raw'

try:
    dbutils.fs.ls(hotel_weather_input_path)
except Exception as e:
    print(f"Error: Source files not found/Access issue occured: {e}")
    raise

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream

# COMMAND ----------

raw_df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .schema(data_schema) # variable from configuration notebook
    .load(hotel_weather_input_path)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream

# COMMAND ----------

target_table_name = "bronze.hotel_weather_raw"

(encrypted_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", f"{checkpoint_path}/data")
    .trigger(availableNow=True)
    .toTable("bronze.hotel_weather_raw")
)
