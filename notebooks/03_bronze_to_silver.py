# Databricks notebook source
# MAGIC %md
# MAGIC ### Setup and Configuration

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run "../utils/encryption_helper"

# COMMAND ----------

# MAGIC %run "../utils/transformation_helper"

# COMMAND ----------

# MAGIC %run "../utils/configuration"

# COMMAND ----------

bronze_table_name = 'bronze.hotel_weather_raw'
silver_table_name = 'silver.hotel_weather_processed'
storage_account_name = 'sasparkhm3'
secret_scope_name = 'secret-scope'
storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key='storage_account_key')
adls_container_name = 'contsparkhm3'

checkpoint_path = f'abfs://{adls_container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/hotel_weather_silver'
storage_account_key = dbutils.secrets.get(scope=secret_scope_name, key='storage_account_key')
spark.conf.set(f'fs.azure.account.key.{storage_account_name}.dfs.core.windows.net', storage_account_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Stream

# COMMAND ----------

bronze_df = (spark.readStream
    .table(bronze_table_name)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Apply Decryption, Transformation, and Re-encryption

# COMMAND ----------

transformation_helper = TransformationHelper()
encryption_helper = EncryptionHelper(dbutils)

hotel_weather_string_fields = ['address', 'city', 'country', 'geoHash', 'name']
hotel_weather_critical_fields = ['id', 'wthr_date', 'avg_tmpr_c']

decrypted_df = encryption_helper.decrypt_dataframe(bronze_df, common_pii_fields) # variable from configuration notebook

transformed_df = (decrypted_df
    .transform(transformation_helper.cast_to_timestamp, ['wthr_date'])
    .transform(transformation_helper.clean_string_types, hotel_weather_string_fields)
    .transform(transformation_helper.cast_to_numeric, 'id')
    .transform(transformation_helper.clean_null_values, hotel_weather_critical_fields)
    .transform(transformation_helper.drop_duplicates, ['id', 'wthr_date'])
)

encrypted_df = encryption_helper.encrypt_dataframe(transformed_df, common_pii_fields) # variable from configuration notebook

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upsert Function Used In Writing Stream

# COMMAND ----------

def upsert_to_silver(micro_batch_df, batch_id):
    silver_target_table = DeltaTable.forName(spark, silver_table_name)
    
    (silver_target_table.alias("target")
        .merge(
            source=micro_batch_df.alias("source"),
            condition="target.id = source.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Stream

# COMMAND ----------

if not spark.catalog.tableExists(silver_table_name):
    (spark.createDataFrame([], encrypted_df.schema)
        .write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table_name)
    )

query = (encrypted_df.writeStream
    .foreachBatch(upsert_to_silver)
    .option("checkpointLocation", checkpoint_path)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()
