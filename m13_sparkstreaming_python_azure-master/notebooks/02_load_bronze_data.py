from pyspark.sql import SparkSession
from utils.encryption_helper import EncryptionHelper
from pyspark.sql import dbutils

def ingest_to_bronze():
    storage_account_name = "sasparkhm3"
    storage_account_key = dbutils.secrets.get(scope='secret-scope', key='encryption-key')
    # TODO: Figure otu how to inject or reuse effictiently DBUtils object, for retrievieng secrets
    container_name = "contsparkhm3"
    
    source_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/landing-zone/hotel-weather/"
    checkpoint_base_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/checkpoints/"
    
    bronze_table_name = "bronze.hotel_weather_raw"

    pii_columns = ["guest_name", "guest_address", "phone_number"] #TODO: Define PII columns based on your schema

    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()
    spark.conf.set(f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", storage_account_key)

    encryptor = EncryptionHelper(scope="formula1-scope", key_name="encryption-key")

    raw_df = (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.schemaLocation", f"{checkpoint_base_path}/schemas/hotel_weather")
            .load(source_path)
    )

    encrypted_df = encryptor.encrypt_df(raw_df, pii_columns)
    
    (
        encrypted_df.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", f"{checkpoint_base_path}/streams/hotel_weather_bronze")
            .trigger(availableNow=True)
            .toTable(bronze_table_name)
    )

if __name__ == "__main__":
    ingest_to_bronze()