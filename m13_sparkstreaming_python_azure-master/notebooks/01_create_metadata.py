from pyspark.sql import SparkSession

CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS "

def create_schemas(spark: SparkSession):
    spark.sql(CREATE_SCHEMA + "bronze")
    spark.sql(CREATE_SCHEMA + "silver")
    spark.sql(CREATE_SCHEMA + "gold")

if __name__ == "__main__":
    spark_session = SparkSession.builder.appName("MetadataCreation").getOrCreate()
    create_schemas(spark_session)