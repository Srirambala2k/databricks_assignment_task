import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col

# -------------------------------
# Bronze Customers Data Table with Auto Loader
# -------------------------------
@dlt.table(
    name="bronze_customers_data_streaming",
    comment="Raw customers data incrementally ingested using Auto Loader from CSV volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect("bronze_customer_id_exists", "customer_id IS NOT NULL")
@dlt.expect("bronze_file_tracked", "_metadata IS NOT NULL")
def bronze_customers_data_streaming():
    # Auto Loader monitors the directory for new files
    raw_path = "/Volumes/customer_orders_task/source_data/raw_datas/"
    
    bronze_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "customer_id int, Name string, Email string, Created_date date")
        .option("pathGlobFilter", "customes_data.csv")
        .load(raw_path)
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
    
    return bronze_df


# -------------------------------
# Bronze Order Data Table with Auto Loader
# -------------------------------
@dlt.table(
    name="bronze_order_data_streaming",
    comment="Raw orders data incrementally ingested using Auto Loader from CSV volume",
    table_properties={
        "quality": "bronze",
        "pipelines.autoOptimize.managed": "true"
    }
)
@dlt.expect("bronze_order_id_exists", "order_id IS NOT NULL")
@dlt.expect("bronze_customer_ref_exists", "customer_id IS NOT NULL")
@dlt.expect("bronze_amount_exists", "Amount IS NOT NULL")
def bronze_order_data_streaming():
    # Auto Loader monitors the directory for new files
    raw_path = "/Volumes/customer_orders_task/source_data/raw_datas/"
    
    bronze_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("delimiter", ",")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaHints", "order_id int, customer_id int, order_date date, Amount int")
        .option("pathGlobFilter", "orders.csv")
        .load(raw_path)
        .withColumn("ingestion_time", F.current_timestamp())
        .withColumn("source_file", col("_metadata.file_path"))
    )
    
    return bronze_df
