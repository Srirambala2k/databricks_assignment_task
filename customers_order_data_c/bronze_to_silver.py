import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, trim, regexp_replace, current_timestamp

@dlt.table(
    name="silver.silver_customers_data_streaming",
    comment="Clean and standardized customers data from bronze layer with Auto Loader (deduplication in gold layer)",
    table_properties={"quality": "silver"}
)
# Data Quality Expectations - Drop invalid records
@dlt.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_email_format", "email IS NULL OR email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\\\.[A-Za-z]{2,}$'")
@dlt.expect_or_drop("valid_customer_name", "customer_name IS NOT NULL AND customer_name != ''")
# Monitoring Expectations - Log violations only
@dlt.expect("email_provided", "email IS NOT NULL AND email != ''")
@dlt.expect("valid_created_date", "created_date IS NOT NULL")
def silver_customers_data_streaming():
    
    # Read from bronze streaming table
    bronze_customers = spark.readStream.table("bronze_customers_data_streaming")
    
    cleaned_customers = (
        bronze_customers
        .withColumn("name", trim(col("Name")))
        .withColumn("email", trim(col("Email")))
        
        # Validate email format
        .withColumn(
            "is_email_valid",
            col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")
        )
        
        .filter(col("customer_id").isNotNull())
        .filter(col("name").isNotNull())
        .filter(col("name") != "")
        
        # ADD SILVER INGESTION TIME
        .withColumn("silver_ingestion_time", current_timestamp())
        
        .select(
            col("customer_id"),
            col("name").alias("customer_name"),
            col("email"),
            col("is_email_valid"),
            col("Created_date").alias("created_date"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            col("source_file"),
            col("silver_ingestion_time")
        )
    )
    
    return cleaned_customers


@dlt.table(
    name="silver.silver_order_data_streaming",
    comment="Clean and standardized order data from bronze layer with Auto Loader (deduplication in gold layer)",
    table_properties={"quality": "silver"}
)
# Data Quality Expectations - Drop invalid records
@dlt.expect_or_drop("valid_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_order_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_amount", "amount > 0 AND amount < 1000000")
@dlt.expect_or_fail("critical_order_date", "order_date IS NOT NULL")
# Monitoring Expectations - Log violations only
@dlt.expect("reasonable_amount", "amount >= 10 AND amount <= 10000")
@dlt.expect("recent_order", "order_date >= '2020-01-01'")
def silver_order_data_streaming():
 
    # Read from bronze streaming table
    bronze_orders = spark.readStream.table("bronze_order_data_streaming")

    cleaned_orders = (
        bronze_orders
        .filter(col("order_id").isNotNull())
        .filter(col("customer_id").isNotNull())
        .filter(col("Amount").isNotNull())
        .filter(col("Amount") > 0)
        
        # ADD SILVER INGESTION TIME
        .withColumn("silver_ingestion_time", current_timestamp())
        
        # STANDARDIZE COLUMN NAMES
        .select(
            col("order_id"),
            col("customer_id"),
            col("order_date"),
            col("Amount").alias("amount"),
            col("ingestion_time").alias("bronze_ingestion_time"),
            col("source_file"),
            col("silver_ingestion_time")
        )
    )
    
    return cleaned_orders
