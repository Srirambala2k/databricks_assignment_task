-- Gold Layer: Customer Summary with Aggregations (Business Ready with Auto Loader and Deduplication)
CREATE OR REFRESH MATERIALIZED VIEW gold.customer_summary_autoloader
(
  -- Business Metric Validations
  CONSTRAINT valid_customer_metrics EXPECT (customer_id IS NOT NULL AND customer_name IS NOT NULL),
  CONSTRAINT valid_order_counts EXPECT (total_orders >= 0),
  CONSTRAINT valid_amounts EXPECT (total_amount_spent >= 0 AND avg_order_amount >= 0),
  CONSTRAINT valid_customer_segment EXPECT (customer_segment IN ('No Orders', 'One-Time Customer', 'Occasional Customer', 'Frequent Customer')),
  CONSTRAINT valid_customer_status EXPECT (customer_status IN ('Never Ordered', 'Active', 'Inactive'))
)
COMMENT 'Business-ready aggregated customer metrics from Auto Loader with deduplication'
AS
WITH deduplicated_customers AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY silver_ingestion_time DESC) AS row_num
  FROM customer_orders_task.silver.silver_customers_data_streaming
),
deduplicated_orders AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY silver_ingestion_time DESC) AS row_num
  FROM customer_orders_task.silver.silver_order_data_streaming
),
clean_customers AS (
  SELECT * FROM deduplicated_customers WHERE row_num = 1
),
clean_orders AS (
  SELECT * FROM deduplicated_orders WHERE row_num = 1
)
SELECT
    c.customer_id,
    c.customer_name,
    COALESCE(c.email, 'no_email_provided') AS email,
    c.is_email_valid,
    c.created_date,
    -- Handle NULL values from LEFT JOIN (customers with no orders)
    COALESCE(COUNT(o.order_id), 0) AS total_orders,
    COALESCE(SUM(o.amount), 0) AS total_amount_spent,
    COALESCE(AVG(o.amount), 0) AS avg_order_amount,
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date,
    COALESCE(DATEDIFF(MAX(o.order_date), MIN(o.order_date)), 0) AS customer_lifetime_days,
    -- Customer segmentation flags
    CASE 
        WHEN COUNT(o.order_id) = 0 THEN 'No Orders'
        WHEN COUNT(o.order_id) = 1 THEN 'One-Time Customer'
        WHEN COUNT(o.order_id) BETWEEN 2 AND 5 THEN 'Occasional Customer'
        ELSE 'Frequent Customer'
    END AS customer_segment,
    -- Active/Inactive flag (based on last order within 90 days)
    CASE 
        WHEN MAX(o.order_date) IS NULL THEN 'Never Ordered'
        WHEN DATEDIFF(CURRENT_DATE(), MAX(o.order_date)) <= 90 THEN 'Active'
        ELSE 'Inactive'
    END AS customer_status
FROM clean_customers c
LEFT JOIN clean_orders o
    ON c.customer_id = o.customer_id
WHERE c.customer_id IS NOT NULL
  AND c.customer_name IS NOT NULL
  AND c.customer_name != ''
GROUP BY ALL;


-- Gold Layer: Detailed Fact Table (Business Ready with Auto Loader and Deduplication)
CREATE OR REFRESH MATERIALIZED VIEW gold.fact_orders_autoloader
(
  -- Business Metric Validations
  CONSTRAINT valid_fact_ids EXPECT (order_id IS NOT NULL AND customer_id IS NOT NULL),
  CONSTRAINT valid_fact_amount EXPECT (amount > 0 AND amount < 1000000),
  CONSTRAINT valid_order_size EXPECT (order_size_category IN ('Small', 'Medium', 'Large', 'Very Large')),
  CONSTRAINT valid_fact_date EXPECT (order_date IS NOT NULL AND order_date <= CURRENT_DATE())
)
COMMENT 'Business-ready fact table with Auto Loader and deduplication'
AS
WITH deduplicated_customers AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY silver_ingestion_time DESC) AS row_num
  FROM customer_orders_task.silver.silver_customers_data_streaming
),
deduplicated_orders AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY silver_ingestion_time DESC) AS row_num
  FROM customer_orders_task.silver.silver_order_data_streaming
),
clean_customers AS (
  SELECT * FROM deduplicated_customers WHERE row_num = 1
),
clean_orders AS (
  SELECT * FROM deduplicated_orders WHERE row_num = 1
)
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    COALESCE(c.email, 'no_email_provided') AS email,
    COALESCE(c.is_email_valid, false) AS is_email_valid,
    o.order_date,
    o.amount,
    -- Add business metrics
    CASE 
        WHEN o.amount < 50 THEN 'Small'
        WHEN o.amount BETWEEN 50 AND 200 THEN 'Medium'
        WHEN o.amount BETWEEN 201 AND 500 THEN 'Large'
        ELSE 'Very Large'
    END AS order_size_category,
    o.bronze_ingestion_time,
    o.silver_ingestion_time
FROM clean_orders o
INNER JOIN clean_customers c
    ON o.customer_id = c.customer_id
WHERE o.order_id IS NOT NULL
  AND o.customer_id IS NOT NULL
  AND o.amount IS NOT NULL
  AND o.amount > 0;
