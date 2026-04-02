# how to run
- Execute set_up.sql to configure catalog, schemas, and volumes.
- Run raw_to_bronze.py to ingest raw data into Bronze tables.
- Run bronze_to_silver.py to apply transformations and create Silver tables.
- Execute silver_to_gold.sql to generate Gold fact tables.


# databricks_assignment_task
full end to end project


the set_up.sql have the query to create catalog,schemas,volume


using the volume we created extracted the external data from s3

customers_order_date this folder consist of 2 python file and and one sql file

in the python file raw_to_bronze i have extracted raw data and created bronze table without any transformation

in the python file bronze_to_silver did some transformation like check for duplicate,white space trim ,column name change,rumove null ect

in the silver_to_gold.sql file created a fact table for business ready data

in this bronze & silver i used streaming table and gold layer is materilized view


# pending task
- Delta Live Tables (DLT) pipeline
You built Bronze and Silver layers using streaming tables and SQL, but DLT requires setting up a managed pipeline with expectations (data quality rules) and orchestration. It’s more complex and usually needs a premium Databricks workspace or additional configuration, which you may not have had access to.
- Lakeflow Connect integration
Lakeflow is Databricks’ orchestration tool for connecting external sources (like Oracle or other databases) directly into pipelines. Since your project ingested data from S3 volumes, you didn’t configure Lakeflow connectors, so this part remains incomplete.
- Lakebridge Analyzer & Transpiler
These tools are specifically for analyzing Oracle SQL scripts and automatically converting them into Databricks-compatible SQL. If you didn’t have Oracle scripts or the Lakebridge feature enabled in your environment, you couldn’t run this step.





