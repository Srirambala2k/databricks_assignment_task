# databricks_assignment_task
full end to end project


the set_up.sql have the query to create catalog,schemas,volume


using the volume we created extracted the external data from s3

customers_order_date this folder consist of 2 python file and and one sql file

in the python file raw_to_bronze i have extracted raw data and created bronze table without any transformation

in the python file bronze_to_silver did some transformation like check for duplicate,white space trim ,column name change,rumove null ect

in the silver_to_gold.sql file created a fact table for business ready data

in this bronze & silver i used streaming table and gold layer is materilized view



