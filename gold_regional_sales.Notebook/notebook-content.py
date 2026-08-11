# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

# Transactions x Customers -> gold_regional_sales
#
# Before running: upload sales_transactions.csv and customers.csv into this
# Lakehouse's Files section (e.g. a "raw" subfolder), and attach a Lakehouse
# to this notebook so relative Files/... paths resolve and Tables/... is
# where gold_regional_sales will land.

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 1. Load source CSVs
CUSTOMERS_PATH = "Files/raw/customers.csv"
TRANSACTIONS_PATH = "Files/raw/sales_transactions.csv"

customers_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(CUSTOMERS_PATH)
)

transactions_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(TRANSACTIONS_PATH)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2. Inner join on CustomerID
joined_df = transactions_df.join(customers_df, on="CustomerID", how="inner")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 3. Aggregate: total sales amount + transaction count by Region / ProductCategory
gold_regional_sales_df = (
    joined_df.groupBy("Region", "ProductCategory")
    .agg(
        F.round(F.sum("SalesAmount"), 2).alias("TotalSalesAmount"),
        F.count("*").alias("TotalTransactionCount"),
    )
    .orderBy("Region", "ProductCategory")
)

gold_regional_sales_df.show(50, truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 4. Write as a managed Delta table in the Lakehouse
(
    gold_regional_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_regional_sales")
)

spark.sql("SELECT * FROM gold_regional_sales ORDER BY Region, ProductCategory").show(
    50, truncate=False
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
