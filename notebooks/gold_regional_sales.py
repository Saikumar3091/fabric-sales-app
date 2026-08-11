# Fabric Notebook: Transactions x Customers -> gold_regional_sales
#
# Run this as a PySpark notebook inside a Microsoft Fabric Lakehouse.
# Local files are not visible to the Fabric Spark cluster, so before running:
#   1. Open your Lakehouse -> Files.
#   2. Upload sales_transactions.csv and customers.csv (e.g. into a "raw" subfolder).
#   3. Update RAW_PATH below to match where you uploaded them.
#
# Attach this notebook to a Lakehouse so the "Tables" section resolves to
# <lakehouse>/Tables and gold_regional_sales lands there as a managed Delta table.

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.getOrCreate()

# ---------------------------------------------------------------------------
# 1. Load source CSVs
# ---------------------------------------------------------------------------
# Files section of the attached Lakehouse. Swap for an ABFS path if the
# Lakehouse isn't attached to this notebook, e.g.:
# "abfss://<workspace>@onelake.dfs.fabric.microsoft.com/<lakehouse>.Lakehouse/Files/raw"
RAW_PATH = "Files/raw"

transactions_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/sales_transactions.csv")
)

customers_df = (
    spark.read.option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{RAW_PATH}/customers.csv")
)

# ---------------------------------------------------------------------------
# 2. Inner join on CustomerID
# ---------------------------------------------------------------------------
joined_df = transactions_df.join(customers_df, on="CustomerID", how="inner")

# ---------------------------------------------------------------------------
# 3. Aggregate: total sales amount + transaction count by Region / ProductCategory
# ---------------------------------------------------------------------------
gold_regional_sales_df = (
    joined_df.groupBy("Region", "ProductCategory")
    .agg(
        F.round(F.sum("SalesAmount"), 2).alias("TotalSalesAmount"),
        F.count("*").alias("TotalTransactionCount"),
    )
    .orderBy("Region", "ProductCategory")
)

gold_regional_sales_df.show(50, truncate=False)

# ---------------------------------------------------------------------------
# 4. Write as a Delta table in the Lakehouse
# ---------------------------------------------------------------------------
(
    gold_regional_sales_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_regional_sales")
)

# Optional: confirm
spark.sql("SELECT * FROM gold_regional_sales ORDER BY Region, ProductCategory").show(
    50, truncate=False
)
