# Databricks notebook source
# MAGIC %md
# MAGIC **Import Required Libraries**

# COMMAND ----------

#Pricing Data processing

# COMMAND ----------

from pyspark.sql import functions as f
from pyspark.sql.window import Window
from  delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC **Load Project Utilities & Initialize Notebook Widgets**

# COMMAND ----------

# MAGIC %run /Workspace/Users/aryancom16@gmail.com/consolidated_pipeline/1_setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "gross_price", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")

base_path = f's3://sportsbarr-dmss/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze

# COMMAND ----------

df = (
    spark.read.format("csv")
        .option("header", True)
        .option("inferSchema", True)
        .load(base_path)
        .withColumn("read_timestamp", f.current_timestamp())
        .select("*", "_metadata.file_name", "_metadata.file_size")
)

# COMMAND ----------

# print check data type
df.printSchema()

# COMMAND ----------

display(df.limit(10))

# COMMAND ----------

df.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

df_bronze = spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")
df_bronze.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transformations**

# COMMAND ----------

# MAGIC %md
# MAGIC - 1: Normalise `month` field

# COMMAND ----------

df_bronze.select('month').distinct().show()

# COMMAND ----------


# as month column has lots of diff date formats, we are going to convert them into uniform format.
date_formats = ["yyyy/MM/dd", "dd/MM/yyyy", "yyyy-MM-dd", "dd-MM-yyyy"]
df_silver = df_bronze.withColumn(
    "month",
    f.coalesce(
        f.try_to_date(f.col("month"), "yyyy/MM/dd"),
        f.try_to_date(f.col("month"), "dd/MM/yyyy"),
        f.try_to_date(f.col("month"), "yyyy-MM-dd"),
        f.try_to_date(f.col("month"), "dd-MM-yyyy")
    )
)

# COMMAND ----------

df_silver.select('month').distinct().show()

# COMMAND ----------

# MAGIC %md
# MAGIC - 2: Handling `gross_price`

# COMMAND ----------

df_silver.show()

# COMMAND ----------

# We are validating the gross_price column, converting only valid numeric values to double, fixing negative prices by making them positive, and replacing all non-numeric values with 0.
#so we are simply making gross_price column good.


#the thing is automatically they convert gross_price to string, to solve this we have to do .cast("double") everytime so that we can do mathermatical operations by keeping it decimal numbers.

df_silver = df_silver.withColumn(
    "gross_price",
    f.when(f.col("gross_price").rlike(r'^-?\d+(\.\d+)?$'),
           f.when(f.col("gross_price").cast("double") < 0, -1*f.col("gross_price").cast("double"))
           .otherwise(f.col("gross_price").cast("double")))
    .otherwise(f.lit(0.0))
)







# COMMAND ----------

df_silver.show()

# COMMAND ----------

# so now we are doing a join on products table to get the same product codes




df_products = spark.table("fmcg.silver.products")
df_joined = df_silver.join(df_products.select("product_id","product_code"),on ="product_id",how="inner")
df_joined = df_joined.select("product_id","product_code","month","gross_price","read_timestamp","file_name","file_size")
df_joined.show()

# COMMAND ----------

df_joined.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true")\
 .option("mergeSchema", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold

# COMMAND ----------

df_silver = spark.sql(f"SELECT * FROM {catalog}.{silver_schema}.{data_source};")

# COMMAND ----------

# select only required columns
df_gold = df_silver.select("product_code", "month", "gross_price")
df_gold.show(5)


# COMMAND ----------

df_gold.write\
 .format("delta") \
 .option("delta.enableChangeDataFeed", "true") \
 .mode("overwrite") \
 .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Merging Data source with parent

# COMMAND ----------

#reloading data from gold layer to do further business aggregations
df_gold_price = spark.table("fmcg.gold.sb_dim_gross_price")
df_gold_price.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC - Get the price for each product_code (aggregated by year)

# COMMAND ----------


# get price for each product_code aggregated by year.

#we extract and create year column from our month column.
df_gold_price = (
    df_gold_price
    .withColumn("year",f.year("month"))
    .withColumn("is_zero",f.when(f.col("gross_price") == 0,1).otherwise(0))

)
#here above if gross_price equals 0 it is termed 1 which is bad and otherwise it is 0.
#1 in column simply means gross_price is 0, and 00 in column represents non zero gross_price.
# is_zero flags the bad condition 


#here we are using window function by product code and year and then we would rank then and whatever has rank 1 we will take as latest pricing.
w = (
    Window
    .partitionBy("product_code", "year")
    .orderBy(f.col("is_zero"), f.col("month").desc())
)


df_gold_latest_price = (
    df_gold_price
      .withColumn("rnk", f.row_number().over(w))
      .filter(f.col("rnk") == 1)
)



# COMMAND ----------

display(df_gold_latest_price)

# COMMAND ----------

## Taking the required columns

df_gold_latest_price = (
    df_gold_latest_price
    .select("product_code", "year", "gross_price")
    .withColumnRenamed("gross_price", "price_inr")
    .select("product_code", "price_inr", "year")
)
# change year to string
df_gold_latest_price = df_gold_latest_price.withColumn("year", f.col("year").cast("string"))

df_gold_latest_price.show(5)

# COMMAND ----------

df_gold_latest_price.printSchema()

# COMMAND ----------

delta_table = DeltaTable.forName(spark, "fmcg.gold.dim_gross_price")


delta_table.alias("target").merge(
    source=df_gold_latest_price.alias("source"),
    condition="target.product_code = source.product_code"
).whenMatchedUpdate(
    set={
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).whenNotMatchedInsert(
    values={
        "product_code": "source.product_code",
        "price_inr": "source.price_inr",
        "year": "source.year"
    }
).execute()
