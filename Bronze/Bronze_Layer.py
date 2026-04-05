# Databricks notebook source
# MAGIC %md
# MAGIC # **Manually Hardcoded Values**

# COMMAND ----------

df=spark.readStream.format('cloudFiles')\
    .option("cloudFiles.format", "csv")\
    .option("cloudFiles.schemaLocation", "abfss://bronze@storageaccountfull.dfs.core.windows.net/checkpoint_customers")\
    .load("abfss://source@storageaccountfull.dfs.core.windows.net/customers")


# COMMAND ----------

df.writeStream.format('parquet')\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://bronze@storageaccountfull.dfs.core.windows.net/checkpoint_customers")\
    .option("path", "abfss://bronze@storageaccountfull.dfs.core.windows.net/customers")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test

# COMMAND ----------

df=spark.read.format('parquet').load('abfss://bronze@storageaccountfull.dfs.core.windows.net/customers')
df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Dynamically Ingesting the file

# COMMAND ----------

source_path = "abfss://source@storageaccountfull.dfs.core.windows.net/"
bronze_path = "abfss://bronze@storageaccountfull.dfs.core.windows.net/"

folders = [f.name.replace("/", "") for f in dbutils.fs.ls(source_path) if f.isDir()]

streams = []

for folder in folders:
    
    print(f"Processing: {folder}")
    
    df = spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "csv") \
        .option("header", "true") \
        .option("cloudFiles.schemaLocation", f"{bronze_path}checkpoint_{folder}") \
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
        .load(f"{source_path}{folder}")
    
    query = df.writeStream.format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", f"{bronze_path}checkpoint_{folder}") \
        .option("path", f"{bronze_path}{folder}") \
        .trigger(once=True) \
        .start()
    
    streams.append(query)

for s in streams:
    s.awaitTermination()