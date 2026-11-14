# Databricks notebook source
df= spark.read.option("header",True).csv("/Volumes/workspace/hiring_bronze/raw_files/employee_data.csv")
df.printSchema()


# COMMAND ----------

print(df.count())

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F

path = "/Volumes/workspace/hiring_bronze/raw_files/employee_data.csv"

df = spark.read.option("header", True).csv(path)

# Delta doesn't allow spaces in column names
for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().replace(" ", "_"))

df = (df
    .withColumn("_source_file", F.lit("employee_data.csv"))
    .withColumn("_load_time", F.current_timestamp()))

df.write.mode("overwrite").format("delta").saveAsTable("workspace.hiring_bronze.employee_data")

# COMMAND ----------

print(spark.table("workspace.hiring_bronze.employee_data").count())