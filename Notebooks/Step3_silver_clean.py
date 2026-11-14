# Databricks notebook source
# MAGIC %md
# MAGIC ## Load the Bronze table and clean text

# COMMAND ----------

from pyspark.sql import functions as F

df = spark.table("workspace.hiring_bronze.employee_data")

for c in df.columns:
    df = df.withColumnRenamed(c, c.strip().replace(" ", "_"))

for c, t in df.dtypes:
        if t == "string":
           df = df.withColumn(c, F.trim(F.col(c)))

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Fix dates and unknown values

# COMMAND ----------

df = (df
  .withColumn("StartDate", F.to_date("StartDate", "dd-MMM-yy"))
  .withColumn("ExitDate",  F.to_date("ExitDate",  "dd-MMM-yy"))
  .withColumn("DOB",       F.to_date("DOB",       "dd-MM-yyyy"))
  .withColumn("Current_Employee_Rating", F.col("Current_Employee_Rating").cast("int"))
  .withColumn("TerminationType",
  F.when(F.col("TerminationType") == "Unk", "Not Terminated")
  .otherwise(F.col("TerminationType")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add new columns and save the Silver table

# COMMAND ----------

df = (df
  .withColumn("StartYear", F.year("StartDate"))
  .withColumn("IsTerminated", F.col("ExitDate").isNotNull())
  .withColumn("TenureYears", F.round(F.greatest(F.lit(0.0),
  F.datediff(F.coalesce("ExitDate", F.current_date()), "StartDate") / 365.25), 1))
   .withColumn("Age", F.floor(F.datediff(F.current_date(), "DOB") / 365.25))
   .dropDuplicates(["EmpID"])
   .filter(F.col("EmpID").isNotNull())
   .drop("ADEmail", "DOB")
)

df.write.mode("overwrite").format("delta").saveAsTable("workspace.hiring_silver.employee_clean")

# COMMAND ----------

silver = spark.table("workspace.hiring_silver.employee_clean")
print(silver.count())
silver.printSchema()
display(silver.limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
silver = spark.table("workspace.hiring_silver.employee_clean")
silver.select(F.min("StartDate"), F.max("StartDate")).show()
silver.filter(F.col("StartYear") == 2023).groupBy(F.month("StartDate").alias("month")).count().orderBy("month").show()