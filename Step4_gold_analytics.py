# Databricks notebook source
# MAGIC %md
# MAGIC ##  Load Silver

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("workspace.hiring_silver.employee_clean")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Hiring trend (How many people joined each year, by department?)

# COMMAND ----------

hiring_trend = (silver
  .groupBy("StartYear", "DepartmentType")
    .agg(F.count("*").alias("hires"))
      .orderBy("StartYear", "DepartmentType"))

hiring_trend.write.mode("overwrite").format("delta").saveAsTable("workspace.hiring_gold.hiring_trend")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Attrition (Which departments are losing the most people?)

# COMMAND ----------

attrition = (silver
  .groupBy("DepartmentType", "BusinessUnit")
   .agg(F.count("*").alias("total_employees"),
    F.sum(F.col("IsTerminated").cast("int")).alias("exits"),
    F.round(F.avg("TenureYears"), 1).alias("avg_tenure_years"))
    .withColumn("attrition_rate_pct", F.round(F.col("exits") / F.col("total_employees") * 100, 1)))

attrition.write.mode("overwrite").format("delta").saveAsTable("workspace.hiring_gold.attrition")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performance (How are departments performing?)

# COMMAND ----------

performance = (silver
  .groupBy("DepartmentType", "Performance_Score")
    .agg(F.count("*").alias("employees"),
    F.round(F.avg("Current_Employee_Rating"), 2).alias("avg_rating")))

performance.write.mode("overwrite").format("delta").saveAsTable("workspace.hiring_gold.performance")

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.hiring_gold"))
display(spark.table("workspace.hiring_gold.hiring_trend").limit(10))
display(spark.table("workspace.hiring_gold.attrition").limit(10))
display(spark.table("workspace.hiring_gold.performance").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC