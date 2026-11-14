# Databricks notebook source
from pyspark.sql import functions as F

silver = spark.table("workspace.hiring_silver.employee_clean")

checks = {
  "no_null_empid":       silver.filter(F.col("EmpID").isNull()).count() == 0,
   "no_duplicate_empid":  silver.count() == silver.select("EmpID").distinct().count(),
   "start_date_not_null": silver.filter(F.col("StartDate").isNull()).count() == 0,
   "exit_after_start":    silver.filter(F.col("ExitDate") < F.col("StartDate")).count() == 0,
   "rating_between_1_5":  silver.filter(~F.col("Current_Employee_Rating").between(1, 5)).count() == 0,
   "has_rows":            silver.count() > 0,
}

for name, ok in checks.items():
    print("PASS" if ok else "FAIL", name)

failed = [n for n, ok in checks.items() if not ok]
if failed:
    raise Exception(f"Quality checks failed: {failed}")