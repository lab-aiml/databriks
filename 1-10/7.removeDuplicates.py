# Databricks notebook source
from pyspark.sql.functions import col

df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

# Method 1 - Distinct

df.distinct().orderBy(col('empid')).show()

# COMMAND ----------

# Method 2 - dropDuplicates

df.dropDuplicates().orderBy('empid').show()

# COMMAND ----------

# Method 3 - Use columns in duplicates in dropDuplicates

df.dropDuplicates(["empid","empname"]).orderBy('empid').show()

# COMMAND ----------


