# Databricks notebook source
# read CSV file - 'dbfs:/mnt/blbstr/EmployeeData.csv'
empDF = spark.read.format("csv").option('delimeter',",").option("inferSchema","true").option("header","true").load('dbfs:/mnt/blbstr/EmployeeData.csv')
empDF.show()

# COMMAND ----------

from pyspark.sql.functions import col, lit

# COMMAND ----------

# Method 1 - Best option

dfRename1 = empDF.withColumn("Country", lit("India"))
dfRename1.show()

# COMMAND ----------

# Method 2 
dfRename2 = empDF.withColumn("Salary", col('empid')*col('salary')).withColumn("Country",lit("India"))
dfRename2.show()

# COMMAND ----------

# Method 3 
dfRename3 = empDF.select(col("empid"), col("empname"), lit("India").alias("Country"))
dfRename3.show()

# COMMAND ----------


