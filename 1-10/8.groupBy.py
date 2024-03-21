# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC I need someone's help to understand groupby. I don't think I'm really good at it.

# COMMAND ----------

# Method 1 - when groupBy used, must use an aggregate function

df.groupBy('empid').max('salary').show()

# COMMAND ----------

# Method 2 - You can add as many columns as you wish in GroupBy

df.groupBy('empid', 'empname', 'department').max('salary').show()

# COMMAND ----------

# Method 3 - if you want to add multiple aggregations then use agg and above method doesn't work.

df.groupBy('empid', 'empname','department').agg(
    min('salary').alias('min_salary'), 
    max('salary').alias('max_salary')
    ).orderBy('empid').show()
