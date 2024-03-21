# Databricks notebook source
df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)


# COMMAND ----------

# Method 1 - Using df.columnName

from pyspark.sql.functions import col

salDF = df.filter(df.salary > 9000)
display(salDF)

# COMMAND ----------

# Method 2 - Using col

namesDF = df.filter(
    (col('gender')=='Female') | 
    (col('empname')=='jacob')
    )
display(namesDF)

# COMMAND ----------

df.filter(col('salary') > 13000).show()

# COMMAND ----------


