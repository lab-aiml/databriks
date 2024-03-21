# Databricks notebook source
df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

#Method 1 
df.sort(df.salary.desc()).show()

# COMMAND ----------

# Method 2 - Multi Columns

df.sort(df.empname.desc(), df.salary.desc()).show()

# COMMAND ----------

# Method 3 - orderBy

df.orderBy(df.department.desc()).show()

# COMMAND ----------

# Method 4 - Using Col

df.sort(col('salary').desc(), col('gender').desc()).show()


# COMMAND ----------


