# Databricks notebook source
# MAGIC %md
# MAGIC There are two ways you can rename the column

# COMMAND ----------

# Read the csv file
df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

#Method 1

dfRename1 = df.withColumnRenamed("empid", "Emp_ID").withColumnRenamed("empname", "Emp_Name")
display(dfRename1)


# COMMAND ----------

# Method 2 - in this method one must list all columns.

dfRename2 = df.selectExpr("empid as Emp_ID", "empname as Emp_Name", "gender", "salary", "department")
display(dfRename2)

# COMMAND ----------

# Method 3
from pyspark.sql.functions import col

dfRename3 = df.select(col("empid").alias("Emp_Id"), col("empname"), col('gender'), col('salary'), col('department'))
dfRename3.show()

# COMMAND ----------


