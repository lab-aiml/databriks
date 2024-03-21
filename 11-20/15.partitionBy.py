# Databricks notebook source
df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

# Write a file into the partition using PartitioBy
df.write.option('header','true').partitionBy('department').mode('overwrite').csv('dbfs:/mnt/blbstr/output/partitionBy')

# COMMAND ----------

# read the CSV file all records
readPath = spark.read.option('header','true').csv('dbfs:/mnt/blbstr/output/partitionBy')
readPath.show()

# COMMAND ----------

# show only IT department
# NOTE: if you do so, you will not comeacross department column, as this is illogical
readIT = spark.read.option('header','true').csv('dbfs:/mnt/blbstr/output/partitionBy/department=IT')
readIT.show()

# COMMAND ----------

# PartitionBy on multiple columns

df.write.mode('overwrite').option('header','true').partitionBy('department', 'empid').csv('dbfs:/mnt/blbstr/output/multiPartitionBy/')

# COMMAND ----------


