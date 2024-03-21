# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

dbutils.fs.ls('/mnt/blbstr')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.createOrReplaceTempView('students')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students
