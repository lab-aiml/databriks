# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

colRenameDF = df.withColumnRenamed('empid', 'Emp_Id').withColumnRenamed('empname', 'Emp_Name').withColumnRenamed('gender','Gender').withColumnRenamed('salary', 'Salary').withColumnRenamed('department', 'Department')
colRenameDF.show()

# COMMAND ----------

groupByDF = colRenameDF.groupBy('Emp_Id','Department').agg(
    min('Salary').alias('Min_Salary'),
    max('Salary').alias('Max_Salary')
).orderBy('Emp_Id')

groupByDF.show()

# COMMAND ----------

groupByDF.write.mode('overwrite').csv('dbfs:/mnt/blbstr/output/EmployeeData.csv') # mode = overwrite / append

# COMMAND ----------

spark.read.format('csv').option('inferschema','true').option('delimiter',',').load('dbfs:/mnt/blbstr/output/EmployeeData.csv/part-00000-tid-6815672049609253921-145f90a3-b3f8-40b7-ad74-66b9dace44ea-91-1-c000.csv').show()

# COMMAND ----------

display(dbutils.fs.ls('/mnt/blbstr/output/EmployeeData.csv/'))

# COMMAND ----------


