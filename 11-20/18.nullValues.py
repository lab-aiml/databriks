# Databricks notebook source
empDF = spark.read.option('header','true').csv('dbfs:/mnt/blbstr/EmpData.csv')
display(empDF)

# COMMAND ----------

empDF.na.fill('None','empname').show()

# COMMAND ----------

empIntDF = empDF.withColumn('empid', empDF.empid.cast('Int')).\
    withColumn('salary', empDF.salary.cast('Int'))

empIntDF.printSchema()

# COMMAND ----------

empIntDF = empIntDF.fillna(0, ['empid', 'salary'])
empIntDF.show()

# COMMAND ----------

empStrDF = empDF.fillna('None', ['empname', 'gender'])
empIntNa = empStrDF.withColumn('empid', empStrDF.empid.cast('int')).\
                    withColumn('salary',empStrDF.salary.cast('int'))
emp = empIntNa.fillna(0, ['empid','salary'])
display(emp)

# COMMAND ----------


