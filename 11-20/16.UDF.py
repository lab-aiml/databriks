# Databricks notebook source
df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

# Write a User defined function

def convertCase(string):
    charConc = ''
    for chr in string:
        if chr in ('abcdefghijklmnopqrstuvwxyz'):
            charConc += chr.upper()
        else:
            charConc += chr.lower()

    return charConc

# COMMAND ----------

from pyspark.sql.functions import col, udf
convertCaseDF = udf(convertCase)

# COMMAND ----------

df.select(convertCaseDF(col('empname')).alias('empname'), convertCaseDF(col('gender')).alias('gender')).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from practise.students

# COMMAND ----------

# register for SQL query
spark.udf.register('convertCaseSql', convertCase)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *, convertCaseSql(name) AS name FROM practise.students

# COMMAND ----------


