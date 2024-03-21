# Databricks notebook source
df = spark.read.format('csv').option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

#Method 1
df = df.withColumn('empid', df.empid.cast('Int')).\
        withColumn('salary', df.salary.cast('Double'))
display(df)

# COMMAND ----------

# Method 2
from pyspark.sql.functions import col

selectDF = df.select(col('empid').cast('Int'),\
                     col('empname'),\
                     col('gender'),\
                     col('salary').cast('Double'),\
                     col('department')
    )
display(selectDF)

# COMMAND ----------

# Method 3

exprDF = df.selectExpr('cast(empid as Int)', 'empname', 'gender', 'cast(salary as Double)', 'department')
display(exprDF)

# COMMAND ----------


