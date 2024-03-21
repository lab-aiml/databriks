# Databricks notebook source
from pyspark.sql.functions import *

dataSet = [('Srujan', 'CSE', 'UP', 80),\
       ('Swetha', 'Mechanical', 'AP', 85),\
       ('Kanni', 'Civil', 'TG', 90),\
       ('Jay', 'CSE', 'UP', 80)
    ]

col = ['Student_name', 'Branch', 'State', 'Marks']

students = spark.createDataFrame(dataSet, col)
students.show()

# COMMAND ----------

stateDF = students.withColumn('States', when(students.State == 'UP', 'Uttar Pradesh')\
                                      .when(students.State == 'AP', 'Andhra Pradesh')\
                                      .when(students.State == 'TG', 'Telangana')\
                                      .otherwise('Unknown'))
stateDF.show()

# COMMAND ----------


