# Databricks notebook source
# Create dataframe 1

dataSet1 = [(1, 'Srujan', 'MBA'),\
            (2, 'Swetha', 'MSc') ]

col1 = ['id', 'Name', 'Qualifications']

firstFrame = spark.createDataFrame(dataSet1, col1)
firstFrame.show()

# COMMAND ----------

dataset2 = [(3, 'Kanni', 'Year 7'),\
            (4, 'Jay', 'Yaer R'), \
            (4, 'Jay', 'Yaer R')
            ]

col2 = ['id', 'name', 'Education']

secondFrame = spark.createDataFrame(dataset2, col2)
secondFrame.show()

# COMMAND ----------

# Union both frames - Columns names are abstracted from firstFrame (default)

firstFrame.union(secondFrame).distinct().show()

# COMMAND ----------


