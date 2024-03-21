# Databricks notebook source
from pyspark.sql.functions import *

dataSet = [(1,'Srujan', 'CSE', 'UP', 80),\
       (2,'Swetha', 'Mechanical', 'AP', 85),\
       (3,'Kanni', 'Civil', 'TG', 90),\
       (4,'Jay', 'CSE', 'UP', 80)
    ]

col = ['Id','Student_name', 'Branch', 'State', 'Marks']

setDF1 = spark.createDataFrame(dataSet, col)
setDF1.show()

# COMMAND ----------

dataSet2 = [(1,'Srujan', 'CSE', 'UP', 80),\
       (2,'Swetha', 'Mechanical', 'AP', 85),\
       (4,'Jay', 'CSE', 'UP', 80)
    ]

col2 = ['Id','Student_name', 'Branch', 'State', 'Marks']
setDF2 = spark.createDataFrame(dataSet2, col2)
setDF2.show

# COMMAND ----------

# write an inner Join
setDF1.join(setDF2, \
            setDF1.Id == setDF2.Id, \
            'inner').show()

# COMMAND ----------

setDF1.join(setDF2,\
            setDF1.Id == setDF2.Id, \
            'left'
            ).show()

# COMMAND ----------

# Set alias for the tables
from pyspark.sql.functions import col
setDF1.alias('df1').join(setDF2.alias('df2'), \
                        col('df1.Id') == col('df2.Id'),\
                        'inner'
                        ).show()



# COMMAND ----------

# Select Columns from alias tables

setJoin = setDF1.alias('df1').join(setDF2.alias('df2'),\
                         col('df1.Id') == col('df2.Id'),\
                         'inner'
    )

setJoin.select(col('df1.Id'),\
               col('df1.Student_name'),\
               col('df2.Branch'),\
               col('df2.State'),\
               col('df2.Marks')
    ).show()
