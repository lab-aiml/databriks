# Databricks notebook source
from pyspark.sql.functions import *

df = spark.read.format('csv').option('inferSchema',"true").option('header','true').option('delimiter',',').load('dbfs:/mnt/blbstr/EmployeeData.csv')
display(df)

# COMMAND ----------

# Write the DF in Parquet file
# Drawback - this creates a single parquet file and that consumes a lot of resources to process, hence partitionBy/repartition is recommended.

writeParquet = df.write.format('parquet').mode('overwrite').save('dbfs:/mnt/blbstr/output/parquetOutput')

# COMMAND ----------

# create repartition of parquet file
repartFile = df.repartition(5).write.format('parquet').mode('overwrite').save('dbfs:/mnt/blbstr/output/parquetOutput')

# COMMAND ----------

# PartitionBy a column instead of random split

partByCol = df.write.format('parquet').mode('overwrite').partitionBy('department').save('dbfs:/mnt/blbstr/output/partBy')

# COMMAND ----------

# Split records further alongside partitionBy to reduce the record count

df.repartition(2).write.format('parquet').mode('overwrite').partitionBy('department').save('dbfs:/mnt/blbstr/output/partPartBy')

# COMMAND ----------


