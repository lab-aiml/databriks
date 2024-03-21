# Databricks notebook source
dbutils.fs.mount(
    source='wasbs://data@demostoragemar.blob.core.windows.net/',
    mount_point='/mnt/blbstrge',
    extra_configs={'fs.azure.account.key.demostoragemar.blob.core.windows.net':dbutils.secrets.get('dbScope', 'blobStorage')}
)

# COMMAND ----------

display(dbutils.fs.ls('/mnt/blbstrge'))

# COMMAND ----------

empDataDF = spark.read.csv('dbfs:/mnt/blbstrge/EmployeeData.csv', header=True)
display(empDataDF)

# COMMAND ----------


