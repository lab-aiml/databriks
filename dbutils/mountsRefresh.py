# Databricks notebook source
#list all mount points
display(dbutils.fs.mounts())

# COMMAND ----------

#refresh all mounts
dbutils.fs.refreshMounts()

# COMMAND ----------


