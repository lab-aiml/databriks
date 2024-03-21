# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS practise

# COMMAND ----------

# MAGIC %sql
# MAGIC USE practise

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students(
# MAGIC   id INT,
# MAGIC   name STRING
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO students
# MAGIC SELECT 1 AS id, 'Srujan' AS name

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students

# COMMAND ----------

# MAGIC %md
# MAGIC Write above SQL in PySpark

# COMMAND ----------

spark.sql("CREATE DATABASE IF NOT EXISTS practise2")
spark.sql("USE practise2")

# COMMAND ----------

spark.sql("""
          CREATE TABLE students
          (
              id INT,
              name STRING
          )
          """)

# COMMAND ----------

spark.sql("""
          INSERT INTO students
          SELECT 1 AS id, 'Srujan' AS name
          """)

# COMMAND ----------

spark.sql("SELECT * FROM students").collect()


# COMMAND ----------


