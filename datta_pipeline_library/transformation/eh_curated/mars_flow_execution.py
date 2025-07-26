# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %run ./common_functions

# COMMAND ----------

# MAGIC %run ./mars_sql_base_tables

# COMMAND ----------

# MAGIC %run ./mars_sql_data_dump_collection

# COMMAND ----------

# MAGIC %run ./mars_sql_margin_bucket_collection
