# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# MAGIC %run ./use_case_fcb_dn_supply_margin
