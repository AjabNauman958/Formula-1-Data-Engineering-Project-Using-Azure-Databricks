-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/databrickscoursedls2/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.notebook.exit("Success")