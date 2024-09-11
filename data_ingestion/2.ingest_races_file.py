# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 1 - Create Schema for the file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType

# COMMAND ----------

races_schema = StructType(fields = [
    StructField("raceId", IntegerType(), False),
    StructField("year", IntegerType(), True),
    StructField("round", IntegerType(), True),
    StructField("circuitId", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("date", DateType(), True),
    StructField("time", StringType(), True),
    StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Read the races.csv file

# COMMAND ----------

races_df = spark.read.schema(races_schema) \
                        .option("header", True) \
                        .csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 3 - Add ingestion date & race_timestamp columns

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, to_timestamp, col

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('race_timestamp', \
                                to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
                                  .withColumn("data_source", lit(v_data_source)) \
                                  .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 4 - Select and rename required columns

# COMMAND ----------

races_selected_df = races_with_ingestion_date_df.select(col("raceId").alias("race_id"), 
                                                    col("year").alias("race_year"),
                                                    col("round"),
                                                    col("circuitId").alias("circuit_id"),
                                                    col("name"),
                                                    col("race_timestamp"),
                                                    col("ingestion_date"),
                                                    col("data_source"),
                                                    col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 - Write data into the data lake as parquet

# COMMAND ----------

""" 
--> To Write data without creating managed tables into data lake
races_selected_df.write.mode("overwrite").partitionBy("race_year").parquet(f"{processed_folder_path}/races") """

"""
# --> To Write data as a managed table into data lake
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races") """

# --> To Write data as a managed table in delta lake
races_selected_df.write.mode("overwrite").partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

dbutils.notebook.exit("Success")