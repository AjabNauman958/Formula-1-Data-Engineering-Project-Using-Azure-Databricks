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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 2 - Read the lap times csv files in one dataframe

# COMMAND ----------

lap_times_df = spark.read.schema(lap_times_schema) \
                        .option("header", True) \
                        .csv(f"{raw_folder_path}/{v_file_date}/lap_times")
                        # .csv("/mnt/databrickscoursedls2/raw/lap_times/lap_times_split*.csv") using wildcards

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step - 3 Rename required columns & add ingestion date

# COMMAND ----------

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                                    .withColumnRenamed("raceId", "race_id") \
                                                    .withColumn("data_source", lit(v_data_source)) \
                                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step - 4 Write data into the data lake as parquet

# COMMAND ----------

# overwrite_partition(lap_times_final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

dbutils.notebook.exit("Success")