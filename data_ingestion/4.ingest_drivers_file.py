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
# MAGIC #### Step 1 - Define name schema for the file

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [
                                    StructField("forename", StringType(), True),
                                    StructField("surname", StringType(), True)
])

# COMMAND ----------

drivers_schema = StructType(fields = [
                                      StructField("driverId", IntegerType(), True),
                                      StructField("driverRef", StringType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("code", StringType(), True),
                                      StructField("name", name_schema),
                                      StructField("dob", DateType(), True),
                                      StructField("nationality", StringType(), True),
                                      StructField("url", StringType(), True)
])

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 - Read the driver.json file

# COMMAND ----------

drivers_df = spark.read.option("header", True) \
                            .schema(drivers_schema) \
                            .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Rename the required columns & Add new columns

# COMMAND ----------

from pyspark.sql.functions import concat, col, lit

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

drivers_with_ingestion_date_df = add_ingestion_date(drivers_renamed_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 - Drop the unwanted columns

# COMMAND ----------

drivers_final_df = drivers_with_ingestion_date_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Write the data into data lake as parquet

# COMMAND ----------

""" 
--> To Write data without creating managed tables into data lake
drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers") """

""""
# --> To Write data as a managed table into data lake
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers") """

# --> To Write data as a managed table into delta lake
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

dbutils.notebook.exit("Success")