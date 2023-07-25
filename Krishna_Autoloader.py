# Databricks notebook source
# MAGIC %md
# MAGIC ###Ingestion
# MAGIC 1. Autoloader
# MAGIC 2. COPY INTO
# MAGIC 3. Delta Live Tables

# COMMAND ----------

from pyspark.sql.functions import *
import datetime

input_file_path="dbfs:/FileStore/streaminput/"
output_file_path="dbfs:/FileStore/streamoutput/autoloader/krishna_al"

# COMMAND ----------

# InferColumn Types
df=spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",f"{output_file_path}/schemalocation2")\
    .option("cloudFiles.inferColumnTypes",True)\
    .load(f"{input_file_path}")\
    .writeStream\
    .option("checkpointLocation",f"{output_file_path}/checkpoint2")\
    .option("path",f"{output_file_path}/output2")\
    .table("krishna.autoloader2")

# COMMAND ----------

# Schema Evolution - rescue

df=spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",f"{output_file_path}/schemalocation7")\
    .option("cloudFiles.inferColumnTypes",True)\
    .option("cloudFiles.schemaEvolutionMode","rescue")\
    .load(f"{input_file_path}")\
    .writeStream\
    .option("checkpointLocation",f"{output_file_path}/checkpoint7")\
    .option("path",f"{output_file_path}/output7")\
    .option("mergeSchema", True)\
    .trigger(processingTime = "1 minute")\
    .table("krishna.autoloader7")

# COMMAND ----------

# Schema Evolution - addNewColums

df=spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",f"{output_file_path}/schemalocation6")\
    .option("cloudFiles.inferColumnTypes",True)\
    .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
    .option("mergeSchema", True)\
    .load(f"{input_file_path}")\
    .writeStream\
    .option("checkpointLocation",f"{output_file_path}/checkpoint6")\
    .option("path",f"{output_file_path}/output5")\
    .trigger(processingTime = "1 minute")\
    .table("krishna.autoloader6")

# COMMAND ----------

# Schema Evolution - addNewColums

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE krishna.copytest

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC COPY INTO krishna.copytest
# MAGIC FROM 'dbfs:/FileStore/streaminput/'
# MAGIC FILEFORMAT=csv
# MAGIC FORMAT_OPTIONS('header'='true')
# MAGIC COPY_OPTIONS('mergeSchema'='true');
# MAGIC
# MAGIC SELECT * from krishna.copytest; 

# COMMAND ----------

# Schema Evolution - rescue

df=spark.readStream\
    .format("cloudFiles")\
    .option("cloudFiles.format","csv")\
    .option("cloudFiles.schemaLocation",f"{output_file_path}/schemalocation10")\
    .option("cloudFiles.inferColumnTypes",True)\
    .option("cloudFiles.schemaEvolutionMode","rescue")\
    .load(f"{input_file_path}")\
    .writeStream\
    .option("checkpointLocation",f"{output_file_path}/checkpoint10")\
    .option("path",f"{output_file_path}/output10")\
    .option("mergeSchema", True)\
    .trigger(processingTime = "1 minute")\
    .table("krishna.autoloader10")

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------


