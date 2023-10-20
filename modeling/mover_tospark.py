import numpy as np
import pyspark
import pandas
from pyspark.sql import SparkSession
from pyspark.sql.types import *

# source /home/rots/.local/share/r-miniconda/envs/r-reticulate/bin/activate
# import pyjanitor

spark = SparkSession.builder.appName("mover").config("spark.driver.memory", "32g").getOrCreate()

# df = spark.read.csv("/home/common/mover_data/baldig/bioprojects2/mirana_private/flowsheets_cleaned/", header=True, inferSchema=True)
# df = spark.read.csv("/home/common/mover_data/srv/disk00/EPIC_flowsheets/1_2018-4633_flowsheets_20211006.csv", header=True, inferSchema=True)
df = spark.read.csv("/home/common/mover_data/srv/disk00/MOVER/EPIC/EMR/patient_labs.csv", header=True, inferSchema=True)

df = df.drop("_c0")
df = df.dropDuplicates()

df = df.orderBy(["MRN", "LOG_ID", "Collection Datetime"])
# these colnames are for patient_labs.csv
df = df.withColumnRenamed("LOG_ID", "log_id")
df = df.withColumnRenamed("MRN", "mrn")
df = df.withColumnRenamed("ENC_TYPE_NM", "enc_type_nm")
df = df.withColumnRenamed("Lab Code", "lab_code")
df = df.withColumnRenamed("Lab Name", "lab_name")
df = df.withColumnRenamed("Observation Value", "obs_value")
df = df.withColumnRenamed("Measurement Units", "meas_unit")
df = df.withColumnRenamed("Reference Range", "ref_range")
df = df.withColumnRenamed("Abnormal Flag", "abnormal_flag")
df = df.withColumnRenamed("Collection Datetime", "datetime")

#df.write.parquet("/home/common/mover_data/spark_vital/", mode="overwrite")
#df.write.parquet("/home/common/mover_data/spark_full_flowsheet/", mode="overwrite")
df.write.parquet("/home/common/mover_data/spark_patient_labs/", mode="overwrite")

spark.stop()
