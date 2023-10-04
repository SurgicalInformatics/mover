import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.appName("mover").config("spark.driver.memory", "32g").getOrCreate()

#df = spark.read.csv("/home/common/mover_data/baldig/bioprojects2/mirana_private/flowsheets_cleaned/", header=True, inferSchema=True)
df = spark.read.csv("/home/common/mover_data/srv/disk00/EPIC_flowsheets/1_2018-4633_flowsheets_20211006.csv", header=True, inferSchema=True)

df = df.drop("_c0")
df = df.dropDuplicates()

df = df.orderBy(["MRN", "LOG_ID", "RECORDED_TIME"])
#df.write.parquet("/home/common/mover_data/spark_vital/", mode="overwrite")
df.write.parquet("/home/common/mover_data/spark_full_flowsheet/", mode="overwrite")

spark.stop()
