library(tidyverse)
library(sparklyr)


conf <- spark_config()
conf$sparklyr.cores.local = 4
conf$`sparklyr.shell.driver-memory` <- "48G"
conf$spark.memory.fraction = 0.9

sc = spark_connect(master = "local",
                   app_name = "mover",
                   version = "3.4.0",
                   config = conf)

flowsheet = spark_read_parquet(sc, "flowsheet", path = "/home/common/mover_data/spark_vital/")