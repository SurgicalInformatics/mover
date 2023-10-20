library(tidyverse)
library(scales)
theme_set(theme_bw())
library(sparklyr)

conf <- spark_config()
conf$sparklyr.cores.local = 4
conf$`sparklyr.shell.driver-memory` <- "48G"
conf$spark.memory.fraction = 0.9

sc = spark_connect(master = "local",
                   app_name = "mover",
                   version = "3.4.0",
                   config = conf)

labs_orig = spark_read_parquet(sc, "labs", path = "/home/common/mover_data/spark_patient_labs/")

labs_list = labs_orig %>% 
  distinct(lab_name) %>% 
  collect()
# Urea
# Lactate dehydrogenase
# Platelets
# Creatine kinase

labs = labs_orig %>% 
  filter(lab_name %in% c("Urea", "Lactate dehydrogenase", "Creatine kinase", "Platelets")) %>% 
  collect()

write_csv(labs, "/home/common/mover_data/surginf_cleaned/labs_allobs_main4_allobs.csv")


labs_counts = labs %>% 
  count(log_id, mrn, lab_name, abnormal_flag) %>% 
  group_by(log_id, mrn, lab_name) %>% 
  mutate(any_high = any(abnormal_flag %in% c("H", "HH"))) %>% 
  mutate(any_low = any(abnormal_flag %in% c("L", "LL"))) %>% 
  mutate(all_normal = all(abnormal_flag == "N")) %>% 
  ungroup()

# check that if all_normal, none are high or low:
stopifnot(
0 == labs_counts %>% 
  filter(all_normal & (any_high | any_low)) %>% 
  nrow())

high_and_low = labs_counts %>% 
  filter(any_high & any_low)


labs_summarised = labs_counts %>% 
  distinct(log_id, mrn, lab_name, any_high, any_low, all_normal)

write_csv(labs_summarised, "/home/common/mover_data/surginf_cleaned/labs_summarised_all4.csv")

c("Urea", "Lactate dehydrogenase", "Creatine kinase", "Platelets")

labs_summarised %>% 
  filter(lab_name == "Urea") %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/labs_summarised_urea.csv")

labs_summarised %>% 
  filter(lab_name == "Lactate dehydrogenase") %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/labs_summarised_lact_dehydro.csv")

labs_summarised %>% 
  filter(lab_name == "Creatine kinase") %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/labs_summarised_creat_kin.csv")

labs_summarised %>% 
  filter(lab_name == "Platelets") %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/labs_summarised_platelets.csv")
