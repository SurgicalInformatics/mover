library(tidyverse)
library(sparklyr)
library(ggbeeswarm)


conf <- spark_config()
conf$sparklyr.cores.local = 4
conf$`sparklyr.shell.driver-memory` <- "48G"
conf$spark.memory.fraction = 0.9

sc = spark_connect(master = "local",
                   app_name = "mover",
                   version = "3.4.0",
                   config = conf)

#flowsheet = spark_read_parquet(sc, "flowsheet", path = "/home/common/mover_data/spark_full_flowsheet/")
flowsheet = spark_read_parquet(sc, "flowsheet", path = "/home/common/mover_data/spark_vital/")

spo2_below90 = flowsheet %>% 
  filter(FLO_DISPLAY_NAME == "SpO2") %>% 
  filter(RECORD_TYPE != "PRE-OP") %>% 
  filter(MEAS_VALUE < 90) %>% 
  collect()

spo2_any = flowsheet %>% 
  filter(FLO_DISPLAY_NAME == "SpO2") %>% 
  filter(RECORD_TYPE != "PRE-OP") %>% 
  distinct(LOG_ID, MRN, RECORD_TYPE) %>% 
  collect()


spo2_below90 = janitor::clean_names(spo2_below90) %>% 
  distinct() %>% 
  # mutate(flo_name = str_trim(flo_name),
  #        meas_value = parse_number(meas_value)) %>% 
  group_by(log_id, mrn, record_type) %>% 
  slice_min(recorded_time, with_ties = FALSE)


spo2 = full_join(janitor::clean_names(spo2_any), spo2_below90)

spo2_postop = spo2 %>% 
  filter(record_type == "POST-OP")

spo2_intraop = spo2 %>% 
  filter(record_type == "INTRA-OP")

# check that log_id and mrn unique
spo2_intraop %>% 
  count(log_id, mrn, sort = TRUE)

write_csv(spo2_postop, "/home/common/mover_data/surginf_cleaned/spo2_postop_below90.csv")
write_csv(spo2_intraop, "/home/common/mover_data/surginf_cleaned/spo2_intraop_below90.csv")

spo2_below90 %>% 
  count(flo_name, sort = TRUE)

# this plot takes 10+ minutes to render
# spo2_below90 %>% 
#   filter(flo_name %in% c("Vital Signs", "Devices Testing Template")) %>% 
#   ggplot(aes(flo_name, meas_value)) +
#   facet_wrap(~flo_name) +
#   #geom_jitter(alpha = 0.1) +
#   geom_beeswarm(alpha = 0.1) +
#   coord_flip()

med = spo2_below90 %>% 
  summarise(.by = flo_name,
            med_spo2 = median(meas_value, na.rm = TRUE)) %>% 
  arrange(med_spo2)
