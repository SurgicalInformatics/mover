library(tidyverse)
library(pointblank)

flowsheet = read_csv("/home/common/mover_data/baldig/bioprojects2/mirana_private/flowsheets_cleaned/flowsheet_part1.csv",
                     n_max = 20000,
                     col_types = cols(.default = "c"))

#                     col_types = cols(RECORDED_TIME = col_datetime(format = "%Y-%m-%d %H:%M:%S")))


flowsheet2 = read_csv("/home/common/mover_data/baldig/bioprojects2/mirana_private/flowsheets_cleaned/flowsheet_part3.csv",
                     #n_max = 20000,
                     col_types = cols(.default = "c"))

flowsheet2 = janitor::clean_names(flowsheet2) %>% 
  mutate(recorded_time = ymd_hms(recorded_time))

flowsheet2_min = flowsheet2 %>% 
  group_by(mrn, log_id) %>% 
  summarise(time_min = min(recorded_time), time_max = max(recorded_time))

pointblank_test = flowsheet2 %>% 
  slice_sample(n = 500000) %>% 
  scan_data()

flowsheet2 %>% 
flowsheet2 %>% 
  mutate(datetime = ymd_hms(RECORDED_TIME), 
         hour = hour(datetime)) %>% 
  count(hour)



flowsheet3 = read_csv("/home/common/mover_data/srv/disk00/EPIC_flowsheets/1_2018-4633_flowsheets_20211006.csv",
                     n_max = 20000,
                     col_types = cols(.default = "c"))
