library(tidyverse)

flowsheet_orig = read_csv("/home/common/mover_data/baldig/bioprojects2/mirana_private/flowsheets_cleaned/flowsheet_part3.csv",
                     #n_max = 20000,
                     col_types = cols(.default = "c"))

flowsheet = janitor::clean_names(flowsheet_orig) %>% 
  mutate(recorded_time = ymd_hms(recorded_time)) %>% 
  distinct()

flowsheet %>% 
  count(record_type, sort = TRUE)

flowsheet %>% 
  count(record_type, flo_name, sort = TRUE)

flo_display_names = flowsheet %>% 
  count(flo_display_name, sort = TRUE)

flo_display_names %>%
  mutate(flo_names = janitor::make_clean_names(flo_display_name))

flowsheet2_min = flowsheet2 %>% 
  group_by(mrn, log_id) %>% 
  summarise(time_min = min(recorded_time), time_max = max(recorded_time))


