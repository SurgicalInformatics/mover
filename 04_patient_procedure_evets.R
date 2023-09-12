# A script to examine patient procedure events record data from MOVER
#
#

library(tidyverse)

# Read patient procedure events
patient_procedure_events <- read_csv("/home/common/mover_data/srv/disk00/MOVER/EPIC/EMR/patient_procedure events.csv", 
                                     col_types = cols(NOTE_TEXT = col_character(),
                                                      EVENT_TIME = col_datetime(format = "%m/%d/%y %H:%M"))) %>% 
  janitor::clean_names() %>% 
  distinct()


patient_procedure_events %>% 
  count(event_display_name, sort = TRUE)


write_csv(patient_procedure_events, "/home/common/mover_data/surginf_cleaned/patient_procedure_events_cleaned.csv")

