# A script to examine patient procedure events record data from MOVER
#
#

library(tidyverse)

# Read patient procedure events
patient_procedure_events <- read_csv("/home/common/mover_data/srv/disk00/MOVER/EPIC/EMR/patient_procedure events.csv", 
                                     col_types = cols(NOTE_TEXT = col_character())) %>% 
  janitor::clean_names() 
