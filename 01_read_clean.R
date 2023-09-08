# A script to examine patient record data from MOVER
#
#

library(tidyverse)

# Read patient procedure events
patient_procedure_events <- read_csv("/home/common/mover_data/srv/disk00/MOVER/EPIC/EMR/patient_procedure events.csv", 
                                     col_types = cols(NOTE_TEXT = col_character())) %>% 
  janitor::clean_names() 

# Read patient information
patient_information = read_csv("/home/common/mover_data/srv/disk00/MOVER/EPIC/EMR/patient_information.csv") %>% 
  janitor::clean_names()  

# Read complications
complications_cleaned <- read_csv("/home/common/mover_data/surginf_cleaned/complications_cleaned.csv")

# Add height in cm to the patient information (the original is a string of ft and inches)
hcm = patient_information %>% 
  select(height) %>% 
  separate(height, into = c("H1", "H2"), sep = "'", convert = T) %>% 
  transmute(height_cm = H1 * 30.48 + H2 * 2.54)

patient_information = bind_cols(patient_information, hcm)

