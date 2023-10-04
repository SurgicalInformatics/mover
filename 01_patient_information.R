library(tidyverse)
library(scales)
theme_set(theme_bw())

# cleaning ----
demographics_orig = read_csv(paste0(Sys.getenv("epic_emr"), "patient_information.csv"),
                        col_types = cols(HOSP_ADMSN_TIME = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         HOSP_DISCH_TIME = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         SURGERY_DATE = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         IN_OR_DTTM = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         OUT_OR_DTTM = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         AN_START_DATETIME = col_datetime(format = "%m/%d/%y %H:%M"), 
                                         AN_STOP_DATETIME = col_datetime(format = "%m/%d/%y %H:%M")))


demographics_orig %>% 
  count(MRN, LOG_ID, sort = TRUE) %>% 
  count(LOG_ID, sort = TRUE)


demographics = demographics_orig %>% 
  janitor::clean_names() %>% 
  # I've checked that duplicates are of the same admission but one has typos etc
  distinct(log_id, .keep_all = TRUE) %>% 
  rename(age = birth_date) %>% 
  mutate(asa_rating = fct_reorder(asa_rating, asa_rating_c)) %>% 
  # Changing from ALL CAPS to lowercase
  mutate(primary_procedure_nm = str_to_sentence(primary_procedure_nm)) %>% 
  # Add height in cm to the patient information (the original is a string of ft and inches)
  separate(height, into = c("H1", "H2"), sep = "'", convert = T) %>% 
  mutate(height_cm = H1 * 30.48 + H2 * 2.54)

# demographics %>% 
#   ggplot(aes(height_cm)) +
#   geom_histogram()

# n_distinct(demographics_orig$MRN)
# n_distinct(demographics_orig$LOG_ID)

# check that no more duplicate procedures
 stopifnot((demographics %>% 
              count(log_id, mrn, sort = TRUE) %>% 
              pull(n) %>% 
              max()) == 1)
 
 write_csv(demographics, "/home/common/mover_data/surginf_cleaned/patient_information_cleaned.csv")
 
 # exploration ----
demographics = read_csv("/home/common/mover_data/surginf_cleaned/patient_information_cleaned.csv")

all_procedures = demographics %>% 
   count(primary_procedure_nm, sort = TRUE)

pointblank::scan_data(demographics) 
