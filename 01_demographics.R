library(tidyverse)
library(scales)
theme_set(theme_bw())


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
  mutate(asa_rating = fct_reorder(asa_rating, asa_rating_c))


# n_distinct(demographics_orig$MRN)
# n_distinct(demographics_orig$LOG_ID)

# check that no more duplicate procedures
 stopifnot((demographics %>% 
              count(log_id, mrn, sort = TRUE) %>% 
              pull(n) %>% 
              max()) == 1)


 
all_procedures = demographics %>% 
   count(primary_procedure_nm, sort = TRUE)
 