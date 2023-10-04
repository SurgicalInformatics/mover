library(tidyverse)
library(scales)
theme_set(theme_bw())

waves_times = read_csv("/home/common/mover_data/surginf_cleaned/waveform_filenames.csv")

waves_times %>% 
  filter(pat_id_extra == "IP") %>% 
  select(pat_id, filegroup_id, filepath, filename, datetime, type = pat_id_extra) %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/pat_id_filegroup_id_filelists.csv")
  

# we are using the flowsheet as the link between waveform data and patient demographics/complications
# as waveform have pat_id, flowsheet have pat_id, log_id, mrn, demographics have log_id, mrn
flowsheet_times = read_csv("/home/common/mover_data/surginf_cleaned/flowsheet_time_start_end_PAT_ID.csv") %>% 
  janitor::clean_names() %>% 
  # weird duplicated or_case_id and log_ids here so removing for simplicity
  select(-or_case_id) %>% 
  distinct(pat_id, start_time, .keep_all = TRUE) %>% 
  rename(flowsheet_start = start_time, flowsheet_end = end_time)

demographics      = read_csv("/home/common/mover_data/surginf_cleaned/patient_information_cleaned.csv", guess_max = Inf)
complications     = read_csv("/home/common/mover_data/surginf_cleaned/complications_cleaned.csv", guess_max = Inf)
#procedure_events  = read_csv("/home/common/mover_data/surginf_cleaned/patient_procedure_events_cleaned.csv", guess_max = Inf)

waves_oneline = waves_times %>% 
  filter(pat_id_extra == "IP") %>% 
  select(pat_id, filegroup_id, datetime) %>% 
  summarise(.by = c(pat_id, filegroup_id),
            n_files = n(),
            wave_start = min(datetime),
            wave_end   = max(datetime)) %>% 
  # each file is 30 minutes, so if a single file the duration is 30 minutes hence adding it on
  mutate(wave_duration  = (difftime(wave_end, wave_start) + dhours(0.5)) %>% 
           as.numeric(units = "hours"))

# if the waveform start time is within 25h of OR start time
# generally waveform data starts ~6h after OR data
cutoff = 25

timings = waves_oneline %>% 
  left_join(flowsheet_times) %>% 
  drop_na() %>% 
  left_join(select(demographics, log_id, mrn, in_or_dttm, out_or_dttm)) %>% 
  relocate(wave_start, .after = in_or_dttm) %>% 
  relocate(wave_end, .after = out_or_dttm) %>% 
  mutate(wave_in_or  = difftime(wave_start, in_or_dttm, unit = "hours"),
         wave_out_or = difftime(wave_end, out_or_dttm, unit = "hours")) %>% 
  mutate(wave_within_cutoff = (abs(wave_in_or) < cutoff & abs(wave_out_or) < cutoff))

timings %>% 
  filter(wave_within_cutoff) %>% 
  select(-wave_within_cutoff) %>% 
  write_csv("/home/common/mover_data/surginf_cleaned/pat_id_log_id_mrn_lookup.csv")

# waveform generally starts 6-7h after 'OR in'
# a very small number of instances where waveform precedes OR time (negative difference)
timings %>% 
  filter(wave_within_cutoff) %>% 
  ggplot(aes(wave_in_or)) +
  geom_histogram() +
  facet_wrap(~wave_in_or > 0, scales = "free")

# waveform data generally starts in the afternoon or evening
timings %>% 
  ggplot(aes(hour(wave_start))) +
  geom_bar() +
  facet_wrap(~wave_within_cutoff)

# whereas OR starts mostly during working day
timings %>% 
  ggplot(aes(hour(in_or_dttm))) +
  geom_bar() +
  facet_wrap(~wave_within_cutoff)

# The waves that start hundreds of days earlier are likely previous procedures 
timings %>% 
  ggplot(aes(wave_in_or/24)) +
  geom_histogram()


# between 5-10 procedures per day
timings %>% 
  filter(year(in_or_dttm) == 2019) %>% 
  filter(month(in_or_dttm) == 1) %>% 
  ggplot(aes(format(in_or_dttm, format = "%y-%m-%d"))) +
  geom_bar() +
  coord_flip()

timings %>% 
  count(wave_within_cutoff)

