library(tidyverse)

load("waves_filenames.rda")


waves_times = waves_filenames %>% 
  # waveform files are in 4 parts/folders, but many files are duplicated so removing duplicates
  mutate(part = str_extract(files, "part.")) %>% 
  arrange(part) %>% 
  distinct(filename, .keep_all = TRUE) %>% 
  # pat_id padded with IP or CB to denote different file types
  # extracting and removing
  mutate(pat_id_extra = str_extract(PAT_ID, ".{2}$")) %>% 
  mutate(PAT_ID = str_remove(PAT_ID, ".{2}$")) %>% 
  # rename variables
  rename(pat_id = PAT_ID) %>% 
  rename(filepath = files) %>% 
  # calculate time diff between file and its preceding. 
  # will use this to differentiate different procedures of same patient
  group_by(pat_id, pat_id_extra) %>% 
  arrange(pat_id_extra, pat_id, datetime) %>% 
  mutate(diff = difftime(datetime, lag(datetime), unit = "mins")) %>% 
  ungroup()


waves_times = waves_times %>% 
  select(-rowid, -datetime_full) %>% 
  # since same pat_id can be mean different procedures we're using timediff to differentiate files that belong to the same procedure and generating a filegroup_id
  arrange(pat_id_extra) %>% 
  mutate(filegroup = if_else(is.na(diff) | diff > 31, 1, 0)) %>% 
  mutate(filegroup_id = cumsum(filegroup)) %>% 
  select(-diff, -filegroup)

waves_times %>% write_csv("/home/common/mover_data/surginf_cleaned/waveform_filenames.csv")

