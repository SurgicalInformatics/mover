library(tidyverse)
library(sparklyr)


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


# waves_orig = fs::dir_ls("/home/common/datastore_surginf/mover_data/") %>% 
#   str_subset("file_list.txt") %>% 
#   read_csv(col_names = "files") %>% 
#   rowid_to_column()
# 
# waves_filenames = waves_orig %>%
#   filter(str_detect(files, "xml")) %>%
#   # remove full path (everything up to and including Wavefolders/Waveforms)
#   mutate(filename = str_remove(files, "^(.*?)Wavefolders/")) %>%
#   mutate(filename = str_remove(filename, "^(.*?)Waveforms/")) %>%
#   # remove everything before / (subfolder name)
#   mutate(filename = str_remove(filename, "^(.*?)/")) %>%
#   separate(filename, into = c("PAT_ID", "datetime"), extra = "merge", remove = FALSE) %>%
#   mutate(datetime_full = str_remove(datetime, "-...Z.xml$")) %>%
#   mutate(datetime = parse_date_time(datetime_full, "%Y-%m-%d-%H-%M-%S"))
# 
# waves_filenames = waves_filenames %>% 
#   select(-files, -filename)
# 
# save(waves_filenames, file = "waves_filenames.rda")

load("waves_filenames.rda")

waves_times = waves_filenames %>% 
  mutate(PAT_ID = str_remove(PAT_ID, ".{2}$")) %>% 
  rename(pat_id = PAT_ID) %>% 
  group_by(pat_id) %>% 
  summarise(wave_start = min(datetime),
            wave_end = max(datetime))

waves_times %>% write_csv("/home/common/mover_data/surginf_cleaned/waveform_time_start_end.csv")

waves_ids = distinct(waves_filenames, PAT_ID) %>% 
  mutate(PAT_ID = str_remove(PAT_ID, ".{2}$"))

# flowsheet_sample = flowsheet %>%
#   filter(PAT_ID %in% !!waves_ids$PAT_ID) %>%
#   collect()


# flowsheet %>%
#   filter(MRN %in% c("7144cef9922e9863")) %>%
#   collect()

fs_startend = flowsheet %>% 
  group_by(PAT_ID, LOG_ID, MRN, OR_CASE_ID) %>% 
  summarise(start_time = min(RECORDED_TIME, na.rm = TRUE),
            end_time = max(RECORDED_TIME, na.rm = TRUE)) %>% 
  collect()

fs_startend %>% write_csv("/home/common/mover_data/surginf_cleaned/flowsheet_time_start_end_PAT_ID.csv")

fs_startend %>% 
  ggplot(aes(start_time)) +
  geom_histogram()

# flowsheet_orig = read_csv("/home/common/mover_data/srv/disk00/EPIC_flowsheets/1_2018-4633_flowsheets_20211006.csv",
#                           skip = 0,
#                           n_max = 2000000,
#                           col_types = cols(.default = "c"),
#                           col_select = 1)
# 
# 
# files = tibble(fullname = dir_ls("/home/common/datastore_surginf/mover_data/waves_sample/ff")) %>% 
#   separate(fullname, into = c(NA, "filename"), sep = "/ff/", remove = FALSE) %>% 
#   separate(filename, into = c("OR_CASE_ID", "datetime"), extra = "merge", remove = FALSE) %>% 
#   mutate(datetime = str_remove(datetime, "Z.xml$"))
# 
# ids = files %>% 
#   pull(OR_CASE_ID) %>% 
#   unique()
# 
# 
# flowsheet_ids = flowsheet_orig %>% 
#   distinct(OR_CASE_ID)
# 
# flowsheet_ids %>% write_csv("flowsheet_ids_2million.csv")
