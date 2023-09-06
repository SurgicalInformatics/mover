library(tidyverse)

complications_orig = read_csv(paste0(Sys.getenv("epic_emr"), "patient_post_op_complications.csv"))

complications_all = complications_orig %>% 
  janitor::clean_names() %>% 
  distinct() %>% 
  rename(comp_abbr    = element_abbr,
         complication = smrtdta_elem_value) %>% 
  mutate(complication = if_else(is.na(complication), "Unknown", complication))

complications_combined = complications_all %>% 
  mutate(comp_abbr = if_else(comp_abbr == "AN Post-op Complications",
                             complication, comp_abbr)) %>% 
  arrange(comp_abbr) %>% 
  summarise(.by = c(log_id, mrn),
            any_complication   = if_else(all(complication == "None"), "No", "Yes"),
            n_complications    = sum(complication != "None"),
            comp_abbr = paste(unique(comp_abbr), collapse = ", "),
            comp_full = paste(unique(complication), collapse = ", ")) %>% 
  mutate(comp_abbr = if_else(any_complication   == "No",
                             "No complications",
                             str_remove(comp_abbr, "None, |, None")),
         comp_full = if_else(any_complication   == "Yes",
                             str_remove(comp_full, "None, |, None"),
                             comp_full))

complications = complications_combined %>% 
  mutate(death       = if_else(str_detect(comp_abbr, "Death"),
                               "Died", "Alive"),
         respiratory_comp = if_else(str_detect(comp_abbr, "Respiratory"),
                                    "Yes", "No"))

write_csv(complications, "/home/common/mover_data/surginf_cleaned/complications_cleaned.csv")


# quick look at complication rates
complications %>% 
  count(any_complication) %>% 
  mutate(prop = scales::percent(n/sum(n)))

complications %>% 
  count(respiratory_comp) %>% 
  mutate(prop = scales::percent(n/sum(n)))


complications %>% 
  count(infection = str_detect(comp_abbr, "Infection")) %>% 
  mutate(prop = scales::percent(n/sum(n), 0.01))