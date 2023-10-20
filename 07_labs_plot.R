library(tidyverse)
library(scales)
theme_set(theme_classic())


labs_orig = read_csv("/home/common/mover_data/surginf_cleaned/labs_allobs_main4_allobs.csv")


labs = labs_orig %>% 
  filter(obs_value < 1e6) %>% 
  mutate(abnormal_flag = fct_recode(abnormal_flag,
                                    "Very high" = "HH",
                                    "High" = "H",
                                    "Normal" = "N",
                                    "Low" = "L",
                                    "Very low" = "LL") %>% 
           fct_relevel("Very low", "Low", "Normal", "High"))

reference = labs %>% 
  distinct(lab_name, ref_range, meas_unit) %>% 
  filter(ref_range != "Unknown") %>% 
  separate(ref_range, into = c("ref_low", "ref_high"), sep = "-", remove = FALSE, convert = TRUE) %>% 
  arrange(lab_name, ref_low, ref_high)

ref = reference %>% 
  select(lab_name, ref_low, ref_high) %>% 
  pivot_longer(-lab_name)


labs %>% 
  group_by(lab_name) %>% 
  slice_sample(n = 2000) %>% 
  ggplot(aes(abnormal_flag, obs_value)) +
  geom_hline(data = ref, aes(yintercept = value), colour = "grey") +
  geom_jitter(alpha = 0.1, width = 0.2) +
  facet_wrap(~lab_name, scales = "free") +
  geom_boxplot(aes(colour = abnormal_flag), fill = NA, outlier.shape = NA) +
  scale_y_log10() +
  scale_colour_manual(values = c("#2166ac","#67a9cf", "#91cf60", "#d73027", "#b2182b")) +
  theme(legend.position = "none")


# explore "Creatine kinase" ----
labs %>% 
  filter(lab_name == "Creatine kinase") %>% 
  filter(obs_value < 1e6) %>% 
  slice(1:10000) %>% 
  ggplot(aes(abnormal_flag, obs_value)) +
  geom_point() +
  geom_boxplot()



ggsave("plots/labs_all4_boxplots.pdf", width = 8, height = 12)
