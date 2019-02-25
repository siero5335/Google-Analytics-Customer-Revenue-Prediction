library(readr)
library(tidyverse)
library(DataExplorer)

df1 <- read_csv("tidy_user_LGBM_-0.14066_xgb-for-gstore-7b8bce-6.csv")
df2 <- read_csv("tidy_user_xGb_0.14465_xgb-for-gstore-7b8bce-5.csv")
df3 <- read_csv("tidy_user_LGBM_-0.14117_xgb-for-gstore-7b8bce-9.csv")
df4 <- read_csv("tidy_user_LGBM_-0.16879_xgb-for-gstore-7b8bce-7.csv")
df5 <- read_csv("tidy_user_LGBM_-0.16943_-0.16879_xgb-for-gstore-7b8bce-10.csv")
df6 <- read_csv("tidy_user_LGBM_-0.174_xgb-for-gstore-7b8bce-8.csv")

df <- df1 %>%
  left_join(df2, PredictedLogRevenue, by = "fullVisitorId") %>%
  left_join(df3, PredictedLogRevenue, by = "fullVisitorId") %>%
  left_join(df4, PredictedLogRevenue, by = "fullVisitorId") %>%
  left_join(df5, PredictedLogRevenue, by = "fullVisitorId") %>%
  left_join(df6, PredictedLogRevenue, by = "fullVisitorId") 

DataExplorer::plot_correlation(df[, -1])
DataExplorer::create_report(df[, -1])

y <-  (df2$PredictedLogRevenue * 0.5 + df1$PredictedLogRevenue * 0.3 + df3$PredictedLogRevenue * 0.1 +
         df4$PredictedLogRevenue * 0.03 + df5$PredictedLogRevenue * 0.03 + df6$PredictedLogRevenue * 0.04)

tibble(pred = y, fullVisitorId = df1$fullVisitorId) %>% 
  right_join(read_csv("data/sample_submission_v2.csv"),
             by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(pred, 5)) %>% 
  select(-pred) %>% 
  write_csv(paste0("mix_res1.csv"))