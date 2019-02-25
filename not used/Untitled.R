library(tidyverse)
library(jsonlite)
library(stringr)
library(feather)
library(lubridate)

col_types <- cols(
  channelGrouping = col_character(),
  customDimensions = col_character(),
  date = col_datetime(), # Parses YYYYMMDD
  device = col_character(),
  fullVisitorId = col_character(),
  geoNetwork = col_character(),
  hits = col_character(),
  #sessionId = col_character(), # not present in v2 comp; not used anwyay
  socialEngagementType = col_skip(), # Skip as always "Not Socially Engaged"
  totals = col_character(),
  trafficSource = col_character(),
  visitId = col_integer(), # visitId & visitStartTime look identical in all but 5000 cases
  visitNumber = col_integer(),
  visitStartTime = col_integer() # Convert to POSIXlt later,
)

# Convert Python array/dictionary string to JSON format
unsnake <- . %>%
  str_replace_all(c("\\[\\]" = "[{}]", # empty element must contain dictionary
                    "^\\[|\\]$" = "", # remove initial and final brackets
                    "(\\[|\\{|, |: |', )'" = "\\1\"", # open single- to double-quote (on key or value)
                    "'(\\]|\\}|: |, )" = '\"\\1')) # close quote

separate_json <- . %>%
  str_replace_all(c("\"[A-Za-z]+\": \"not available in demo dataset\"(, )?" = "",
                    ", \\}" = "}")) %>% # if last property in list was removed
  paste(collapse = ",") %>% paste("[", ., "]") %>% # As fromJSON() isn't vectorised
  fromJSON(., flatten = TRUE)

NMAX = Inf
tr <- read_csv("data/train_v2.csv", skip = 1:436393, col_types = col_types, n_max = NMAX) %>% mutate(test = F) %>%
  bind_cols(separate_json(.$device))        %>% select(-device) %>%
  bind_cols(separate_json(.$geoNetwork))    %>% select(-geoNetwork) %>%
  bind_cols(separate_json(.$totals))        %>% select(-totals) %>%
  bind_cols(separate_json(.$trafficSource)) %>% select(-trafficSource) %>%
  bind_cols(separate_json(unsnake(.$customDimensions))) %>% select(-customDimensions) %>%
  bind_cols(separate_json(unsnake(.$hits))) %>% select(-hits)

gc(); gc()

te <-  read_csv("data/test_v2.csv",  col_types = col_types, n_max = NMAX) %>% mutate(test = T) %>%
  bind_cols(separate_json(.$device))        %>% select(-device) %>%
  bind_cols(separate_json(.$geoNetwork))    %>% select(-geoNetwork) %>%
  bind_cols(separate_json(.$totals))        %>% select(-totals) %>%
  bind_cols(separate_json(.$trafficSource)) %>% select(-trafficSource) %>%
  bind_cols(separate_json(unsnake(.$customDimensions))) %>% select(-customDimensions)

# Remove junk to save a bit of time on loading
df$visits <- NULL # [2] constant column, no information
df$adwordsClickInfo.gclId <- NULL # [3,4] useless hash, no obvious information
#sessionId # [2] "unique" sessionId (some duplicates), not present in v2 comp
#campaignCode # [2] only one row with value, none present in test set (may not be true in v2 comp)

# Identify types
df <-
  df %>%
  mutate_at(vars(hits:transactions, index), as.integer) %>%
  mutate(
    visitStartTime = lubridate::as_datetime(visitStartTime),
    transactionRevenue = as.numeric(transactionRevenue), # Target
    totalTransactionRevenue = as.numeric(transactionRevenue)
  )

format(object.size(df), units = "auto")

write_feather(df, "df.feather")

tri <- 1:1708337
id <- df[1708338:2109926, "fullVisitorId"]

