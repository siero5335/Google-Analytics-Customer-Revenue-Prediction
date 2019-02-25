library(jsonlite)
library(tidyverse)
library(lubridate)

tr <- read_csv("data/train_v2.csv")

fix_json_hits <- function(jsonstring) {
  jsonstring %>%
    str_replace_all("'(?![a-z])|(?<=\\{|\\s)'", '"') %>% # replace single quote w/ double and deal with apostrophes within quotes strings
    str_replace_all("True", '"True"') %>% # quote the logical values, all of which are not quoted
    str_replace_all("\\|", "") %>% # just a helper for the line below
    str_replace_all('"pageTitle": "Kids" Apparel  Google Merchandise Store"', '"pageTitle":"Kids Apparel  Google Merchandise Store"')# this is one of the unexpected misplaced quot. mark issues present in the original data 
}

fix_jsons <- function(jsonstring) {
  jsonstring %>%
    str_replace_all("'(?![a-z])|(?<=\\{|\\s)'", '"') %>% # replace single quote w/ double and deal with apostrophes within quotes strings
    str_replace_all("True", '"True"') %>% # quote the logical values, all of which are not quoted
    str_replace_all("\\|", "") # just a helper for the line below
}

flatten_json <- . %>% 
  str_c(., collapse = ",") %>% 
  str_c("[", ., "]") %>% 
  fromJSON(flatten = T)

tr$date = ymd(tr$date)
tr <- tr[order(as.Date(tr$date, format="%Y/%m/%d")),]

tr <- tr[436394:1708337, ]
# write_csv(tr, "tr.csv")

parse <- . %>% 
  bind_cols(flatten_json(.$device)) %>%
  bind_cols(flatten_json(.$geoNetwork)) %>% 
  bind_cols(flatten_json(.$trafficSource)) %>% 
  bind_cols(flatten_json(.$totals)) %>% 
  select(-device, -geoNetwork, -trafficSource, -totals)

is_na_val <- function(x) x %in% c("not available in demo dataset", "(not set)", 
                                  "unknown.unknown", "(not provided)")

has_many_values <- function(x) n_distinct(x) > 1

create_time_fea <- function(fun = lag, n = 1)
  select(tr_te, fullVisitorId, date) %>% 
  group_by(fullVisitorId) %>% 
  mutate(time_var = fun(date, n)) %>% 
  ungroup() %$% 
  as.integer(time_var) / 3600

tr <- parse(tr)

customDimensions_tr  <- tr %>%
  select(fullVisitorId, visitStartTime, customDimensions) %>%
  rename(csv_customDimensions = customDimensions) %>%
  mutate(fixed_json_customDimensions = fix_jsons(csv_customDimensions)) %>%
  mutate(r_object_customDimensions = map(fixed_json_customDimensions, fromJSON))  %>%
  mutate(cD_index = map(r_object_customDimensions, "index")) %>%
  mutate(cD_value = map(r_object_customDimensions, "value"))%>%
  select(fullVisitorId, visitStartTime, cD_index, cD_value)

tr$customDimensions <- NULL


cD_index <- as.data.frame(unlist(customDimensions_tr$cD_index))
customDimensions_tr$cD_index <- unlist(customDimensions_tr$cD_index)

customDimensions_tr <- as.data.frame(customDimensions_tr)
customDimensions_tr[customDimensions_tr$cD_index=="NULL"] <- NA

tr <- left_join(tr, customDimensions_tr, by=c("fullVisitorId","x2"))
  

te <-  read_csv("data/test_v2.csv")