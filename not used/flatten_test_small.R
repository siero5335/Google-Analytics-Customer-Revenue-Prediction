tr  <- read_csv("data/train_v2.csv", n_max=20)   # 20 row sample

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

parse <- . %>% 
  bind_cols(flatten_json(.$device)) %>%
  bind_cols(flatten_json(.$geoNetwork)) %>% 
  bind_cols(flatten_json(.$trafficSource)) %>% 
  bind_cols(flatten_json(.$totals)) %>% 
  select(-device, -geoNetwork, -trafficSource, -totals)

tr <- parse(tr)

## parse customDimensions
customDimensions_tr  <- tr %>%
  select(fullVisitorId, visitStartTime, customDimensions) %>%
  rename(csv_customDimensions = customDimensions) %>%
  mutate(fixed_json_customDimensions = fix_jsons(csv_customDimensions)) %>%
  mutate(r_object_customDimensions = map(fixed_json_customDimensions, fromJSON))  %>%
  mutate(cD_index = map(r_object_customDimensions, "index")) %>%
  mutate(cD_value = map(r_object_customDimensions, "value"))%>%
  select(fullVisitorId, visitStartTime, cD_index, cD_value)

customDimensions_tr$cD_index[sapply(customDimensions_tr$cD_index, is.null)] <- NA

temp_cDindex <- customDimensions_tr$cD_index %>% 
  unlist() %>% 
  enframe() %>% 
  unnest() %>%
  select(value) %>%
  rename(cD_index = value)


customDimensions_tr$cD_value[sapply(customDimensions_tr$cD_value, is.null)] <- NA

temp_cD_value <- customDimensions_tr$cD_value %>% 
  unlist() %>% 
  enframe() %>% 
  unnest() %>%
  select(value) %>%
  rename(cD_value = value)

customDimensions_tr$cD_index <- NULL
customDimensions_tr$cD_value <- NULL

customDimensions_tr <- as.data.frame(customDimensions_tr, temp_cDindex, temp_cD_value)
rm(temp_cDindex, temp_cD_value)
gc(); gc()


## hit data parse
hit_type_df  <- tr %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  rename(csv_hits = hits) %>%
  mutate(fixed_json_hits = fix_json_hits(csv_hits)) %>%
  select(-csv_hits)

hit_type_df$fixed_json_hits <- gsub("NULL", "NA", hit_type_df$fixed_json_hits)

hit_type_df  <- hit_type_df  %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

hit_type_df$r_object_hits[sapply(hit_type_df$r_object_hits, is.null)] <- NA
hit_type_df$r_object_hits[sapply(hit_type_df$r_object_hits, " ")] <- NA
# これ空白もNAうめしないとだめか

temp <- hit_type_df %>%
  mutate(page_hits = map(r_object_hits, "page")) %>%
  mutate(pages_pagePaths = map(page_hits, "pagePath"))%>%
  select(fullVisitorId, visitStartTime, pages_pagePaths)