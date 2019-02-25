library("tidyverse"); library("magrittr"); library("jsonlite"); 
library("caret"); library("lubridate"); library("xgboost"); 
library("DataExplorer"); library("GGally"); library("irlba"); 
library("summarytools"); library("e1071"); library("magrittr"); library("sessioninfo")

set.seed(71)

#---------------------------
cat("Loading data...\n")

ctypes <- cols(fullVisitorId = col_character(),
               channelGrouping = col_character(),
               date = col_datetime(),
               device = col_character(),
               geoNetwork = col_character(),
               socialEngagementType = col_skip(), 
               totals = col_character(),
               trafficSource = col_character(),
               visitId = col_integer(), 
               visitNumber = col_integer(),
               visitStartTime = col_integer(),
               hits = col_skip(),
               customDimensions = col_character())

tr <- read_csv("data/train_v2.csv", col_types = ctypes)
te <- read_csv("data/test_v2.csv", col_types = ctypes)

#---------------------------
cat("Defining auxiliary functions...\n")

flatten_json <- . %>% 
  str_c(., collapse = ",") %>% 
  str_c("[", ., "]") %>% 
  fromJSON(flatten = T)

fix_jsons <- function(jsonstring) {
  jsonstring %>%
    str_replace_all("'(?![a-z])|(?<=\\{|\\s)'", '"') %>% # replace single quote w/ double and deal with apostrophes within quotes strings
    str_replace_all("True", '"True"') %>% # quote the logical values, all of which are not quoted
    str_replace_all("\\|", "") # just a helper for the line below
}

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

#---------------------------
cat("Basic preprocessing...\n")

tr <- parse(tr)
te <- parse(te)

## parse customDimensions tr
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

temp_cDvalue <- customDimensions_tr$cD_value %>% 
  unlist() %>% 
  enframe() %>% 
  unnest() %>%
  select(value) %>%
  rename(cD_value = value)

customDimensions_tr$cD_index <- NULL
customDimensions_tr$cD_value <- NULL

customDimensions_tr <- data.frame(customDimensions_tr, temp_cDindex, temp_cDvalue)
rm(temp_cDindex, temp_cDvalue)
gc(); gc()

## parse customDimensions te
customDimensions_te  <- te %>%
  select(fullVisitorId, visitStartTime, customDimensions) %>%
  rename(csv_customDimensions = customDimensions) %>%
  mutate(fixed_json_customDimensions = fix_jsons(csv_customDimensions)) %>%
  mutate(r_object_customDimensions = map(fixed_json_customDimensions, fromJSON))  %>%
  mutate(cD_index = map(r_object_customDimensions, "index")) %>%
  mutate(cD_value = map(r_object_customDimensions, "value"))%>%
  select(fullVisitorId, visitStartTime, cD_index, cD_value)

customDimensions_te$cD_index[sapply(customDimensions_te$cD_index, is.null)] <- NA

temp_cDindex <- customDimensions_te$cD_index %>% 
  unlist() %>% 
  enframe() %>% 
  unnest() %>%
  select(value) %>%
  rename(cD_index = value)


customDimensions_te$cD_value[sapply(customDimensions_te$cD_value, is.null)] <- NA

temp_cDvalue <- customDimensions_te$cD_value %>% 
  unlist() %>% 
  enframe() %>% 
  unnest() %>%
  select(value) %>%
  rename(cD_value = value)

customDimensions_te$cD_index <- NULL
customDimensions_te$cD_value <- NULL

customDimensions_te <- data.frame(customDimensions_te, temp_cDindex, temp_cDvalue)


tr <- left_join(tr, customDimensions_tr, by = c("fullVisitorId", "visitStartTime"))
te <- left_join(te, customDimensions_te, by = c("fullVisitorId", "visitStartTime"))

rm(temp_cDindex, temp_cDvalue, customDimensions_tr, customDimensions_te)

y <- log1p(as.numeric(tr$transactionRevenue))
y[is.na(y)] <- 0

tr$transactionRevenue <- NULL
tr$campaignCode <- NULL

id <- te[, "fullVisitorId"]
tri <- 1:nrow(tr)
idx <- ymd(tr$date) < ymd("20170601")

tr_te <- tr %>% 
  bind_rows(te) %>% 
  select_if(has_many_values) %>% 
  mutate_all(funs(ifelse(is_na_val(.), NA, .))) %>% 
  mutate(pageviews = ifelse(is.na(pageviews), 0L, as.integer(pageviews)),
         visitNumber =  visitNumber,
         newVisits = ifelse(newVisits == "1", 1L, 0L),
         bounces = ifelse(is.na(bounces), 0L, 1L),
         isMobile = ifelse(isMobile, 1L, 0L),
         adwordsClickInfo.isVideoAd = ifelse(is.na(adwordsClickInfo.isVideoAd), 0L, 1L),
         isTrueDirect = ifelse(is.na(isTrueDirect), 0L, 1L),
         browser_dev = str_c(browser, "_", deviceCategory),
         browser_os = str_c(browser, "_", operatingSystem),
         browser_chan = str_c(browser,  "_", channelGrouping),
         campaign_medium = str_c(campaign, "_", medium),
         chan_os = str_c(operatingSystem, "_", channelGrouping),
         country_adcontent = str_c(country, "_", adContent),
         country_medium = str_c(country, "_", medium),
         country_source = str_c(country, "_", source),
         dev_chan = str_c(deviceCategory, "_", channelGrouping),
         date = as_datetime(visitStartTime),
         year = year(date),
         month = month(date),
         week = week(date),
         wday = wday(date),
         yday = yday(date),
         qday = qday(date),
         day = day(date),
         hour = hour(date)) %>% 
  mutate(weekofyear = factor(paste(year, week, sep = "_")),
         wdayofyear = factor(paste(year, wday, sep = "_")),
         ydayofyear = factor(paste(year, yday, sep = "_")),
         qdayofyear = factor(paste(year, qday, sep = "_")),
         weekofmonth = factor(paste(year, month, sep = "_")),
         weekofday = factor(paste(year, day, sep = "_"))
  )

tr_te$customDimensions <- NULL
#---------------------------
cat("Adding time features...\n")

for (i in 1:3) {
  tr_te[str_c("next_sess", i)] <- create_time_fea(lag, i)
  tr_te[str_c("prev_sess", i)] <- create_time_fea(lead, i)
}

#---------------------------
cat("Creating group features...\n")

for (grp in c("wday", "hour")) {
  col <- paste0(grp, "_user_cnt")
  tr_te %<>% 
    group_by_(grp) %>% 
    mutate(!!col := n_distinct(fullVisitorId)) %>% 
    ungroup()
}

fn <- funs(mean, var, skewness, min, max, sum, .args = list(na.rm = TRUE))
for (grp in c("browser", "city", "country", "networkDomain", 
              "referralPath", "source", "visitNumber")) {
  df <- paste0("sum_by_", grp)
  s <- paste0("_", grp)
  tr_te %<>% 
    left_join(assign(df, tr_te %>% 
                       select_(grp, "pageviews") %>% 
                       group_by_(grp) %>% 
                       summarise_all(fn)),  by = grp, suffix = c("", s)) 
}

tr_te %<>% 
  select(-date, -fullVisitorId, -visitId, -visitStartTime, -hits) %>% 
  mutate_if(is.character, funs(factor(.) %>% as.integer)) %>% 
  select_if(has_many_values) 

rm(tr, te, grp, col, flatten_json, parse, has_many_values, 
   is_na_val, fn, df, s)
gc()

#---------------------------
cat("Preparing data...\n")

dtest <- xgb.DMatrix(data = data.matrix(tr_te[-tri, ]))
tr_te <- tr_te[tri, ]
dtr <- xgb.DMatrix(data = data.matrix(tr_te[idx, ]), label = y[idx])
dval <- xgb.DMatrix(data = data.matrix(tr_te[!idx, ]), label = y[!idx])
dtrain <- xgb.DMatrix(data = data.matrix(tr_te), label = y)
cols <- colnames(tr_te)
rm(tr_te, y, tri)
gc()

#---------------------------
cat("Training model...\n")

p <- list(objective = "reg:linear",
          booster = "gbtree",
          eval_metric = "rmse",
          nthread = 4,
          eta = 0.01,
          max_depth = 7,
          min_child_weight = 5,
          gamma = 0,
          subsample = 0.8,
          colsample_bytree = 0.7,
          colsample_bylevel = 0.6,
          alpha = 0,
          lambda = 1)

set.seed(0)
cv <- xgb.train(p, dtr, 5000, list(val = dval), print_every_n = 200, early_stopping_rounds = 200)

nrounds <- round(cv$best_iteration * (1 + sum(!idx) / length(idx)))

set.seed(0)
m_xgb <- xgb.train(p, dtrain, nrounds)

imp <- xgb.importance(cols, model = m_xgb) %T>% 
  xgb.plot.importance(top_n = 30)

#---------------------------
cat("Making predictions...\n")

pred <- predict(m_xgb, dtest) %>% 
  as_tibble() %>% 
  set_names("y") %>% 
  mutate(y = ifelse(y < 0, 0, expm1(y))) %>% 
  bind_cols(id) %>% 
  group_by(fullVisitorId) %>% 
  summarise(y = log1p(sum(y)))
  
#---------------------------
cat("Making submission file...\n")

read_csv("data/sample_submission_v2.csv") %>%  
  left_join(pred, by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(y, 5)) %>% 
  select(-y) %>% 
  write_csv(paste0("tidy_xGb_", round(cv$best_score, 5), ".csv"))