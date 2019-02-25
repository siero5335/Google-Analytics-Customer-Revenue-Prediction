library(jsonlite)
library(lubridate)
library(lightgbm)
library(rsample)
library(rlist)
library(tidyverse)
library(magrittr)
library(e1071)
library(irlba)
library(caret)
set.seed(0)

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

is_na_val <- function(x) x %in% c("not available in demo dataset", "(not provided)",
                                  "(not set)", "<NA>", "unknown.unknown",  "(none)", "[]")

has_many_values <- function(x) n_distinct(x) > 1

get_folds <- function(data, group, v = 5) {
  group_folds <- group_vfold_cv(data[group], group, v = v)
  list.zip(tr = tr_ind <- map(group_folds$splits, ~ .x$in_id),
           val = val_ind <- map(group_folds$splits, ~ setdiff(1:nrow(data), .x$in_id))) 
}

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

y <- log1p(as.numeric(tr$transactionRevenue))
y[is.na(y)] <- 0

tr$transactionRevenue <- NULL
tr$totalTransactionRevenue <- NULL
tr$campaignCode <- NULL
te$transactionRevenue <- NULL
te$totalTransactionRevenue <- NULL
tri <- 1:nrow(tr)

tr_id <- tr$fullVisitorId
te_id <- te$fullVisitorId

tr_val_ind <- get_folds(tr, "fullVisitorId", 5)

## parse customDimensions tr
customDimensions_tr  <- tr %>%
  select(fullVisitorId, visitStartTime, customDimensions) %>%
  rename(csv_customDimensions = customDimensions) %>%
  mutate(fixed_json_customDimensions = fix_jsons(csv_customDimensions)) %>%
  mutate(r_object_customDimensions = map(fixed_json_customDimensions, fromJSON))  %>%
  mutate(cD_index = map(r_object_customDimensions, "index")) %>%
  mutate(cD_value = map(r_object_customDimensions, "value"))%>%
  select(fullVisitorId, visitStartTime, cD_index, cD_value)

tr$customDimensions <- NULL
gc(); gc()

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

tr <- left_join(tr, customDimensions_tr, by = c("fullVisitorId", "visitStartTime"))

rm(temp_cDindex, temp_cDvalue, customDimensions_tr)

## parse customDimensions te
te <- read_csv("data/test_v2.csv", col_types = ctypes)
te <- parse(te)

id <- te[, "fullVisitorId"]

customDimensions_te  <- te %>%
  select(fullVisitorId, visitStartTime, customDimensions) %>%
  rename(csv_customDimensions = customDimensions) %>%
  mutate(fixed_json_customDimensions = fix_jsons(csv_customDimensions)) %>%
  mutate(r_object_customDimensions = map(fixed_json_customDimensions, fromJSON))  %>%
  mutate(cD_index = map(r_object_customDimensions, "index")) %>%
  mutate(cD_value = map(r_object_customDimensions, "value"))%>%
  select(fullVisitorId, visitStartTime, cD_index, cD_value)

te$customDimensions <- NULL
gc(); gc()

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

te <- left_join(te, customDimensions_te, by = c("fullVisitorId", "visitStartTime"))

rm(temp_cDindex, temp_cDvalue, customDimensions_te)



# merge hits and FE

tr_te_hit <- read_csv("data/concat_hit.csv") 
tr_te_hit$X1 <- NULL

tr_te_hit$transaction.localTransactionRevenue <- NULL
tr_te_hit$transaction.localTransactionShipping <- NULL
tr_te_hit$transaction.localTransactionTaX <- NULL
tr_te_hit$transaction.transactionRevenue <- NULL
tr_te_hit$transaction.transactionShipping <- NULL
tr_te_hit$transaction.transactionTaX <- NULL
tr_te_hit$contentGroup.previousContentGroup1 <- NULL
tr_te_hit$contentGroup.previousContentGroup2 <- NULL
tr_te_hit$contentGroup.previousContentGroup3 <- NULL
tr_te_hit$contentGroup.previousContentGroup4 <- NULL
tr_te_hit$contentGroup.previousContentGroup5 <- NULL
tr_te_hit$contentGroup.contentGroup4 <- NULL
tr_te_hit$contentGroup.contentGroup5 <- NULL
tr_te_hit$customDimensions <- NULL
tr_te_hit$customMetrics <- NULL
tr_te_hit$customVariables <- NULL
tr_te_hit$experiment <- NULL
tr_te_hit$publisher_infos <- NULL
tr_te_hit$social.socialInteractionNetworkAction <- NULL

tr_te <- tr %>% 
  bind_rows(te) %>% 
  left_join(tr_te_hit, by = c("fullVisitorId", "visitStartTime")) %>% 
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
         hour2 = hour(date)) %>% 
  mutate(weekofyear = factor(paste(year, week, sep = "_")),
         wdayofyear = factor(paste(year, wday, sep = "_")),
         ydayofyear = factor(paste(year, yday, sep = "_")),
         qdayofyear = factor(paste(year, qday, sep = "_")),
         monthofyear = factor(paste(year, month, sep = "_")),
         wdayofmonth = factor(paste(month, wday, sep = "_")),
         dayofyear = factor(paste(year, day, sep = "_")),
         hour_minues = factor(paste(hour2, minute, sep = "_"))
  )

tr_te <- tr_te[, -which(colMeans(is.na(tr_te)) > 0.999)]

tr_te$totalTransactionRevenue <- NULL
tr_te$transactionRevenue <- NULL

rm(tr_te_hit)
gc(); gc()

tr_te_pca <- tr_te %>% 
  select(-date, -fullVisitorId, -visitId, -visitStartTime, -hits) %>% 
  mutate_if(is.character, funs(factor(.) %>% as.integer)) %>% 
  mutate_if(is.factor, as.integer)


tr_te_pca[is.na(tr_te_pca)]<-0

nzv <- nearZeroVar(tr_te_pca)
tr_te_pca <- tr_te_pca[,-nzv] 

set.seed(71)
n_pca <- 20
m_pca <- prcomp_irlba(data.matrix(tr_te_pca), n = n_pca, scale. = TRUE, fastpath=FALSE)
tr_te_pca <- m_pca$x %>% as_tibble

library(h2o)
h2o.no_progress()
h2o.init(nthreads = 6, max_mem_size = "10G")

tr_te_h2o <- as.h2o(tr_te_pca)

n_ae <- 6
m_ae <- h2o.deeplearning(training_frame = tr_te_h2o,
                         x = 1:ncol(tr_te_h2o),
                         autoencoder = T,
                         activation="Tanh",
                         reproducible = TRUE,
                         seed = 71,
                         sparse = T,
                         standardize = TRUE,
                         hidden = c(32, n_ae, 32),
                         max_w2 = 5,
                         epochs = 15)
tr_te_ae <- h2o.deepfeatures(m_ae, tr_te_h2o, layer = 2) %>% as_tibble

h2o.shutdown(prompt = FALSE)

rm(m_ae); gc(); gc()

tr_te %<>% bind_cols(tr_te_pca) %>% 
  bind_cols(tr_te_ae)

rm(tr_te_pca, tr_te_ae)
gc(); gc()

#---------------------------
cat("Adding combinative features...\n")

for (i in c("city", "country", "networkDomain", "continent", "metro", "subContinent", "cD_value"))
  for (j in c("browser", "operatingSystem", "source", "dataSource", "deviceCategory"))
    tr_te[str_c(i, "_", j)] <- str_c(tr_te[[i]], tr_te[[j]], sep = "_")

#---------------------------
cat("Adding time features...\n")

for (i in 1:2) {
  tr_te[str_c("next_sess", i)] <- create_time_fea(lag, i)
  tr_te[str_c("prev_sess", i)] <- create_time_fea(lead, i)
}

#---------------------------
cat("Creating group features...\n")

for (grp in c("wday", "hour", "hour2", "month", "qday", "wdayofyear", 
              "qdayofyear", "monthofyear", "hour_minues", "wdayofmonth")) {
  col <- paste0(grp, "_user_cnt")
  tr_te %<>% 
    group_by_(grp) %>% 
    mutate(!!col := n_distinct(fullVisitorId)) %>% 
    ungroup()
}

fn <- funs(mean, median, var, skewness, min, max, sum, .args = list(na.rm = TRUE))
for (grp in c("city", "country", "networkDomain", "continent", "metro", "subContinent", "cD_value", "browser", "promoId", 
              "browser_dev", "browser_os", "browser_chan", "country_medium", "country_source", "dev_chan",
              "referralPath", "source", "visitNumber", "appInfo.screenDepth", "operatingSystem", "dataSource", 
              "eCommerceAction.action_type", "eCommerceAction.step", "hitNumber")) {
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

rm(tr, te, grp, col, flatten_json, parse, 
   has_many_values, is_na_val, fn, df, s, m_pca, tr_te_h2o)
gc(); gc()


write_csv(tr_te, "data/tr_te_xgb_ver6.csv")

# tr_te <- read_csv("data/tr_te_xgb_ver6.csv")
#---------------------------
cat("Training model: session level...\n")

dtest <- data.matrix(tr_te[-tri, ])
pred_tr <- rep(0, length(tri)) 
pred_te <- 0

for (i in seq_along(tr_val_ind)) {
  cat("Group fold:", i, "\n")
  
  tr_ind <- tr_val_ind[[i]]$tr
  val_ind <- tr_val_ind[[i]]$val
  
  dtrain <- lgb.Dataset(data = data.matrix(tr_te[tr_ind, ]), label = y[tr_ind])
  dval2 <- data.matrix(tr_te[val_ind, ])
  dval <- lgb.Dataset(data = data.matrix(tr_te[val_ind, ]), label = y[val_ind])
  
  p <- list(objective = "regression",
            metric="rmse",
            learning_rate = 0.05,
            num_leaves = 512,
            max_bin = 255, 
            max_depth = 8,
            subsample = 0.8,
            colsample_bytree = 0.8,
            feature_fraction = 0.95,
            bagging_fraction = 0.85,
            min_child_samples = 20)
  
  set.seed(71)
  cv <- lgb.train(data = dtrain, valids = list(eval = dval), params = p, nrounds = 5000,
                  eval_freq = 100, early_stopping_rounds = 100)
  
  pred_tr[val_ind] <- expm1(predict(cv, dval2))
  pred_te <- pred_te + expm1(predict(cv, dtest))
  
  rm(dtrain, dval, dval2, tr_ind, val_ind, p)
  gc(); gc()
}

pred_tr <- ifelse(pred_tr < 0, 0, pred_tr)
pred_te <- ifelse(pred_te < 0, 0, pred_te / length(tr_val_ind))

rm(dtest, cv); gc(); gc()

pred_te2 <- pred_te %>% 
  as_tibble() %>% 
  set_names("y") %>% 
  bind_cols(id) %>% 
  group_by(fullVisitorId) %>% 
  summarise(y = log1p(sum(y)))

read_csv("data/sample_submission_v2.csv") %>%  
  left_join(pred_te2, by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(y, 5)) %>% 
  select(-y) %>% 
  write_csv(paste0("tidy_LGBM6.csv"))

#---------------------------
cat("Handling data at user level...\n")

y_ul <- tibble(fullVisitorId = tr_id, 
               y = expm1(y)) %>% 
  group_by(fullVisitorId) %>% 
  summarise_all(funs(sum(.) %>% log1p))

tr_ul <- tr_te %>% 
  slice(tri) %>%
  mutate(fullVisitorId = tr_id) %>% 
  group_by(fullVisitorId) %>% 
  summarise_all(funs(mean(. , na.rm = TRUE)))

tr_preds <- tr_te %>% 
  slice(tri) %>% 
  mutate(fullVisitorId = tr_id, 
         pred = pred_tr) %>% 
  select(fullVisitorId, pred) %>%
  group_by(fullVisitorId) %>% 
  mutate(pred_num = str_c("pred", 1:n())) %>% 
  ungroup() %>% 
  filter(pred_num %in% paste0("pred", 1:30)) %>% 
  spread(pred_num, pred) %>%
  mutate(p_mean = apply(select(., starts_with("pred")), 1, mean, na.rm = TRUE),
         p_med = apply(select(., starts_with("pred")), 1, median, na.rm = TRUE),
         p_sd = apply(select(., starts_with("pred")), 1, sd, na.rm = TRUE),
         p_sum = apply(select(., starts_with("pred")), 1, sum, na.rm = TRUE),
         p_min = apply(select(., starts_with("pred")), 1, min, na.rm = TRUE),
         p_max = apply(select(., starts_with("pred")), 1, max, na.rm = TRUE))

tr_ul %<>% 
  left_join(tr_preds, by = "fullVisitorId") %>% 
  left_join(y_ul, by = "fullVisitorId")

y_ul <- tr_ul$y
tr_ul$y <- NULL

rm(tr_preds); gc(); gc()

te_ul <- tr_te %>% 
  slice(-tri) %>%
  mutate(fullVisitorId = te_id) %>% 
  group_by(fullVisitorId) %>% 
  summarise_all(funs(mean(. , na.rm = TRUE)))

te_preds <- tr_te %>% 
  slice(-tri) %>% 
  mutate(fullVisitorId = te_id, 
         pred = pred_te) %>% 
  select(fullVisitorId, pred) %>%
  group_by(fullVisitorId) %>% 
  mutate(pred_num = str_c("pred", 1:n())) %>% 
  ungroup() %>% 
  filter(pred_num %in% paste0("pred", 1:30)) %>% 
  spread(pred_num, pred) %>%
  mutate(p_mean = apply(select(., starts_with("pred")), 1, mean, na.rm = TRUE),
         p_med = apply(select(., starts_with("pred")), 1, median, na.rm = TRUE),
         p_sd = apply(select(., starts_with("pred")), 1, sd, na.rm = TRUE),
         p_sum = apply(select(., starts_with("pred")), 1, sum, na.rm = TRUE),
         p_min = apply(select(., starts_with("pred")), 1, min, na.rm = TRUE),
         p_max = apply(select(., starts_with("pred")), 1, max, na.rm = TRUE))

te_ul %<>% 
  left_join(te_preds, by = "fullVisitorId")
te_id <- te_ul$fullVisitorId
te_ul$fullVisitorId <- NULL

rm(te_preds); gc(); gc()

tr_val_ind <- get_folds(tr_ul, "fullVisitorId", 5)
tr_ul$fullVisitorId <- NULL

cols <- intersect(names(tr_ul), names(te_ul))
tr_ul %<>% select(cols)
te_ul %<>% select(cols)

rm(tr_te, cols, tr_id, tri, y); gc(); gc()

write_csv(tr_ul, "data/tr_ul_ver6.csv")
write_csv(te_ul, "data/te_ul_ver6.csv")
#---------------------------
cat("Training model: user level...\n")

dtest <- data.matrix(te_ul)
rm(te_ul); gc(); gc()

pred_tr <- rep(0, nrow(tr_ul)) 
pred_te <- 0
err <- 0

for (i in seq_along(tr_val_ind)) {
  cat("Group fold:", i, "\n")
  
  tr_ind <- tr_val_ind[[i]]$tr
  val_ind <- tr_val_ind[[i]]$val
  
  dtrain <- lgb.Dataset(data = data.matrix(tr_ul[tr_ind, ]), label = y_ul[tr_ind])
  dval2 <- data.matrix(tr_ul[val_ind, ])
  dval <- lgb.Dataset(data = data.matrix(tr_ul[val_ind, ]), label = y_ul[val_ind])
  
  p <- list(objective = "regression",
            metric="rmse",
            learning_rate = 0.01,
            num_leaves = 512,
            max_bin = 255, 
            max_depth = 7,
            subsample = 0.8,
            colsample_bytree = 0.8,
            feature_fraction = 0.95,
            bagging_fraction = 0.85,
            min_child_samples = 20)
  
  set.seed(71)
  cv <- lgb.train(data = dtrain, valids = list(eval = dval), params = p, nrounds = 5000,
                  eval_freq = 200, early_stopping_rounds = 200)
  
  pred_tr[val_ind] <- predict(cv, dval2)
  pred_te <- pred_te + predict(cv, dtest)
  err <- err + cv$best_score
  
  rm(dtrain, dval, dval2, tr_ind, val_ind, p)
  gc(); gc()
}

pred_tr <- ifelse(pred_tr < 0, 0, pred_tr)
pred_te <- ifelse(pred_te < 0, 0, pred_te / length(tr_val_ind))
err <- err / length(tr_val_ind)

#---------------------------
cat("Making submission file...\n")

tibble(pred = pred_te, fullVisitorId = te_id) %>% 
  right_join(read_csv("data/sample_submission_v2.csv"),
             by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(pred, 5)) %>% 
  select(-pred) %>% 
  write_csv(paste0("tidy_user_LGBM_", round(err, 5), ".csv"))