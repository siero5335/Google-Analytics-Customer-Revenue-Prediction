library("tidyverse"); library("magrittr"); library("jsonlite"); 
library("caret"); library("lubridate"); library("lightgbm"); 
library("DataExplorer"); library("GGally"); library("irlba"); 
library("summarytools"); library("e1071"); library("magrittr"); library("xgboost")

set.seed(71)

#---------------------------
cat("Loading data...\n")

tr <- read_csv("train.csv") 
te <- read_csv("test.csv") 

#---------------------------
cat("Defining auxiliary functions...\n")

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

is_na_val <- function(x) x %in% c("not available in demo dataset", "(not set)", 
                                  "unknown.unknown", "(not provided)")

has_many_values <- function(x) n_distinct(x) > 1

#---------------------------
cat("Basic preprocessing...\n")

tr <- parse(tr)
te <- parse(te)


tr$adwordsClickInfo.criteriaParameters <- NULL
te$adwordsClickInfo.criteriaParameters <- NULL

tr$date2 = ymd(tr$date)
te$date2 = ymd(te$date)

tr <- tr[order(as.Date(tr$date2, format="%Y/%m/%d")),]

te <- te[order(as.Date(te$date2, format="%Y/%m/%d")),]

temp_daycount <- as.data.frame(table(tr$date2))
temp_daycount2 <- as.data.frame(table(te$date2))

tr$googlehome <- c(rep(0, sum(temp_daycount$Freq[1:95])),  #NULL
                        rep(1, (sum(temp_daycount$Freq[1:248]) - sum(temp_daycount$Freq[1:95]))), #america
                        rep(2, (sum(temp_daycount$Freq[1:307]) - sum(temp_daycount$Freq[1:248]))), #Eng
                        rep(3, (sum(temp_daycount$Freq[1:353]) - sum(temp_daycount$Freq[1:307]))), #Canada
                        rep(4, (sum(temp_daycount$Freq[1:366]) - sum(temp_daycount$Freq[1:353]))) #austraria
)

te$googlehome <- c(rep(4, sum(temp_daycount2$Freq[1])),  #austraria
                       rep(5, (sum(temp_daycount2$Freq[1:6]) - sum(temp_daycount2$Freq[1]))), #france
                       rep(6, (sum(temp_daycount2$Freq[1:65]) - sum(temp_daycount2$Freq[1:6]))), #german
                       rep(7, (sum(temp_daycount2$Freq[1:272]) - sum(temp_daycount2$Freq[1:65]))) #Japan
)

tr$googlehome2 <- c(rep(0, sum(temp_daycount$Freq[1:95])),  #NULL
                         rep(1, (sum(temp_daycount$Freq[1:96]) - sum(temp_daycount$Freq[1:95]))), #america
                         rep(0, (sum(temp_daycount$Freq[1:248]) - sum(temp_daycount$Freq[1:96]))), #america  
                         rep(1, (sum(temp_daycount$Freq[1:249]) - sum(temp_daycount$Freq[1:248]))), #Eng
                         rep(0, (sum(temp_daycount$Freq[1:307]) - sum(temp_daycount$Freq[1:249]))), #Eng  
                         rep(1, (sum(temp_daycount$Freq[1:308]) - sum(temp_daycount$Freq[1:307]))), #Canada
                         rep(0, (sum(temp_daycount$Freq[1:353]) - sum(temp_daycount$Freq[1:308]))), #Canada  
                         rep(1, (sum(temp_daycount$Freq[1:354]) - sum(temp_daycount$Freq[1:353]))), #austraria
                         rep(0, (sum(temp_daycount$Freq[1:366]) - sum(temp_daycount$Freq[1:354]))) #austraria
)

te$googlehome2 <- c(rep(0, sum(temp_daycount2$Freq[1])),  #austraria
                        rep(1, (sum(temp_daycount2$Freq[1:2]) - sum(temp_daycount2$Freq[1]))), #france                     
                        rep(0, (sum(temp_daycount2$Freq[1:6]) - sum(temp_daycount2$Freq[1:2]))), #france
                        rep(1, (sum(temp_daycount2$Freq[1:7]) - sum(temp_daycount2$Freq[1:6]))), #france
                        rep(0, (sum(temp_daycount2$Freq[1:65]) - sum(temp_daycount2$Freq[1:7]))), #german
                        rep(1, (sum(temp_daycount2$Freq[1:66]) - sum(temp_daycount2$Freq[1:65]))), #german  
                        rep(0, (sum(temp_daycount2$Freq[1:272]) - sum(temp_daycount2$Freq[1:66]))) #Japan
)



y <- log1p(as.numeric(tr$transactionRevenue))
y[is.na(y)] <- 0

tr$transactionRevenue <- NULL
tr$campaignCode <- NULL
te$campaignCode <- NULL
tr$date2 <- NULL
te$date2 <- NULL

id <- te[, "fullVisitorId"]
tri <- 1:nrow(tr)
idx <- ymd(tr$date) < ymd("20170501")

tr_te <- tr %>% 
  bind_rows(te) %>% 
  select_if(has_many_values) %>% 
  mutate_all(funs(ifelse(is_na_val(.), NA, .))) %>% 
  mutate(hits = log1p(as.integer(hits)),
         pageviews = ifelse(is.na(pageviews), 0L, log1p(as.integer(pageviews))),
         visitNumber =  log1p(visitNumber),
         newVisits = ifelse(newVisits == "1", 1L, 0L),
         bounces = ifelse(bounces == "1", 1L, 0L),
         isMobile = ifelse(isMobile, 1L, 0L),
         adwordsClickInfo.isVideoAd = ifelse(!adwordsClickInfo.isVideoAd, 1L, 0L),
         isTrueDirect = ifelse(isTrueDirect, 1L, 0L),
         date = ymd(date),
         year = year(date),
         month = month(date),
         day = day(date),
         wday = wday(date),
         week = week(date),
         yday = yday(date),
         qday = qday(date),
         weekofyear = paste(year, week, sep = "_"),
         wdayofyear = paste(year, wday, sep = "_"),
         ydayofyear = paste(year, yday, sep = "_"),
         qdayofyear = paste(year, qday, sep = "_"),
         monthofyear = paste(year, month, sep = "_"),
         dayofyear = paste(year, day, sep = "_")
        )

#---------------------------
cat("Creating group features...\n")

for (grp in c("month", "day", "wday", "week", "yday", "qday", "weekofyear",
              "wdayofyear", "ydayofyear", "qdayofyear", "monthofyear", "dayofyear")) {
  col <- paste0(grp, "_user_cnt")
  tr_te %<>% 
    group_by_(grp) %>% 
    mutate(!!col := n_distinct(fullVisitorId)) %>% 
    ungroup()
}

fn <- funs(mean, median, skewness, kurtosis, var, min, max, sum, n_distinct, .args = list(na.rm = TRUE))

sum_by_day <- tr_te %>%
  select(day, hits, pageviews) %>% 
  group_by(day) %>% 
  summarise_all(fn) 

sum_by_month <- tr_te %>%
  select(month, hits, pageviews) %>% 
  group_by(month) %>% 
  summarise_all(fn) 

sum_by_dom <- tr_te %>%
  select(networkDomain, hits, pageviews) %>% 
  group_by(networkDomain) %>% 
  summarise_all(fn) 

sum_by_vn <- tr_te %>%
  select(visitNumber, hits, pageviews) %>% 
  group_by(visitNumber) %>% 
  summarise_all(fn) 

sum_by_country <- tr_te %>%
  select(country, hits, pageviews) %>% 
  group_by(country) %>% 
  summarise_all(fn) 

sum_by_city <- tr_te %>%
  select(city, hits, pageviews) %>% 
  group_by(city) %>% 
  summarise_all(fn) 

sum_by_medium <- tr_te %>%
  select(medium, hits, pageviews) %>% 
  group_by(medium) %>% 
  summarise_all(fn) 

sum_by_source <- tr_te %>%
  select(source, hits, pageviews) %>% 
  group_by(source) %>% 
  summarise_all(fn) 

sum_by_ref <- tr_te %>%
  select(referralPath, hits, pageviews) %>% 
  group_by(referralPath) %>% 
  summarise_all(fn) 

sum_by_wdayofyear <- tr_te %>%
  select(wdayofyear, hits, pageviews) %>% 
  group_by(wdayofyear) %>% 
  summarise_all(fn) 

sum_by_qdayofyear <- tr_te %>%
  select(qdayofyear, hits, pageviews) %>% 
  group_by(qdayofyear) %>% 
  summarise_all(fn) 

sum_by_wday <- tr_te %>%
  select(wday, hits, pageviews) %>% 
  group_by(wday) %>% 
  summarise_all(fn) 

sum_by_qday <- tr_te %>%
  select(qday, hits, pageviews) %>% 
  group_by(qday) %>% 
  summarise_all(fn) 

sum_by_monthofyear <- tr_te %>%
  select(monthofyear, hits, pageviews) %>% 
  group_by(monthofyear) %>% 
  summarise_all(fn) 

sum_by_dayofyear <- tr_te %>%
  select(dayofyear, hits, pageviews) %>% 
  group_by(dayofyear) %>% 
  summarise_all(fn) 
#---------------------------
cat("Creating ohe features...\n")

tr_te_ohe <- tr_te %>%
  select(-date, -fullVisitorId, -visitId, -sessionId, -visitStartTime,
         -weekofyear, -ydayofyear, -qdayofyear, -dayofyear, -campaign, -adContent, -keyword, 
         -adwordsClickInfo.gclId, -adwordsClickInfo.adNetworkType) %>% 
  select_if(is.character) %>% 
  mutate_all(factor)  %>%
  mutate_all(fct_lump, prop = 0.025) %>% 
  mutate_all(fct_explicit_na) %>% 
  model.matrix(~.-1, .) %>% 
  as.data.frame() %>% 
  mutate_all(as.integer)

#---------------------------
cat("Joining datasets...\n")

tr_te %<>% 
  select(-date, -fullVisitorId, -visitId, -sessionId, -visitStartTime) %>% 
  left_join(sum_by_city, by = "city", suffix = c("", "_city")) %>% 
  left_join(sum_by_country, by = "country", suffix = c("", "_country")) %>% 
  left_join(sum_by_day, by = "day", suffix = c("", "_day")) %>% 
  left_join(sum_by_dom, by = "networkDomain", suffix = c("", "_dom")) %>% 
  left_join(sum_by_medium, by = "medium", suffix = c("", "medium")) %>% 
  left_join(sum_by_month, by = "month", suffix = c("", "_month")) %>% 
  left_join(sum_by_ref, by = "referralPath", suffix = c("", "_ref")) %>% 
  left_join(sum_by_source, by = "source", suffix = c("", "_source")) %>% 
  left_join(sum_by_vn, by = "visitNumber", suffix = c("", "_vn")) %>% 
  left_join(sum_by_wdayofyear, by = "wdayofyear", suffix = c("", "_wdayofyear")) %>% 
  left_join(sum_by_qdayofyear, by = "qdayofyear", suffix = c("", "_qdayofyear")) %>% 
  left_join(sum_by_wday, by = "wday", suffix = c("", "_wday")) %>% 
  left_join(sum_by_qday, by = "qday", suffix = c("", "_qday")) %>% 
  left_join(sum_by_monthofyear, by = "monthofyear", suffix = c("", "_monthofyear")) %>% 
  left_join(sum_by_dayofyear, by = "dayofyear", suffix = c("", "_dayofyear")) %>% 
  bind_cols(tr_te_ohe) %>% 
  mutate_if(is.character, funs(factor(.) %>% as.integer)) %>% 
  select_if(has_many_values) 

rm(tr, te, grp, col, flatten_json, parse, has_many_values, is_na_val,
   fn, sum_by_city, sum_by_country, sum_by_day, sum_by_dom, sum_by_medium, 
   sum_by_month, sum_by_ref, sum_by_source, sum_by_vn, tr_te_ohe, sum_by_dayofyear,
   sum_by_monthofyear, sum_by_qday, sum_by_qdayofyear, sum_by_wday, sum_by_wdayofyear)
gc(); gc()

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
          eta = 0.025,
          max_depth = 8,
          min_child_weight = 5,
          gamma = 0,
          subsample = 0.8,
          colsample_bytree = 0.9,
          alpha = 0,
          lambda = 1)

set.seed(0)
cv <- xgb.train(p, dtr, 5000, list(val = dval), print_every_n = 100, early_stopping_rounds = 250)

nrounds <- round(cv$best_iteration * (1 + sum(!idx) / length(idx)))

set.seed(0)
m_xgb <- xgb.train(p, dtrain, nrounds)

imp <- xgb.importance(cols, model = m_xgb) %T>% 
  xgb.plot.importance(top_n = 25)

#---------------------------
cat("Making predictions...\n")

pred <- predict(m_xgb, dtest) %>% 
  as_tibble() %>% 
  set_names("y") %>% 
  mutate(y = ifelse(y < 0, 0, y)) %>% 
  bind_cols(id) %>% 
  group_by(fullVisitorId) %>% 
  summarise(y = sum(y))

#---------------------------
cat("Making submission file...\n")

read_csv("sample_submission.csv") %>%  
  left_join(pred, by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(y, 5)) %>% 
  select(-y) %>% 
  write_csv(paste0("tidy_xGb_", round(cv$best_score, 5), ".csv"))