library("tidyverse"); library("magrittr"); library("jsonlite"); 
library("caret"); library("lubridate"); library("lightgbm"); 
library("DataExplorer"); library("GGally"); library("irlba"); 
library("summarytools"); library("e1071"); library("magrittr"); library("Matrix"); 
library("lightgbm"); library("keras"); library("embed")

set.seed(0)

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
#---------------------------
cat("Basic preprocessing...\n")

tr <- parse(tr)
te <- parse(te)

te_bounces <- te[, "bounces"]

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

#---------------------------
cat("Preprocessing...\n")

tri <- 1:nrow(tr)
id <- te[, "fullVisitorId"]

tr_te <- tr %>% 
  bind_rows(te) %>% 
  mutate(
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
    dayofyear = paste(year, day, sep = "_"),
    visitStartTime1= as.POSIXct(visitStartTime, tz="UTC", origin='1970-01-01'),
    sess_date_dow = as.factor(weekdays(visitStartTime1)),
    sess_date_hours = as.numeric(format(visitStartTime1, "%H")),
    sess_date_dom = as.factor(format(visitStartTime1, "%m")),
    transactionRevenue = as.numeric(transactionRevenue)) %>%
  mutate(bounces = as.numeric(ifelse(is.na(bounces), -1, bounces)), 
         hits = log1p(as.integer(hits)),
         pageviews = ifelse(is.na(pageviews), 0L, log1p(as.integer(pageviews))),
         campaignCode = ifelse(is.na(campaignCode),0,campaignCode),
         referralPath = ifelse(is.na(referralPath),-1,referralPath),
         newVisits = as.factor(ifelse(is.na(newVisits), -1, newVisits)),  
         adContent = ifelse(is.na(adContent),-1,adContent),
         adwordsClickInfo.adNetworkType = ifelse(is.na(adwordsClickInfo.adNetworkType),-1,adwordsClickInfo.adNetworkType),
         adwordsClickInfo.gclId = ifelse(is.na(adwordsClickInfo.gclId),-1,adwordsClickInfo.gclId),
         adwordsClickInfo.slot = ifelse(is.na(adwordsClickInfo.slot),-1,adwordsClickInfo.slot),
         adwordsClickInfo.page = ifelse(is.na(adwordsClickInfo.page),-1,adwordsClickInfo.page),
         visits = ifelse(is.na(visits),-1,visits),
         newVisits = ifelse(is.na(newVisits),-1,newVisits),
         campaign = ifelse(is.na(campaign),-1,campaign),
         adwordsClickInfo.isVideoAd = as.factor(ifelse(is.na(adwordsClickInfo.isVideoAd),-1,adwordsClickInfo.isVideoAd))
  ) %>%      
  mutate(transactionRevenue = ifelse(is.na(transactionRevenue), 0, transactionRevenue)
  ) %>% 
  select(-adwordsClickInfo.criteriaParameters,            
         -socialEngagementType,-browserSize,-campaignCode,-visitStartTime1,           
         -language,-flashVersion,-mobileDeviceBranding,-mobileDeviceInfo,-mobileDeviceMarketingName,
         -mobileDeviceModel,-mobileInputSelector,-operatingSystemVersion,-screenColors,-screenResolution,
         -cityId,-latitude,-longitude,-networkLocation) %>% 
  mutate_if(is.character, funs(factor(.) %>% as.integer))

for (grp in c("month", "day", "wday", "week", "yday", "qday", "weekofyear",
              "wdayofyear", "ydayofyear", "qdayofyear", "monthofyear", "dayofyear")) {
  col <- paste0(grp, "_user_cnt")
  tr_te %<>% 
    group_by_(grp) %>% 
    mutate(!!col := n_distinct(fullVisitorId)) %>% 
    ungroup()
}


fn <- funs(mean, median, skewness, kurtosis, var, min, max, sum, n_distinct, .args = list(na.rm = TRUE))

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

sum_by_os <- tr_te %>%
  select(operatingSystem, hits, pageviews) %>% 
  group_by(operatingSystem) %>% 
  summarise_all(fn) 

sum_by_dc <- tr_te %>%
  select(deviceCategory, hits, pageviews) %>% 
  group_by(deviceCategory) %>% 
  summarise_all(fn) 


sum_by_rg <- tr_te %>%
  select(region, hits, pageviews) %>% 
  group_by(region) %>% 
  summarise_all(fn) 

session <- tr_te %>%
  select(fullVisitorId, sessionId) %>%
  group_by(fullVisitorId) %>%
  summarise(session_count = sum(!is.na(sessionId))) 

session_diff <- tr_te %>%
  select(sessionId,fullVisitorId,visitStartTime,date,visitId) %>%
  arrange(fullVisitorId,date,visitStartTime)  %>%
  group_by(fullVisitorId) %>%
  mutate(diff = (lead(visitStartTime) - visitStartTime)/360) %>%
  select(fullVisitorId,sessionId,diff,visitId,date,visitStartTime)

session_diff <- session_diff %>%
  arrange(fullVisitorId,date,visitStartTime) %>%
  mutate(next_session_1 = lead(diff),next_session_2=lag(diff)) %>%
  select(-diff)


dim(session_diff)
session_diff <- as.data.frame(session_diff)


tr_te2 <- tr %>% 
  bind_rows(te) %>% 
  mutate(
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
    dayofyear = paste(year, day, sep = "_"),
    browser_dev = str_c(browser, "_", deviceCategory),
    browser_os = str_c(browser, "_", operatingSystem),
    browser_chan = str_c(browser,  "_", channelGrouping),
    campaign_medium = str_c(campaign, "_", medium),
    chan_os = str_c(operatingSystem, "_", channelGrouping),
    country_adcontent = str_c(country, "_", adContent),
    country_medium = str_c(country, "_", medium),
    country_source = str_c(country, "_", source),
    dev_chan = str_c(deviceCategory, "_", channelGrouping),
    visitStartTime1= as.POSIXct(visitStartTime, tz="UTC", origin='1970-01-01'),
    sess_date_dow = as.factor(weekdays(visitStartTime1)),
    sess_date_hours = as.numeric(format(visitStartTime1, "%H")),
    sess_date_dom = as.factor(format(visitStartTime1, "%m")),
    transactionRevenue = as.numeric(transactionRevenue)) %>%
  mutate(bounces = as.numeric(ifelse(is.na(bounces), -1, bounces)), 
         hits = log1p(as.integer(hits)),
         pageviews = ifelse(is.na(pageviews), 0L, log1p(as.integer(pageviews))),
         campaignCode = ifelse(is.na(campaignCode),0,campaignCode),
         referralPath = ifelse(is.na(referralPath),-1,referralPath),
         newVisits = as.factor(ifelse(is.na(newVisits), -1, newVisits)),  
         adContent = ifelse(is.na(adContent),-1,adContent),
         adwordsClickInfo.adNetworkType = ifelse(is.na(adwordsClickInfo.adNetworkType),-1,adwordsClickInfo.adNetworkType),
         adwordsClickInfo.gclId = ifelse(is.na(adwordsClickInfo.gclId),-1,adwordsClickInfo.gclId),
         adwordsClickInfo.slot = ifelse(is.na(adwordsClickInfo.slot),-1,adwordsClickInfo.slot),
         adwordsClickInfo.page = ifelse(is.na(adwordsClickInfo.page),-1,adwordsClickInfo.page),
         visits = ifelse(is.na(visits),-1,visits),
         newVisits = ifelse(is.na(newVisits),-1,newVisits),
         campaign = ifelse(is.na(campaign),-1,campaign),
         adwordsClickInfo.isVideoAd = as.factor(ifelse(is.na(adwordsClickInfo.isVideoAd),-1,adwordsClickInfo.isVideoAd))
  ) %>%      
  mutate(transactionRevenue = ifelse(is.na(transactionRevenue), 0, transactionRevenue)
  ) %>% 
  select(-adwordsClickInfo.criteriaParameters,            
         -socialEngagementType,-browserSize,-campaignCode,-visitStartTime1,           
         -language,-flashVersion,-mobileDeviceBranding,-mobileDeviceInfo,-mobileDeviceMarketingName,
         -mobileDeviceModel,-mobileInputSelector,-operatingSystemVersion,-screenColors,-screenResolution,
         -cityId,-latitude,-longitude,-networkLocation)



tr_te_ohe <- tr_te2 %>%
  select(-date, -fullVisitorId, -visitStartTime, -sessionId, -referralPath, -networkDomain, 
         -weekofyear, -ydayofyear, -qdayofyear, -dayofyear, -campaign, -adContent, -keyword, -date2, 
         -bounces, -transactionRevenue, -visitNumber, -visits, -browserVersion,
         -hits, -pageviews, -adwordsClickInfo.gclId, -adwordsClickInfo.adNetworkType)  %>% 
  select_if(is.character) %>% 
  mutate_all(factor)  %>%
  mutate_all(fct_lump, prop = 0.05) %>% 
  mutate_all(fct_explicit_na) %>% 
  model.matrix(~.-1, .) %>% 
  as.data.frame() %>% 
  mutate_all(as.integer)




tr_te %<>% 
  left_join(sum_by_city, by = "city", suffix = c("", "_city")) %>% 
  left_join(sum_by_country, by = "country", suffix = c("", "_country")) %>% 
  left_join(sum_by_dom, by = "networkDomain", suffix = c("", "_dom")) %>% 
  left_join(sum_by_medium, by = "medium", suffix = c("", "medium")) %>% 
  left_join(sum_by_source, by = "source", suffix = c("", "_source")) %>% 
  left_join(sum_by_vn, by = "visitNumber", suffix = c("", "_vn")) %>% 
  left_join(session_diff, by = c("sessionId","visitId","fullVisitorId","date","visitStartTime")) %>% 
  bind_cols(tr_te_ohe) %>%
  select(-fullVisitorId,-sessionId,-visitId) %>%
  mutate_if(is.character, funs(factor(.) %>% as.integer)) 


tr_te <- mutate_all(tr_te, funs(as.numeric))

#---------------------------
cat("Preparing data...\n")
dtest <-  as.matrix(tr_te[-tri, !(colnames(tr_te) %in% c("date","transactionRevenue", "date2"))])
te_bounces <- tr_te[-tri, "bounces"]
tr_te <- tr_te[tri, ]

lgb.grid = list(objective = "regression"
                , metric = "rmse"
                , verbose = 0
                ,num_boost_round=5000
)

#lgb.model.cv = lgb.cv(params = lgb.grid, data = lgb.train, learning_rate = 0.01, num_leaves = 48,
#                   num_threads = 4 , nrounds = 40000, early_stopping_rounds = 200,
#                  eval_freq = 300, eval = "rmse",
#                   categorical_feature = categoricals.vec)

#best.iter = lgb.model.cv$best_iter
#best.iter 


categorical_feature <- c("visitNumber","networkDomain","country","source","operatingSystem","deviceCategory","region","browser","metro","city","continent"
                         ,"subContinent","channelGrouping","medium","keyword","adContent","adwordsClickInfo.adNetworkType"
                         ,"adwordsClickInfo.page","adwordsClickInfo.slot","campaign","visits","newVisits","campaign")


rm(tr, te)

dtrain <- tr_te[tr_te$date > ymd("20160831")  ,]
dval <- tr_te[tr_te$date <= ymd("20160831"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date, -date2)
dval <- dval %>%
  select(-transactionRevenue,-date, -date2)          


#---------------------------
cat("Lightgbm Training model1 ...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature = categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature = categorical_feature)

model1 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model1$best_score 
dim(dtrain)
dim(dval)

tree_imp <- lgb.importance(model1, percentage = TRUE)
lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred1 <- predict(model1, dtest)

rm(model1,tree_imp,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 1 ...\n")


dval <- tr_te[tr_te$date >= ymd("20160901") & tr_te$date <= ymd("20160930") ,]
dtrain <- tr_te[tr_te$date < ymd("20160901") | tr_te$date > ymd("20160930"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model2 ...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)


model2 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       



model2$best_score

dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(model2, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred2 <- predict(model2, dtest)

rm(model2,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 2 ...\n")




dval <- tr_te[tr_te$date >= ymd("20161001") & tr_te$date <= ymd("20161031") ,]
dtrain <- tr_te[tr_te$date < ymd("20161001") | tr_te$date > ymd("20161031"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model3 ...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)


model3 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       



model3$best_score

dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(model2, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred3 <- predict(model3, dtest)

rm(model3,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 3 ...\n")



dval <- tr_te[tr_te$date >= ymd("20161101") & tr_te$date <= ymd("20161130") ,]
dtrain <- tr_te[tr_te$date < ymd("20161101") | tr_te$date > ymd("20161130"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 4...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model4 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model4$best_score 
dim(dtrain)
dim(dval)
#---------------------------

# tree_imp <- lgb.importance(model3, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred4 <- predict(model4, dtest)

rm(model4,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 4 ...\n")



dval <- tr_te[tr_te$date >= ymd("20161201") & tr_te$date <= ymd("20161231") ,]
dtrain <- tr_te[tr_te$date < ymd("20161201") | tr_te$date > ymd("20161231"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 5...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model5 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model5$best_score 
dim(dtrain)
dim(dval)
#---------------------------

# tree_imp <- lgb.importance(model3, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred5 <- predict(model5, dtest)

rm(model5,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 5 ...\n")



dval <- tr_te[tr_te$date >= ymd("20161201") & tr_te$date <= ymd("20161231") ,]
dtrain <- tr_te[tr_te$date < ymd("20161201") | tr_te$date > ymd("20161231"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 6...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model6 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model6$best_score 
dim(dtrain)
dim(dval)
#---------------------------

# tree_imp <- lgb.importance(model3, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred6 <- predict(model6, dtest)

rm(model6,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 6 ...\n")


dval <- tr_te[tr_te$date > ymd("20170101") & tr_te$date <= ymd("20170131") ,]
dtrain <- tr_te[tr_te$date < ymd("20170101") | tr_te$date > ymd("20170131"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 7...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model7 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model7$best_score 
dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(lgb.model4, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred7 <- predict(model7, dtest)

rm(model7,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 7 ...\n")



dval <- tr_te[tr_te$date > ymd("20170201") & tr_te$date <= ymd("20170228") ,]
dtrain <- tr_te[tr_te$date < ymd("20170201") | tr_te$date > ymd("20170228"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 8...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model8 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model8$best_score 
dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(lgb.model4, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred8 <- predict(model8, dtest)

rm(model8,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 8 ...\n")


dval <- tr_te[tr_te$date > ymd("20170301") & tr_te$date <= ymd("20170331") ,]
dtrain <- tr_te[tr_te$date < ymd("20170301") | tr_te$date > ymd("20170331"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 9...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model9 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model9$best_score 
dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(lgb.model4, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred9 <- predict(model9, dtest)

rm(model9,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 9 ...\n")


dval <- tr_te[tr_te$date > ymd("20170401") & tr_te$date <= ymd("20170430") ,]
dtrain <- tr_te[tr_te$date < ymd("20170401") | tr_te$date > ymd("20170430"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 10...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model10 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model10$best_score 
dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(lgb.model4, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred10 <- predict(model10, dtest)

rm(model10,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 10 ...\n")



dval <- tr_te[tr_te$date > ymd("20170501") & tr_te$date <= ymd("20170531") ,]
dtrain <- tr_te[tr_te$date < ymd("20170501") | tr_te$date > ymd("20170531"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 11...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model11 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model11$best_score 
dim(dtrain)
dim(dval)

# tree_imp <- lgb.importance(lgb.model4, percentage = TRUE)
# lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred11 <- predict(model11, dtest)

rm(model11,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 11 ...\n")


dtrain <- tr_te[tr_te$date < ymd("20170601"),]
dval <- tr_te[tr_te$date >= ymd("20170601"),]

dtrain_target <- log1p(dtrain$transactionRevenue)
dval_target <-   log1p(dval$transactionRevenue)

dtrain <- dtrain %>%
  select(-transactionRevenue,-date)
dval <- dval %>%
  select(-transactionRevenue,-date)          


#---------------------------
cat("Lightgbm Training model 12...\n")


train = lgb.Dataset(data=as.matrix(dtrain),label=dtrain_target,categorical_feature =categorical_feature)
valid = lgb.Dataset(data=as.matrix(dval),label=dval_target,categorical_feature =categorical_feature)

model12 <- lgb.train(
  params = lgb.grid
  , data = train
  , valids = list(val = valid)
  , learning_rate = .01
  , num_leaves = 48
  , min_data_in_leaf = 30
  , max_depth = 8
  , min_child_samples=20
  , boosting = "gbdt"
  , feature_fraction = 0.8
  , metric = 'rmse'
  , lambda_l1 = 1
  , lambda_l2 = 1
  , verbose = -1
  , seed = 0
  , nrounds = 60000
  , early_stopping_rounds = 200
)       


model12$best_score 
dim(dtrain)
dim(dval)
#---------------------------

#tree_imp <- lgb.importance(model5, percentage = TRUE)
#lgb.plot.importance(tree_imp, top_n = 50, measure = "Gain")
pred12 <- predict(model12, dtest)

rm(model12,dtrain,dval,train,valid,dtrain_target,dval_target)
gc()
cat("Lightgbm Training model end 12 ...\n")

pred1 <- as.data.frame(pred1)
pred2 <- as.data.frame(pred2)
pred3 <- as.data.frame(pred3)
pred4 <- as.data.frame(pred4)
pred5 <- as.data.frame(pred5)
pred6 <- as.data.frame(pred6)
pred7 <- as.data.frame(pred7)
pred8 <- as.data.frame(pred8)
pred9 <- as.data.frame(pred9)
pred10 <- as.data.frame(pred10)
pred11 <- as.data.frame(pred11)
pred12 <- as.data.frame(pred12)

predict <- (pred1+pred2+pred3+pred4+pred5+pred6+pred7+pred8+pred9+pred10+pred11+pred12)/12


rm(pred1,pred2,pred3,pred4,pred5,pred6,pred7,pred8,pred9,pred10,pred11,pred12)
gc()

cat("Making predictions...\n")

pred <- predict %>% 
  as_tibble() %>% 
  set_names("y") %>% 
  mutate(y = expm1(y)) %>% 
  bind_cols(te_bounces) %>%
  mutate(y = ifelse(bounces == 1 , 0,y)) %>%
  mutate(y = ifelse(y < 0, 0, y)) %>% 
  select(-bounces) %>%
  bind_cols(id) %>% 
  group_by(fullVisitorId) %>% 
  summarise(y = log1p(sum(y)))

rm(predict)
gc()

#---------------------------
cat("Making submission file...\n")

read_csv("sample_submission.csv") %>%  
  left_join(pred, by = "fullVisitorId") %>% 
  mutate(PredictedLogRevenue = round(y, 5)) %>% 
  select(-y) %>% 
  write_csv("Lightgbm_output.csv")