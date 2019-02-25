library(jsonlite)
library(tidyverse)
library(lubridate)
library(purrr)
library(caret)
library(rlist)
library(pipeR)

tr  <- read_csv("data/train_v2.csv", n_max=1000)   # 20 row sample

fix_json_hits <- function(jsonstring) {
  jsonstring %>%
    str_replace_all(c(
      '"' = "'", # there are few double quotes in the original, converted to single as a first step, for uniformity
      "'(?![a-z])|(?<=\\{|\\s)'" = '"', # converts single quotes to double, except apostrophes
      "True" = '"True"', # add double quotes to logical values
      '" : "' = '":"', # helper
      '(?<!: )"(?= |é)' = "" # remove last remaining double quotes
    ))
}


## hit data parse
hit_type_df  <- tr %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  rename(csv_hits = hits) %>%
  mutate(fixed_json_hits = fix_json_hits(csv_hits)) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits, -csv_hits)

temp <- hit_type_df %>%
  mutate(page_hits = map(r_object_hits, "page")) %>%
  mutate(pages_pagetitle = map(page_hits, "pageTitle"))%>% bind_rows() %>%
  mutate(pages_pagePaths2 = map(page_hits, "pagePathLevel2"))%>% bind_rows() %>%
  mutate(pages_pagePaths3 = map(page_hits, "pagePathLevel3"))%>% bind_rows() %>%
  
  mutate(hitNumber = map(r_object_hits, "hitNumber")) %>% bind_rows() %>%
  mutate(hittime = map(r_object_hits, "time")) %>% bind_rows() %>%
  mutate(hithour = map(r_object_hits, "hour")) %>% bind_rows() %>%
  mutate(hitminute = map(r_object_hits, "minute")) %>% bind_rows() %>%
  mutate(hitinteraction = map(r_object_hits, "isInteraction")) %>% bind_rows() %>%
  mutate(hitentrance = map(r_object_hits, "isEntrance")) %>% bind_rows() %>%
  mutate(hitExit = map(r_object_hits, "isExit")) %>% bind_rows() %>%
  mutate(hittype = map(r_object_hits, "type")) %>% bind_rows() %>%
  mutate(hitdataSource = map(r_object_hits, "dataSource")) %>% bind_rows() %>%
  
  mutate(appInfo= map(r_object_hits, "appInfo")) %>%
  mutate(appInfo_screenName= map(appInfo, "screenName")) %>% bind_rows() %>%
  mutate(appInfo_landingScreenName= map(appInfo, "landingScreenName")) %>% bind_rows() %>%
  mutate(appInfo_exitScreenName= map(appInfo, "exitScreenName")) %>% bind_rows() %>%
  
  mutate(transaction = map(r_object_hits, "transaction")) %>% 
  mutate(transaction_currencyCode = map(transaction, "currencyCode")) %>% bind_rows() %>%
  
  mutate(item = map(r_object_hits, "item")) %>% 
  mutate(item_currencyCode = map(item, "currencyCode")) %>% bind_rows() %>%
  
  mutate(product = map(r_object_hits, "product")) %>% 
  mutate(product_productSKU = map(product, "productSKU")) %>% bind_rows() %>%  
  mutate(product_v2ProductName = map(product, "v2ProductName")) %>% bind_rows() %>%  
  mutate(product_v2ProductCategory = map(product, "v2ProductCategory")) %>% bind_rows() %>%  
  mutate(product_productVariant = map(product, "productVariant")) %>% bind_rows() %>%  
  mutate(product_productBrand = map(product, "productBrand")) %>% bind_rows() %>%  
  mutate(product_productPrice = map(product, "productPrice")) %>% bind_rows() %>%  
  mutate(product_productlocalProductPrice = map(product, "localProductPrice")) %>% bind_rows() %>%  
  mutate(product_isImpression = map(product, "isImpression")) %>% bind_rows() %>%  
  
  mutate(promotion = map(r_object_hits, "promotion")) %>%   
  mutate(promotion_promoId = map(promotion, "promoId")) %>% bind_rows() %>%  
  mutate(promotion_promoName = map(promotion, "promoName")) %>% bind_rows() %>%  
  mutate(promotion_promoCreative = map(promotion, "promoCreative")) %>% bind_rows() %>%  
  mutate(promotion_promoPosition = map(promotion, "promoPosition")) %>% bind_rows() %>%  
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits, -promotion, -social, -eCommerceAction) 



mutate(product_productBrand = map(product, "productBrand")) %>% bind_rows() %>%  tr <- left_join(tr, temp, by = c("fullVisitorId", "visitStartTime"))

temp <- hit_type_df %>%
  mutate(page_hits = map(r_object_hits, "page")) %>%
  mutate(product = map(r_object_hits, "product")) %>% 
  select(-page_hits, -r_object_hits)   

temp2 <- list.map(temp$product, productSKU)



temp2 <- temp$product[[1]] 
  mutate(product_productSKU = map(product, "productSKU")) %>% bind_rows() %>%  
  mutate(product_v2ProductName = map(product, "v2ProductName")) %>% bind_rows() %>%  
  mutate(product_v2ProductCategory = map(product, "v2ProductCategory")) %>% bind_rows() %>%  
  mutate(product_productVariant = map(product, "productVariant")) %>% bind_rows() %>%  
  mutate(product_productBrand = map(product, "productBrand")) %>% bind_rows() %>%  
  mutate(product_productPrice = map(product, "productPrice")) %>% bind_rows() %>%  
  mutate(product_productlocalProductPrice = map(product, "localProductPrice")) %>% bind_rows() %>%  
  mutate(product_isImpression = map(product, "isImpression")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits, -product, -promotion, -social, -eCommerceAction) 