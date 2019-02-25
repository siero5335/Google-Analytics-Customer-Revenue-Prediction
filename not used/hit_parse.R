library("tidyverse");  library("jsonlite");  library("purrr")

#---------------------------
cat("Loading data...\n")

ctypes <- cols(fullVisitorId = col_character(),
               visitStartTime = col_character(),
               hits = col_character())

#---------------------------
cat("Defining auxiliary functions...\n")


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



#---------------------------
cat("Basic preprocessing...\n")

## parse hit tr1
tr1  <- read_csv("data/train_v2_sp1.csv", col_types = ctypes)
  
tr1 %>% tr1
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

tr1  <- tr1 %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(tr1, "data/tr_hit1.csv")

rm(tr1)
gc(); gc()

## parse hit tr2
tr2  <- read_csv("data/train_v2_sp2.csv", col_types = ctypes) %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

tr2  <- tr2 %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(tr2, "data/tr_hit2.csv")

rm(tr2)
gc(); gc()

## parse hit tr3
tr3  <- read_csv("data/train_v2_sp3.csv", col_types = ctypes) %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

tr3  <- tr3 %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(tr3, "data/tr_hit3.csv")

rm(tr3)
gc(); gc()

## parse hit tr4
tr4  <- read_csv("data/train_v2_sp4.csv", col_types = ctypes) %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

tr4  <- tr4 %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(tr4, "data/tr_hit4.csv")

rm(tr4)
gc(); gc()

## parse hit tr5
tr5  <- read_csv("data/train_v2_sp5.csv", col_types = ctypes) %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

tr5  <- tr5 %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(tr5, "data/tr_hit5.csv")

rm(tr5)
gc(); gc()

## parse hit te
te  <- read_csv("data/test_v2.csv", col_types = ctypes) %>%
  select(fullVisitorId, visitStartTime, hits) %>%
  mutate(fixed_json_hits = fix_json_hits(hits)) %>%
  select(-hits) %>%
  mutate(r_object_hits = map(fixed_json_hits, fromJSON)) %>%
  select(-fixed_json_hits)

te  <- te %>%
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
  
  mutate(social = map(r_object_hits, "social")) %>%    
  mutate(social_socialNetwork = map(social, "socialNetwork")) %>% bind_rows() %>%    
  mutate(social_hasSocialSourceReferral = map(social, "hasSocialSourceReferral")) %>% bind_rows() %>%    
  mutate(social_socialInteractionNetworkAction = map(social, "socialInteractionNetworkAction")) %>% bind_rows() %>%   
  
  mutate(eCommerceAction = map(r_object_hits, "eCommerceAction")) %>%     
  mutate(eCommerceAction_action_type = map(eCommerceAction, "action_type")) %>% bind_rows() %>%  
  mutate(eCommerceAction_step = map(eCommerceAction, "step")) %>% bind_rows() %>%  
  
  select(-appInfo, -page_hits, -transaction, -item, -r_object_hits,  -social, -eCommerceAction) %>%  
  write_csv(te, "data/te_hit.csv")
  
  
rm(te)
gc(); gc()