library(tidyverse)


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
               hits = col_character(),
               customDimensions = col_character())

tr <- read_csv("data/train_v2.csv", skip = 1:436393, col_types = ctypes) %>% write_csv(tr,"tr.csv")

