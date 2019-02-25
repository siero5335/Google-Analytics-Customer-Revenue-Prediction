library("tidyverse");  library("jsonlite");  library("purrr")

#---------------------------
cat("Loading data...\n")

ctypes <- cols(fullVisitorId = col_character(),
               channelGrouping = col_skip(),
               date = col_skip(),
               device = col_skip(),
               geoNetwork = col_skip(),
               socialEngagementType = col_skip(), 
               totals = col_skip(),
               trafficSource = col_skip(),
               visitId = col_skip(), 
               visitNumber = col_skip(),
               visitStartTime = col_character(),
               hits = col_character(),
               customDimensions = col_skip())

## psplit tr
tr  <- read_csv("data/train_v2.csv", col_types = ctypes)

write_csv(tr[1:350000,], "data/train_v2_sp1.csv")
write_csv(tr[350001:700000,], "data/train_v2_sp2.csv")
write_csv(tr[700001:1050000,], "data/train_v2_sp3.csv")
write_csv(tr[1050001:1400000,], "data/train_v2_sp4.csv")
write_csv(tr[1400001:1708337,], "data/train_v2_sp5.csv")