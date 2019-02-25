library("tidyverse"); library("magrittr"); library("jsonlite"); 
library("caret"); library("lubridate"); library("xgboost"); 
library("DataExplorer"); library("GGally"); library("irlba"); library("purrr")
library("summarytools"); library("e1071"); library("magrittr"); library("sessioninfo")

#---------------------------
cat("Loading data...\n")

tr000 <- read_csv("data/hits-000.csv")
tr001 <- read_csv("data/hits-001.csv")
tr002 <- read_csv("data/hits-002.csv")
tr003 <- read_csv("data/hits-003.csv")
tr004 <- read_csv("data/hits-004.csv")
tr005 <- read_csv("data/hits-005.csv")
tr_test <- read_csv("data/hits_test.csv")

tr_te_hit <- tr000 %>% 
  bind_rows(tr001, tr002, tr003, tr004, tr005, tr_test) 